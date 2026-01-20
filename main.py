import os
import time
import json
import base64
import traceback
import shutil
from io import BytesIO

from PyPDF2 import PdfReader
from pdf2image import convert_from_path

from ebooklib import epub, ITEM_DOCUMENT
from bs4 import BeautifulSoup
from PIL import Image, ImageDraw, ImageFont

import psycopg2
from psycopg2 import pool

# ============================================================
# CONFIG
# ============================================================

BOOKS_DIR = "files/books"
CONTENT_DIR = "files/books_content"
LOCK_FILE = os.path.join(CONTENT_DIR, "lock.json")

SUPPORTED_EXTENSIONS = [".pdf", ".epub"]

os.makedirs(BOOKS_DIR, exist_ok=True)
os.makedirs(CONTENT_DIR, exist_ok=True)

DB_POOL = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=5,
    host=os.environ.get("DB_HOST", "prod_postgres"),
    port=os.environ.get("DB_PORT", 5432),
    dbname=os.environ.get("DB_NAME", "ma_base"),
    user=os.environ.get("DB_USER", "mon_user"),
    password=os.environ.get("DB_PASSWORD", "mon_password"),
)

# ============================================================
# DB HELPERS
# ============================================================

def get_db_conn():
    return DB_POOL.getconn()

def release_db_conn(conn):
    DB_POOL.putconn(conn)

def update_book_status(book_id, status, page_count=None, error_md=None):
    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE school_book
            SET status = %s,
                page = COALESCE(%s, page),
                processing_error = COALESCE(%s, processing_error),
                updated_at = NOW()
            WHERE id = %s
        """, (status, page_count, error_md, book_id))
        conn.commit()
        cur.close()
        release_db_conn(conn)
        print(f"[DB] {book_id} → {status}")
    except Exception as e:
        if conn:
            conn.rollback()
            release_db_conn(conn)
        print(f"[DB ERROR] {book_id}: {e}")

# ============================================================
# DB LOCKING
# ============================================================

def fetch_one_book_for_processing():
    conn = None
    try:
        conn = get_db_conn()
        conn.autocommit = False
        cur = conn.cursor()

        cur.execute("""
            SELECT id
            FROM school_book
            WHERE status IN ('pending', 'error')
            ORDER BY updated_at NULLS FIRST
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        """)

        row = cur.fetchone()
        if not row:
            conn.commit()
            cur.close()
            release_db_conn(conn)
            return None

        book_id = row[0]

        cur.execute("""
            UPDATE school_book
            SET status = 'processing',
                updated_at = NOW()
            WHERE id = %s
        """, (book_id,))

        conn.commit()
        cur.close()
        release_db_conn(conn)
        return book_id

    except Exception as e:
        if conn:
            conn.rollback()
            release_db_conn(conn)
        print(f"[DB LOCK ERROR] {e}")
        return None

# ============================================================
# SOURCE DETECTION
# ============================================================

def detect_source_file(book_id):
    for ext in SUPPORTED_EXTENSIONS:
        path = os.path.join(BOOKS_DIR, f"{book_id}{ext}")
        if os.path.exists(path):
            return path, ext
    return None, None

# ============================================================
# CONVERTERS
# ============================================================

def update_lock(page):
        with open(LOCK_FILE, "w") as f:
            json.dump({"book_id": self.book_id, "page": page}, f)
            
class BaseConverter:
    def __init__(self, source_path, book_id):
        self.source_path = source_path
        self.book_id = book_id
        self.book_dir = os.path.join(CONTENT_DIR, book_id)
        os.makedirs(self.book_dir, exist_ok=True)

    def convert(self):
        raise NotImplementedError


class PdfConverter(BaseConverter):
    def convert(self):
        reader = PdfReader(self.source_path)
        total_pages = len(reader.pages)

        for i in range(total_pages):
            images = convert_from_path(
                self.source_path,
                dpi=200,
                first_page=i + 1,
                last_page=i + 1
            )

            buffer = BytesIO()
            images[0].save(buffer, format="PNG")

            encoded = base64.b64encode(buffer.getvalue()).decode()
            with open(os.path.join(self.book_dir, f"content_{i+1:03d}.txt"), "w") as f:
                f.write(encoded)

            update_lock(i + 1)

        self._clear_lock()
        return total_pages


class EpubConverter(BaseConverter):
    def convert(self):
        book = epub.read_epub(self.source_path)
        items = [i for i in book.get_items() if i.get_type() == ITEM_DOCUMENT]

        page = 0
        for item in items:
            soup = BeautifulSoup(item.get_content(), "html.parser")
            text = soup.get_text().strip()
            if not text:
                continue

            image = self._text_to_image(text)
            buffer = BytesIO()
            image.save(buffer, format="PNG")

            encoded = base64.b64encode(buffer.getvalue()).decode()
            page += 1
            with open(os.path.join(self.book_dir, f"content_{page:03d}.txt"), "w") as f:
                f.write(encoded)

            update_lock(page)

        self._clear_lock()
        return page

    def _text_to_image(self, text):
        img = Image.new("RGB", (1654, 2339), "white")
        draw = ImageDraw.Draw(img)
        draw.text((50, 50), text[:4000], fill="black")
        return img

    def _clear_lock(self):
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)

# ============================================================
# MAIN PROCESS
# ============================================================

def process_book(book_id):
    source_path, ext = detect_source_file(book_id)
    if not source_path:
        update_book_status(book_id, "error", error_md="Aucun fichier source trouvé")
        return

    try:
        if ext == ".pdf":
            converter = PdfConverter(source_path, book_id)
        elif ext == ".epub":
            converter = EpubConverter(source_path, book_id)
        else:
            raise Exception(f"Extension non supportée : {ext}")

        pages = converter.convert()
        update_book_status(book_id, "done", page_count=pages)

    except Exception as e:
        error_md = f"""# ❌ Erreur conversion `{book_id}`

                    ```text
                    {str(e)}

                    {traceback.format_exc()}
                    ```"""
        print(error_md)
        shutil.rmtree(os.path.join(CONTENT_DIR, book_id), ignore_errors=True)
        update_book_status(book_id, "error", error_md=error_md)

# ============================================================
# WORKER LOOP
# ============================================================

def worker_loop():
    print("🚀 Worker prêt (PDF + EPUB)")
    while True:
        try:
            book_id = fetch_one_book_for_processing()
            if not book_id:
                time.sleep(3)
                continue

            print(f"[LOCKED] {book_id}")
            process_book(book_id)

        except Exception as e:
            print(f"[WORKER ERROR] {e}")
            time.sleep(5)

# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    worker_loop()
