import os
import time
import json
import base64
from io import BytesIO
from PyPDF2 import PdfReader
from pdf2image import convert_from_path
import psycopg2
from psycopg2 import sql
import shutil
import traceback

# =============================
# CONFIGURATION DES DOSSIERS
# =============================
parent_folder = "files/books"
output_folder = "files/books_content"

os.makedirs(parent_folder, exist_ok=True)
os.makedirs(output_folder, exist_ok=True)

lock_file = os.path.join(output_folder, "lock.json")

db_config = {
    "host": os.environ.get("DB_HOST", "postgres_host"),
    "port": os.environ.get("DB_PORT", 5432),
    "dbname": os.environ.get("DB_NAME", "ma_base"),
    "user": os.environ.get("DB_USER", "mon_user"),
    "password": os.environ.get("DB_PASSWORD", "mon_password"),
}

# =============================
# FONCTIONS DB
# =============================
def get_all_books():
    """Retourne tous les livres avec id et status"""
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        cur.execute("SELECT id, status, title FROM school_book")
        books = cur.fetchall()
        cur.close()
        conn.close()
        return books
    except Exception as e:
        print(f"[DB ERROR] Impossible de récupérer les livres: {e}")
        return []

def update_book_status(book_id, status, page_count=None, error_md=None):
    """
    Met à jour le status d'un livre dans la DB.
    - status : 'done', 'error', etc.
    - page_count : nombre de pages (si applicable)
    - error_md : message d'erreur Markdown (si applicable)
    """
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        query = """
            UPDATE school_book
            SET status = %s,
                page = COALESCE(%s, page),
                processing_error = COALESCE(%s, processing_error)
            WHERE id = %s
        """
        cur.execute(query, (status, page_count, error_md, book_id))
        conn.commit()
        cur.close()
        conn.close()

        if status == "done":
            print(f"[DB] Livre {book_id} marqué comme traité ({page_count} pages)")
        elif status == "error":
            print(f"[DB] Livre {book_id} marqué en erreur")
        else:
            print(f"[DB] Livre {book_id} mis à jour : status={status}")

    except Exception as e:
        print(f"[DB ERROR] Livre {book_id} : {e}")

# =============================
# TRAITEMENT PDF
# =============================
def process_pdf(book_id, book_title):
    file_path = os.path.join(parent_folder, f"{book_id}.pdf")
    subfolder_path = os.path.join(output_folder, book_id)

    if not os.path.exists(file_path):
        print(f"[SKIP] Fichier source [{book_title}] {file_path} inexistant.")
        return

    os.makedirs(subfolder_path, exist_ok=True)

    try:
        reader = PdfReader(file_path)
        total_pages = len(reader.pages)
        start_page = 0

        # Reprise avec lock
        if os.path.exists(lock_file):
            with open(lock_file, "r", encoding="utf-8") as f:
                lock = json.load(f)
            if lock.get("file") == f"{book_id}.pdf":
                start_page = lock.get("page", 0)

        print(f"Traitement du livre [{book_title}/>>>{book_id}<<<] à partir de la page {start_page + 1}")

        # Conversion page par page
        for i in range(start_page, total_pages):
            print(f"[INFO] Conversion page {i+1}/{total_pages} de [{book_title}/>>>{book_id}<<<]")
            images = convert_from_path(file_path, dpi=200, first_page=i+1, last_page=i+1)
            image = images[0]
            buffer = BytesIO()
            image.save(buffer, format="PNG")
            base64_image = base64.b64encode(buffer.getvalue()).decode("utf-8")

            output_path = os.path.join(subfolder_path, f"content_{i+1:02d}.txt")
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(base64_image)

            # Mise à jour du lock
            with open(lock_file, "w", encoding="utf-8") as f_lock:
                json.dump({"file": f"{book_id}.pdf", "page": i+1}, f_lock)

        # Suppression du lock
        if os.path.exists(lock_file):
            os.remove(lock_file)

        update_book_status(book_id, status="done", page_count=total_pages)
        print(f"Traitement terminé pour le livre [{book_title}/>>>{book_id}<<<]")

    except Exception as e:
        print(f"[ERROR] Livre {book_id} : {e}")
        error_md = f"""# ❌ Erreur de traitement du livre [{book_title}/>>>{book_id}<<<]
                    ## 📄 Fichier
                    `{book_id}.pdf`

                    ## 🧨 Exception
                    ```text
                    {str(e)}

                    Traceback
                    {traceback.format_exc()}
                    ```"""
        # Nettoyage fichiers
        if os.path.exists(file_path):
            os.remove(file_path)
        if os.path.exists(subfolder_path):
            shutil.rmtree(subfolder_path, ignore_errors=True)
        if os.path.exists(lock_file):
            os.remove(lock_file)

        update_book_status(book_id, status="error", error_md=error_md)

# =============================
# LOGIQUE DE REPRISE
# =============================
def should_process(book):
    """
    Vérifie si le livre doit être traité selon les règles :
    1️⃣ status != done & dossier book_content inexistant
    2️⃣ status != done & dossier existant & source existe & pages diffèrent
    3️⃣ status = done & dossier existant & source existe & pages diffèrent
    """
    book_id, status, title = book
    source_file = os.path.join(parent_folder, f"{book_id}.pdf")
    subfolder_path = os.path.join(output_folder, book_id)

    # Cas 1 et 2
    if status != "done":
        if not os.path.exists(subfolder_path):
            return True
        if os.path.exists(subfolder_path) and os.path.exists(source_file):
            try:
                source_pages = len(PdfReader(source_file).pages)
                book_pages = len([f for f in os.listdir(subfolder_path) if f.endswith(".txt")])
                if source_pages != book_pages:
                    return True
            except Exception as e:
                print(f"[CHECK ERROR] Livre [{title}/{book_id}]: {e}")
                return True
        return False

    # Cas 3
    if status == "done" and os.path.exists(subfolder_path) and os.path.exists(source_file):
        try:
            source_pages = len(PdfReader(source_file).pages)
            book_pages = len([f for f in os.listdir(subfolder_path) if f.endswith(".txt")])
            if source_pages != book_pages:
                return True
        except Exception as e:
            print(f"[CHECK ERROR] Livre [{title}/{book_id}]: {e}")
            return True

    return False

# =============================
# SERVICE PRINCIPAL
# =============================
if __name__ == "__main__":
    print("Service de traitement PDF démarré...")
    while True:
        try:
            books = get_all_books()
            for book in books:
                if should_process(book):
                    book_id, status, title = book
                    process_pdf(book_id, title)
            time.sleep(60* os.environ.get("SLEEP_TIME", 1))  # intervalle entre vérifications
        except KeyboardInterrupt:
            print("Arrêt manuel demandé.")
            break
        except Exception as e:
            print(f"[SERVICE ERROR] {e}")
            time.sleep(60* os.environ.get("SLEEP_TIME", 1))
