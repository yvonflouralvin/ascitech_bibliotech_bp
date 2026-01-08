import os
import time
import json
import base64
from io import BytesIO

from PyPDF2 import PdfReader
from pdf2image import convert_from_path

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

import psycopg2
from psycopg2 import sql


# =============================
# CONFIGURATION DES DOSSIERS
# =============================

parent_folder = "files/books"
output_folder = "files/books_content"

os.makedirs(parent_folder, exist_ok=True)
os.makedirs(output_folder, exist_ok=True)

lock_file = os.path.join(output_folder, "lock.json")


# =============================
# POSTGRESQL
# =============================

def mark_as_processed(book_id, page_count):
    """
    Met à jour already_process = TRUE et page_count
    """
    db_config = {
        "host": os.environ.get("DB_HOST", "postgres_host"),
        "port": os.environ.get("DB_PORT", 5432),
        "dbname": os.environ.get("DB_NAME", "ma_base"),
        "user": os.environ.get("DB_USER", "mon_user"),
        "password": os.environ.get("DB_PASSWORD", "mon_password"),
    }

    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        query = sql.SQL("""
            UPDATE school_book
            SET already_process = TRUE,
                page = %s
            WHERE id = %s
        """)

        cur.execute(query, (page_count, book_id))
        conn.commit()

        cur.close()
        conn.close()

        print(f"[DB] Livre {book_id} marqué comme traité ({page_count} pages)")

    except Exception as e:
        print(f"[DB ERROR] {book_id} : {e}")


# =============================
# TRAITEMENT PDF
# =============================

def process_pdf(file_path):
    print("Nouveau fichier détecté, attente de stabilisation...")
    time.sleep(10)

    filename = os.path.basename(file_path)
    book_id = os.path.splitext(filename)[0]
    subfolder_path = os.path.join(output_folder, book_id)

    # Déjà traité ?
    if os.path.exists(subfolder_path):
        if not os.path.exists(lock_file):
            print(f"{filename} déjà traité, ignoré.")
            return

        with open(lock_file, "r", encoding="utf-8") as f:
            lock = json.load(f)

        if lock.get("file") != filename:
            print(f"{filename} déjà traité, ignoré.")
            return

    os.makedirs(subfolder_path, exist_ok=True)

    try:
        # Nombre total de pages
        reader = PdfReader(file_path)
        total_pages = len(reader.pages)

        # Reprise
        start_page = 0
        if os.path.exists(lock_file):
            with open(lock_file, "r", encoding="utf-8") as f:
                lock = json.load(f)
            if lock.get("file") == filename:
                start_page = lock.get("page", 0)

        print(f"Traitement de {filename} à partir de la page {start_page + 1}")

        # Conversion PDF → images
        images = convert_from_path(
            file_path,
            dpi=200,
            first_page=start_page + 1
        )

        for idx, image in enumerate(images, start=start_page + 1):
            buffer = BytesIO()
            image.save(buffer, format="PNG")

            base64_image = base64.b64encode(buffer.getvalue()).decode("utf-8")

            output_path = os.path.join(
                subfolder_path,
                f"content_{idx:02d}.txt"
            )

            with open(output_path, "w", encoding="utf-8") as f:
                f.write(base64_image)

            # Mise à jour du lock
            with open(lock_file, "w", encoding="utf-8") as f_lock:
                json.dump(
                    {"file": filename, "page": idx},
                    f_lock
                )

        # Fin du PDF
        if os.path.exists(lock_file):
            os.remove(lock_file)

        print(f"Traitement terminé pour {filename}")

        # Update DB
        mark_as_processed(book_id, total_pages)

    except Exception as e:
        print(f"[ERROR] {filename} : {e}")


# =============================
# WATCHDOG
# =============================

class PDFHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.lower().endswith(".pdf"):
            print(f"Nouveau fichier déposé : {event.src_path}")
            process_pdf(event.src_path)


observer = Observer()
observer.schedule(PDFHandler(), path=parent_folder, recursive=False)
observer.start()

print("Surveillance du dossier files/books...")


# =============================
# SERVICE TOUJOURS ACTIF
# =============================

while True:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        print("Arrêt manuel demandé.")
        observer.stop()
        break
    except Exception as e:
        print(f"[SERVICE ERROR] {e}")
        continue

observer.join()
print("Service arrêté.")
