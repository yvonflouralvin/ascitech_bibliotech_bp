import os
import time
import json
import base64
from PyPDF2 import PdfReader
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import psycopg2
from psycopg2 import sql
import os

# Dossiers
parent_folder = "books"
output_folder = "books_processed"
os.makedirs(output_folder, exist_ok=True)
lock_file = os.path.join(output_folder, "lock.json")


def mark_as_processed(filename):
    """
    Connecte à PostgreSQL et met à jour le champ already_process = True
    pour le fichier donné.
    """
    # Informations de connexion à adapter
    db_config = {
        "host": os.environ.get('DB_HOST', "postgres_host"),
        "port": os.environ.get('DB_PORT', 5432),
        "dbname": os.environ.get('DB_NAME', "ma_base"),
        "user": os.environ.get('DB_USER', "mon_user"),
        "password": os.environ.get('DB_PASSWORD', "mon_password")
    }

    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        # Exécuter la mise à jour
        query = sql.SQL("UPDATE school_book SET already_process = TRUE WHERE id = %s")
        cur.execute(query, (filename,))
        conn.commit()

        cur.close()
        conn.close()
        print(f"{filename} marqué comme traité en base.")
    except Exception as e:
        print(f"Erreur lors de la mise à jour PostgreSQL pour {filename}: {e}")

# Fonction pour traiter un PDF
def process_pdf(file_path):
    filename = os.path.basename(file_path)
    pdf_name = os.path.splitext(filename)[0]
    subfolder_path = os.path.join(output_folder, pdf_name)
    
    # Vérifier si le PDF est déjà traité
    if os.path.exists(subfolder_path) and not (os.path.exists(lock_file) and json.load(open(lock_file)).get("file") == filename):
        print(f"{filename} déjà traité.")
        return

    os.makedirs(subfolder_path, exist_ok=True)

    try:
        reader = PdfReader(file_path)
        total_pages = len(reader.pages)

        # Reprendre depuis lock si nécessaire
        start_page = 0
        if os.path.exists(lock_file):
            lock = json.load(open(lock_file))
            if lock.get("file") == filename:
                start_page = lock.get("page", 0)

        for i in range(start_page, total_pages):
            page = reader.pages[i]
            text = page.extract_text() or ""
            base64_text = base64.b64encode(text.encode("utf-8")).decode("utf-8")
            output_filename = f"content_{i+1:02d}.txt"
            output_path = os.path.join(subfolder_path, output_filename)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(base64_text)

            # Mettre à jour lock
            lock_data = {"file": filename, "page": i + 1}
            with open(lock_file, "w", encoding="utf-8") as f_lock:
                json.dump(lock_data, f_lock)

        # PDF terminé → supprimer lock
        if os.path.exists(lock_file):
            os.remove(lock_file)
        print(f"Traitement terminé pour {filename}")

        # Mettre à jour la base PostgreSQL
        mark_as_processed(filename)

    except Exception as e:
        print(f"Erreur avec {filename}: {e}")

# Classe pour gérer les événements du dossier
class PDFHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.lower().endswith(".pdf"):
            print(f"Nouveau fichier détecté: {event.src_path}")
            process_pdf(event.src_path)

# Observer le dossier
observer = Observer()
observer.schedule(PDFHandler(), path=parent_folder, recursive=False)
observer.start()

print("Surveillance du dossier /books...")

while True:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        print("Arrêt demandé par l'utilisateur...")
        observer.stop()
        break
    except Exception as e:
        # Ne jamais arrêter le service en cas d'erreur
        print(f"Erreur inattendue dans la boucle principale: {e}")
        # On continue à observer le dossier
        continue

observer.join()
print("Service arrêté.")
