# Utiliser une image Python officielle
FROM python:3.11-slim

# Installer les dépendances système pour PyPDF2 et watchdog
RUN apt-get update && apt-get install -y \
    build-essential \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

# Créer le dossier de l'application
WORKDIR /app

# Copier le script Python dans le conteneur
COPY main.py .

# Installer les dépendances Python
RUN pip install --no-cache-dir PyPDF2 watchdog psycopg2-binary

# Créer les dossiers pour les PDFs et les résultats
RUN mkdir -p /books /books_processed

# Dossier à surveiller
#VOLUME ["/books"]

# Lancer le script de surveillance au démarrage
CMD ["python", "main.py"]
