FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    build-essential \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir PyPDF2 watchdog psycopg2-binary

# Lance main.py et empêche l'arrêt du container
CMD sh -c "python main.py || echo 'main.py crashed'; tail -f /dev/null"