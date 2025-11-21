# Imagen base: Python 3.12
FROM python:3.12-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiamos solo lo necesario
COPY etl/ ./etl/
COPY schema/ ./schema/
COPY README.md ./README.md

# Instalamos dependencias
RUN pip install --no-cache-dir requests psycopg2-binary python-dotenv

# Variables de entorno para la DB (se pueden sobreescribir al ejecutar)
ENV DB_HOST=localhost \
    DB_PORT=5433 \
    DB_NAME=postgres \
    DB_USER=camilo \
    DB_PASSWORD=camilo

# Comando por defecto: correr el ETL RAW
CMD ["python", "etl/load_raw_from_sources.py"]
