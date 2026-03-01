"""
db.py

Este archivo se encarga de crear la conexión a la base de datos PostgreSQL.
Usamos SQLAlchemy porque permite trabajar con bases de datos de forma más
segura y flexible que escribir SQL directamente en todo el proyecto.
"""

import os

# Librería para leer el archivo .env
from dotenv import load_dotenv

# Librería para crear conexiones a la base de datos
from sqlalchemy import create_engine


# Cargamos las variables del archivo .env
load_dotenv()


# Obtenemos cada variable de entorno
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


# Construimos la URL de conexión a PostgreSQL
DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


# Creamos el engine de SQLAlchemy
engine = create_engine(DATABASE_URL)


def get_connection():
    """
    Esta función devuelve una conexión activa a la base de datos.
    Otros módulos del proyecto podrán usar esta función para ejecutar queries.
    """
    return engine.connect()