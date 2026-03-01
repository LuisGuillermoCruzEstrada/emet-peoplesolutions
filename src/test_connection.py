"""
test_connection.py

Script simple para probar que la conexión a la base de datos funciona.
"""

from db import get_connection


def test_db_connection():
    try:
        conn = get_connection()
        print("Conexión a PostgreSQL exitosa 🚀")
        conn.close()
    except Exception as e:
        print("Error conectando a la base de datos:", e)


if __name__ == "__main__":
    test_db_connection()