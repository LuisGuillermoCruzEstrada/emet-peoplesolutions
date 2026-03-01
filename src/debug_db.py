"""
debug_db.py

Imprime información de la conexión real a Postgres:
- host, puerto, db
- usuario
- search_path (schema)
y lista tablas existentes en el schema actual.
"""

from db import get_connection

if __name__ == "__main__":
    with get_connection() as conn:
        # ¿Dónde estoy conectado?
        row = conn.exec_driver_sql("""
            SELECT
              current_database() AS db,
              current_user AS usr,
              inet_server_addr()::text AS server_addr,
              inet_server_port() AS server_port,
              current_schema() AS schema,
              current_setting('search_path') AS search_path
        """).mappings().one()

        print("=== CONEXIÓN REAL ===")
        for k, v in row.items():
            print(f"{k}: {v}")

        # ¿Qué tablas ve este usuario en public?
        tables = conn.exec_driver_sql("""
            SELECT tablename
            FROM pg_catalog.pg_tables
            WHERE schemaname = 'public'
            ORDER BY tablename;
        """).fetchall()

        print("\n=== TABLAS EN public ===")
        if not tables:
            print("(ninguna)")
        else:
            for t in tables:
                print("-", t[0])
                