"""
create_tables.py

Ejecuta el archivo SQL para crear tablas/índices en PostgreSQL.
Usamos engine.begin() para abrir una transacción que hace COMMIT automático.
Esto evita el caso donde las sentencias se ejecutan pero no se guardan.
"""

from pathlib import Path
from db import engine


def run_sql_file(sql_path: Path):
    sql_text = sql_path.read_text(encoding="utf-8")

    # Quitamos comentarios y líneas vacías
    lines = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or stripped == "":
            continue
        lines.append(line)

    cleaned_sql = "\n".join(lines)

    # Partimos por ';' y ejecutamos statement por statement
    statements = [s.strip() for s in cleaned_sql.split(";") if s.strip()]

    # begin() => abre transacción y hace COMMIT al final si todo sale bien
    with engine.begin() as conn:
        for i, stmt in enumerate(statements, start=1):
            conn.exec_driver_sql(stmt)
            print(f"OK [{i}/{len(statements)}]: {stmt[:60]}...")

    print(f"\n✅ Tablas/índices creados y confirmados (COMMIT): {sql_path}")


if __name__ == "__main__":
    run_sql_file(Path("sql/01_create_tables.sql"))