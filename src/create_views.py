"""
create_views.py

Ejecuta el SQL que crea las vistas requeridas (punto 1.5).
Usa engine.begin() para asegurar COMMIT.
"""

from pathlib import Path
from db import engine


def run_sql_file(sql_path: Path):
    sql_text = sql_path.read_text(encoding="utf-8")

    # Quitar comentarios y líneas vacías
    lines = []
    for line in sql_text.splitlines():
        s = line.strip()
        if s.startswith("--") or s == "":
            continue
        lines.append(line)

    cleaned = "\n".join(lines)
    statements = [s.strip() for s in cleaned.split(";") if s.strip()]

    with engine.begin() as conn:
        for stmt in statements:
            conn.exec_driver_sql(stmt)

    print(f"✅ Vistas creadas: {sql_path}")


if __name__ == "__main__":
    run_sql_file(Path("sql/03_views.sql"))