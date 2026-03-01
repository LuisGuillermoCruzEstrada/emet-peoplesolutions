"""
reset_db.py

Resetea el esquema public para empezar desde cero.
Usa COMMIT garantizado con engine.begin().
"""

from pathlib import Path
from db import engine


def run_sql_file(sql_path: Path):
    sql_text = sql_path.read_text(encoding="utf-8")
    lines = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or stripped == "":
            continue
        lines.append(line)
    cleaned_sql = "\n".join(lines)
    statements = [s.strip() for s in cleaned_sql.split(";") if s.strip()]

    with engine.begin() as conn:
        for stmt in statements:
            conn.exec_driver_sql(stmt)

    print(f"✅ Reset aplicado: {sql_path}")


if __name__ == "__main__":
    run_sql_file(Path("sql/00_reset.sql"))