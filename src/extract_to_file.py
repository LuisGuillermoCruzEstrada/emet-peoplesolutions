"""
extract_to_file.py

Punto 1.2 - Extracción:

Extraemos la información ya cargada en PostgreSQL y la guardamos en archivos finales
(Parquet y CSV).

Parquet
- Es el formato más compatible para revisión rápida.
"""

from pathlib import Path
import pandas as pd
from db import engine

EXPORT_DIR = Path("exports")
EXPORT_DIR.mkdir(exist_ok=True)

PARQUET_PATH = EXPORT_DIR / "charges_enriched.parquet"
CSV_PATH = EXPORT_DIR / "charges_enriched.csv"


def main():
    # 1) Query de extracción:
    # "Enriched" = charges + nombre de la compañía (join)
    query = """
    SELECT
        ch.charge_id,
        ch.company_id,
        co.company_name,
        ch.amount,
        ch.status,
        ch.created_at,
        ch.paid_at
    FROM charges ch
    JOIN companies co
      ON co.company_id = ch.company_id
    ORDER BY ch.created_at ASC;
    """

    # 2) Extraemos desde Postgres hacia un DataFrame
    # read_sql_query abre conexión y trae el resultado a pandas
    df = pd.read_sql_query(query, engine)

    # 3) Guardamos en Parquet (mejor para datos / conserva tipos)
    df.to_parquet(PARQUET_PATH, index=False)

    # 4) Guardamos en CSV (más compatible)
    df.to_csv(CSV_PATH, index=False, encoding="utf-8")

    print("✅ Extracción completada.")
    print(f"📦 Parquet: {PARQUET_PATH}")
    print(f"📄 CSV:     {CSV_PATH}")
    print(f"📊 Filas exportadas: {len(df)}")


if __name__ == "__main__":
    main()