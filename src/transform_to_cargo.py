"""
transform_to_cargo.py

Punto 1.3 - Transformación:

Toma el archivo extraído (charges_enriched.parquet) y lo transforma al
esquema "Cargo" solicitado:

- id (varchar 24)
- company_name
- company_id (varchar 24)
- amount decimal(16,2)
- status
- created_at
- updated_at (timestamp NULL)

Decisión:
- updated_at se mappea desde paid_at (si existe). Si no, queda NULL.
"""

from pathlib import Path
import pandas as pd

EXPORT_DIR = Path("exports")

INPUT_PARQUET = EXPORT_DIR / "charges_enriched.parquet"
OUTPUT_PARQUET = EXPORT_DIR / "cargo_transformed.parquet"
OUTPUT_CSV = EXPORT_DIR / "cargo_transformed.csv"


def main():
    if not INPUT_PARQUET.exists():
        raise FileNotFoundError(f"No encuentro el archivo: {INPUT_PARQUET.resolve()}")

    # 1) Leemos el dataset enriquecido (ya viene de BD)
    df = pd.read_parquet(INPUT_PARQUET)

    # 2) Renombramos/mapeamos columnas al esquema requerido
    # charge_id -> id
    # paid_at -> updated_at
    df = df.rename(columns={
        "charge_id": "id",
        "paid_at": "updated_at",
    })

    # 3) Asegurar tipos / normalizar
    # IDs y company_id son strings (hashes)
    df["id"] = df["id"].astype(str).str.strip()
    df["company_id"] = df["company_id"].astype(str).str.strip()
    df["company_name"] = df["company_name"].astype(str).str.strip()
    df["status"] = df["status"].astype(str).str.strip()

    # Asegurar que id y company_id cumplan "varchar 24" según el spec:
    # (no modificamos el contenido, solo validamos y, si excede, lo reportamos)
    too_long_id = (df["id"].str.len() > 24).sum()
    too_long_company = (df["company_id"].str.len() > 24).sum()

    # Amount con 2 decimales (pandas float -> redondeo)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").round(2)

    # Fechas a datetime (por si se degradaron al leer)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

    # 4) Reordenar columnas EXACTO como pide el esquema
    df = df[[
        "id",
        "company_name",
        "company_id",
        "amount",
        "status",
        "created_at",
        "updated_at",
    ]]

    # 5) Guardar transformado
    df.to_parquet(OUTPUT_PARQUET, index=False)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

    print("✅ Transformación completada (esquema Cargo).")
    print(f"📦 Parquet: {OUTPUT_PARQUET}")
    print(f"📄 CSV:     {OUTPUT_CSV}")
    print(f"📊 Filas:   {len(df)}")

    # 6) Reporte de validación de longitudes (solo informativo)
    if too_long_id or too_long_company:
        print("\n⚠️ Aviso de validación (varchar 24):")
        print(f" - ids con longitud > 24: {too_long_id}")
        print(f" - company_id con longitud > 24: {too_long_company}")
        print("   (No se truncó nada para no perder información; se reporta para documentarlo.)")


if __name__ == "__main__":
    main()