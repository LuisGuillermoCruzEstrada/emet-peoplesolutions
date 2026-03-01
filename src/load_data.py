"""
load_data.py

Carga el dataset CSV a PostgreSQL en dos tablas:
- companies(company_id, company_name)
- charges(charge_id, company_id, amount, status, created_at, paid_at)

Estrategia (idempotente):
1) Leer CSV con pandas (Extract)
2) Limpiar/convertir tipos y aplicar reglas de calidad (Transform)
3) Cargar a tablas "staging" temporales: stg_companies y stg_charges (Load intermedio)
4) Hacer UPSERT (insert/update) a tablas finales usando ON CONFLICT
   -> así puedes correrlo muchas veces sin duplicar
5) Validar con conteos finales
"""

from pathlib import Path
import pandas as pd
from db import engine

DATA_PATH = Path("data/dataset.csv")

# Reglas de calidad (las ajustamos según el EDA que hiciste)
MAX_REASONABLE_AMOUNT = 100000  # filtra outliers absurdos (10^10, 10^16, 10^34, etc.)

VALID_STATUS = {
    "paid",
    "voided",
    "pending_payment",
    "refunded",
    "charged_back",
    "pre_authorized",
    "expired",
    "partially_refunded",
}


def extract() -> pd.DataFrame:
    """Leer el CSV tal cual viene."""
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"No encuentro el archivo: {DATA_PATH.resolve()}")
    return pd.read_csv(DATA_PATH)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza + reglas de negocio para calidad de datos.

    Lo más importante:
    - id, name, company_id NO pueden venir nulos (son claves / dimensiones)
    - id no debe duplicarse
    - amount debe ser numérico y razonable
    - created_at debe convertirse a datetime
    - paid_at puede ser NULL
    - status: filtramos valores corruptos (p&0x3fid, 0xFFFF, etc.)
    """
    # 1) Validación mínima de columnas esperadas
    expected = {"id", "name", "company_id", "amount", "status", "created_at", "paid_at"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas en el CSV: {missing}")

    original_rows = len(df)

    # 2) Quitar filas con claves críticas faltantes
    df = df.dropna(subset=["id", "name", "company_id"])

    # Después de filtrar y antes de asignar columnas:
    df = df.copy()

    # Normalizar strings sin warning
    df.loc[:, "id"] = df["id"].astype(str).str.strip()
    df.loc[:, "company_id"] = df["company_id"].astype(str).str.strip()
    df.loc[:, "name"] = df["name"].astype(str).str.strip()
    df.loc[:, "status"] = df["status"].astype(str).str.strip()

    # 4) Eliminar duplicados por id (charge id)
    df = df.drop_duplicates(subset=["id"], keep="first")

    # 5) amount: asegurar numérico
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    # Si amount se volvió NaN por valores inválidos, esas filas se eliminan
    df = df.dropna(subset=["amount"])

    # 6) Filtrar montos absurdos detectados en el EDA
    df = df[(df["amount"] >= 0) & (df["amount"] < MAX_REASONABLE_AMOUNT)]

    # 7) Filtrar status inválidos/corruptos
    df = df[df["status"].isin(VALID_STATUS)]

    # 8) Fechas: convertir
    # created_at debe existir y ser parseable
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df = df.dropna(subset=["created_at"])

    # paid_at puede ser nulo (si no está pagado)
    df["paid_at"] = pd.to_datetime(df["paid_at"], errors="coerce")

    # 9) Reporte rápido de limpieza
    cleaned_rows = len(df)
    removed = original_rows - cleaned_rows
    print(f"📌 Filas originales: {original_rows}")
    print(f"🧹 Filas después de limpieza: {cleaned_rows}")
    print(f"🗑️  Filas eliminadas: {removed}")

    return df


def build_tables(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Separar el dataframe en las dos tablas destino."""

    # Aseguramos que IDs sean texto (tu dataset trae hashes)
    df["company_id"] = df["company_id"].astype(str).str.strip()
    df["id"] = df["id"].astype(str).str.strip()

    # Dimensión companies: una fila por company_id
    companies_df = (
        df[["company_id", "name"]]
        .assign(name=lambda x: x["name"].astype(str).str.strip())
        .drop_duplicates(subset=["company_id"])
        .rename(columns={"name": "company_name"})
    )

    # Hechos charges
    charges_df = (
        df.rename(columns={"id": "charge_id"})[
            ["charge_id", "company_id", "amount", "status", "created_at", "paid_at"]
        ]
        .assign(
            charge_id=lambda x: x["charge_id"].astype(str).str.strip(),
            company_id=lambda x: x["company_id"].astype(str).str.strip(),
        )
    )

    return companies_df, charges_df


def load_with_staging_and_upsert(companies_df: pd.DataFrame, charges_df: pd.DataFrame) -> None:
    """
    Carga idempotente:
    - crea/reemplaza staging
    - UPSERT a tablas finales
    - elimina staging
    """
    with engine.begin() as conn:
        # 1) Staging: reemplazamos cada corrida (no afecta tablas finales)
        companies_df.to_sql("stg_companies", conn, if_exists="replace", index=False)
        charges_df.to_sql("stg_charges", conn, if_exists="replace", index=False)

        # 2) UPSERT companies (por company_id)
        conn.exec_driver_sql("""
            INSERT INTO companies (company_id, company_name)
            SELECT company_id, company_name
            FROM stg_companies
            ON CONFLICT (company_id)
            DO UPDATE SET company_name = EXCLUDED.company_name;
        """)

        # 3) UPSERT charges (por charge_id)
        conn.exec_driver_sql("""
            INSERT INTO charges (charge_id, company_id, amount, status, created_at, paid_at)
            SELECT charge_id, company_id, amount, status, created_at, paid_at
            FROM stg_charges
            ON CONFLICT (charge_id)
            DO UPDATE SET
              company_id = EXCLUDED.company_id,
              amount = EXCLUDED.amount,
              status = EXCLUDED.status,
              created_at = EXCLUDED.created_at,
              paid_at = EXCLUDED.paid_at;
        """)

        # 4) Limpieza de staging
        conn.exec_driver_sql("DROP TABLE IF EXISTS stg_companies;")
        conn.exec_driver_sql("DROP TABLE IF EXISTS stg_charges;")


def validate_counts() -> None:
    """Conteos para verificar que ya cargó a tablas finales."""
    with engine.connect() as conn:
        companies_count = conn.exec_driver_sql("SELECT COUNT(*) FROM companies;").scalar_one()
        charges_count = conn.exec_driver_sql("SELECT COUNT(*) FROM charges;").scalar_one()
    print(f"✅ companies (final): {companies_count}")
    print(f"✅ charges (final):   {charges_count}")


def main():
    print("=== EXTRACT ===")
    df = extract()

    print("\n=== TRANSFORM ===")
    df = transform(df)

    print("\n=== SPLIT TABLES ===")
    companies_df, charges_df = build_tables(df)
    print(f"Empresas únicas a cargar: {len(companies_df)}")
    print(f"Charges a cargar:        {len(charges_df)}")

    print("\n=== LOAD (STAGING + UPSERT) ===")
    load_with_staging_and_upsert(companies_df, charges_df)

    print("\n=== VALIDATION ===")
    validate_counts()

    print("\n🚀 Listo. Puedes correr este script otra vez y NO duplicará datos.")


if __name__ == "__main__":
    main()