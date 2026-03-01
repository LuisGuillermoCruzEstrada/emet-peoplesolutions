import pandas as pd
from pathlib import Path

def test_extract_creates_files(tmp_path, monkeypatch):
    # Import aquí para poder monkeypatchear globals del módulo
    import extract_to_file as m

    # 1) DataFrame fake como si viniera de Postgres
    fake_df = pd.DataFrame(
        {
            "charge_id": ["c1", "c2"],
            "company_id": ["comp1", "comp2"],
            "company_name": ["A", "B"],
            "amount": [10.111, 20.999],
            "status": ["paid", "pending"],
            "created_at": ["2024-01-01", "2024-01-02"],
            "paid_at": ["2024-01-03", None],
        }
    )

    # 2) Mock: que read_sql_query NO pegue a DB
    monkeypatch.setattr(m.pd, "read_sql_query", lambda query, engine: fake_df)

    # 3) Redirigir exports a un tmp_path
    monkeypatch.setattr(m, "EXPORT_DIR", tmp_path)
    monkeypatch.setattr(m, "PARQUET_PATH", tmp_path / "charges_enriched.parquet")
    monkeypatch.setattr(m, "CSV_PATH", tmp_path / "charges_enriched.csv")

    # 4) Ejecutar
    m.main()

    assert (tmp_path / "charges_enriched.parquet").exists()
    assert (tmp_path / "charges_enriched.csv").exists()

def test_extract_schema(tmp_path, monkeypatch):
    import extract_to_file as m

    fake_df = pd.DataFrame(
        {
            "charge_id": ["c1"],
            "company_id": ["comp1"],
            "company_name": ["A"],
            "amount": [10.0],
            "status": ["paid"],
            "created_at": ["2024-01-01"],
            "paid_at": ["2024-01-02"],
        }
    )

    monkeypatch.setattr(m.pd, "read_sql_query", lambda query, engine: fake_df)
    monkeypatch.setattr(m, "EXPORT_DIR", tmp_path)
    monkeypatch.setattr(m, "PARQUET_PATH", tmp_path / "charges_enriched.parquet")
    monkeypatch.setattr(m, "CSV_PATH", tmp_path / "charges_enriched.csv")

    m.main()
    df = pd.read_parquet(tmp_path / "charges_enriched.parquet")

    expected = [
        "charge_id", "company_id", "company_name",
        "amount", "status", "created_at", "paid_at"
    ]
    assert list(df.columns) == expected