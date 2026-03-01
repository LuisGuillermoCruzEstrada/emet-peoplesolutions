import pandas as pd

def test_full_etl_pipeline(tmp_path, monkeypatch):
    import extract_to_file as ex
    import transform_to_cargo as tr

    # Fake extracción
    fake_df = pd.DataFrame(
        {
            "charge_id": ["c1"],
            "company_id": ["comp1"],
            "company_name": ["A"],
            "amount": [10.999],
            "status": ["paid"],
            "created_at": ["2024-01-01"],
            "paid_at": ["2024-01-02"],
        }
    )
    monkeypatch.setattr(ex.pd, "read_sql_query", lambda query, engine: fake_df)

    # Redirigir exports a tmp
    monkeypatch.setattr(ex, "EXPORT_DIR", tmp_path)
    monkeypatch.setattr(ex, "PARQUET_PATH", tmp_path / "charges_enriched.parquet")
    monkeypatch.setattr(ex, "CSV_PATH", tmp_path / "charges_enriched.csv")

    monkeypatch.setattr(tr, "EXPORT_DIR", tmp_path)
    monkeypatch.setattr(tr, "INPUT_PARQUET", tmp_path / "charges_enriched.parquet")
    monkeypatch.setattr(tr, "OUTPUT_PARQUET", tmp_path / "cargo_transformed.parquet")
    monkeypatch.setattr(tr, "OUTPUT_CSV", tmp_path / "cargo_transformed.csv")

    # correr pipeline
    ex.main()
    tr.main()

    df = pd.read_parquet(tmp_path / "cargo_transformed.parquet")
    assert len(df) == 1
    assert df.loc[0, "amount"] == 11.00  # 10.999 -> 11.00