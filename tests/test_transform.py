import pandas as pd
from pathlib import Path

def test_transform_creates_files(tmp_path, monkeypatch):
    import transform_to_cargo as m

    # 1) Crear input parquet fake (charges_enriched.parquet)
    input_df = pd.DataFrame(
        {
            "charge_id": [" 123 ", "456"],
            "company_id": [" comp1 ", "comp2"],
            "company_name": [" A ", "B"],
            "amount": [10.129, 20.999],  # mezcla int/str para probar coerción
            "status": [" paid ", "pending"],
            "created_at": ["2024-01-01", "2024-01-02"],
            "paid_at": ["2024-01-03", None],
        }
    )

    input_path = tmp_path / "charges_enriched.parquet"
    input_df.to_parquet(input_path, index=False)

    # 2) Redirigir paths del módulo a tmp_path
    monkeypatch.setattr(m, "EXPORT_DIR", tmp_path)
    monkeypatch.setattr(m, "INPUT_PARQUET", input_path)
    monkeypatch.setattr(m, "OUTPUT_PARQUET", tmp_path / "cargo_transformed.parquet")
    monkeypatch.setattr(m, "OUTPUT_CSV", tmp_path / "cargo_transformed.csv")

    # 3) Ejecutar
    m.main()

    assert (tmp_path / "cargo_transformed.parquet").exists()
    assert (tmp_path / "cargo_transformed.csv").exists()

def test_transform_schema_and_types(tmp_path, monkeypatch):
    import transform_to_cargo as m

    input_df = pd.DataFrame(
    {
        "charge_id": [" 123 ", "456"],
        "company_id": [" comp1 ", "comp2"],
        "company_name": [" A ", "B"],
        "amount": [10.129, 20.999],  # <-- NUMÉRICO, sin strings
        "status": [" paid ", "pending"],
        "created_at": ["2024-01-01", "2024-01-02"],
        "paid_at": ["2024-01-03", None],
    }
    )

    input_path = tmp_path / "charges_enriched.parquet"
    input_df.to_parquet(input_path, index=False)

    monkeypatch.setattr(m, "EXPORT_DIR", tmp_path)
    monkeypatch.setattr(m, "INPUT_PARQUET", input_path)
    monkeypatch.setattr(m, "OUTPUT_PARQUET", tmp_path / "cargo_transformed.parquet")
    monkeypatch.setattr(m, "OUTPUT_CSV", tmp_path / "cargo_transformed.csv")

    m.main()

    df = pd.read_parquet(tmp_path / "cargo_transformed.parquet")

    expected_cols = ["id", "company_name", "company_id", "amount", "status", "created_at", "updated_at"]
    assert list(df.columns) == expected_cols

    # trims
    assert df.loc[0, "id"] == "123"
    assert df.loc[0, "company_id"] == "comp1"
    assert df.loc[0, "company_name"] == "A"
    assert df.loc[0, "status"] == "paid"

    # amount redondeado
    assert float(df.loc[0, "amount"]) == 10.13

    # datetimes
    assert pd.api.types.is_datetime64_any_dtype(df["created_at"])
    assert pd.api.types.is_datetime64_any_dtype(df["updated_at"])