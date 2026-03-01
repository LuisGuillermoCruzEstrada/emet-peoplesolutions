"""
explore_dataset.py

Explora el dataset antes de cargarlo a la base de datos.
Esto permite detectar:

- valores faltantes
- duplicados
- valores extraños
- problemas de formato
"""

import pandas as pd
from pathlib import Path

DATA_PATH = Path("data/dataset.csv")


def main():

    df = pd.read_csv(DATA_PATH)

    print("\n========== INFO GENERAL ==========")
    print(df.info())

    print("\n========== PRIMERAS FILAS ==========")
    print(df.head())

    print("\n========== VALORES FALTANTES ==========")
    print(df.isna().sum())

    print("\n========== DUPLICADOS POR ID ==========")
    print(df.duplicated(subset=["id"]).sum())

    print("\n========== ESTADISTICAS NUMERICAS ==========")
    print(df.describe())

    print("\n========== STATUS UNICOS ==========")
    print(df["status"].value_counts())

    print("\n========== RANGO DE FECHAS ==========")
    print("created_at min:", df["created_at"].min())
    print("created_at max:", df["created_at"].max())


if __name__ == "__main__":
    main()