"""
run_cli.py

Ejecución desde terminal usando argumento.
Demuestra el cálculo del número faltante.
"""

import sys
from logic.first100set import First100Set


def main():

    if len(sys.argv) != 2:
        print("Uso: python src/run_cli.py <numero_1_a_100>")
        sys.exit(1)

    raw = sys.argv[1]

    try:
        n = int(raw)
    except ValueError:
        print("Error: el argumento debe ser un número entero.")
        sys.exit(1)

    s = First100Set()

    try:
        s.extract(n)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    missing = s.find_missing()

    # suma esperada de 1..100
    expected_sum = sum(range(1, 101))

    # suma actual del conjunto
    current_sum = sum(s.numbers)

    print("\n=== DEMOSTRACIÓN DEL CÁLCULO ===")
    print(f"Suma esperada (1..100): {expected_sum}")
    print(f"Suma del conjunto actual: {current_sum}")

    print("\n=== RESULTADO ===")
    print(f"✅ Número extraído: {n}")
    print(f"🔎 Número faltante calculado: {missing}")


if __name__ == "__main__":
    main()