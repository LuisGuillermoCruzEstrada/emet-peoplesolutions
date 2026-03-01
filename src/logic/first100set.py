class First100Set:
    """
    Representa el conjunto de los primeros 100 números naturales.
    """

    def __init__(self):
        self.numbers = list(range(1, 101))

    def extract(self, n):
        """
        Extrae un número del conjunto.
        """
        if not isinstance(n, int):
            raise ValueError("El número debe ser entero")

        if n < 1 or n > 100:
            raise ValueError("El número debe estar entre 1 y 100")

        if n not in self.numbers:
            raise ValueError("El número ya fue extraído")

        self.numbers.remove(n)

    def find_missing(self):
        """
        Calcula el número faltante.
        """
        total_expected = sum(range(1, 101))
        current_sum = sum(self.numbers)

        return total_expected - current_sum