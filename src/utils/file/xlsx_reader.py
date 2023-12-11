import pandas as pd

class XLSXReader:
    def __init__(self, path) -> None:
        self._path = path

    def execute(self):
        try:
            tbl = pd.read_excel(self._path)
        except IOError as e:
            raise IOError(f"Falha na leitura do arquivo: {e}")

        return tbl