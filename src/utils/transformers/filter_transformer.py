import pandas as _pd

from .base_transformer import BaseTransformer as _BaseTransformer


class FilterTransformer(_BaseTransformer):
    def __init__(self, columns:dict):
        super().__init__()
        self._columns = columns

    def check_request(self, dataframe:_pd.DataFrame):
        return isinstance(dataframe, _pd.DataFrame)
    
    def handle_request(self, dataframe:_pd.DataFrame):
        try:
            dataframe = dataframe.filter(items=self._columns)
        except TypeError as e:
            raise TypeError(f"Falha ao filtrar colunas: {e}")

        return dataframe
    