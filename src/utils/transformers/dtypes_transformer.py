import pandas as _pd
from datetime import datetime

from .base_transformer import BaseTransformer as _BaseTransformer


class DtypesTransformer(_BaseTransformer):
    def __init__(self, dtypes:dict):
        super().__init__()
        self._dtypes = dtypes

    def check_request(self, dataframe:_pd.DataFrame):
        return isinstance(dataframe, _pd.DataFrame)
    
    def handle_request(self, dataframe:_pd.DataFrame):
        try:
            dataframe = dataframe.astype(self._dtypes)
        except TypeError as e:
            raise TypeError(f"Falha converter dados: {e}")

        return dataframe
    