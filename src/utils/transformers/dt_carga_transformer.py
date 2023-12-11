import pandas as _pd
from datetime import datetime

from .base_transformer import BaseTransformer as _BaseTransformer


class DTCargaTransformer(_BaseTransformer):
    def __init__(self, dt_carga:datetime=None):
        super().__init__()
        dt_carga = dt_carga if dt_carga else datetime.now()
        if not isinstance(dt_carga, datetime):
            raise ValueError('dt_carga deve ser do tipo `datetime`')
        
        self._dt_carga = dt_carga if dt_carga else datetime.now()

    def check_request(self, dataframe:_pd.DataFrame):
        return isinstance(dataframe, _pd.DataFrame)
    
    def handle_request(self, dataframe:_pd.DataFrame):
        try:
            dataframe['dt_carga'] = self._dt_carga
        except ValueError as e:
            raise ValueError(f"Falha ao adicionar dt_carga: {e}")

        return dataframe
    