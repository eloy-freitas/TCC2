import pandas as _pd

from src.utils.table.table_manager import TableManager as _TableManager
from .base_transformer import BaseTransformer as _BaseTransformer


class SKTransformer(_BaseTransformer):
    def __init__(self, conn, table_name, sk_column, schema):
        super().__init__()
        self._conn = conn
        self._table_name = table_name
        self._sk_column = sk_column
        self._schema = schema

    def check_request(self, dataframe:_pd.DataFrame):
        return isinstance(dataframe, _pd.DataFrame)
    
    def handle_request(self, dataframe:_pd.DataFrame):
        try:
            max = _TableManager().get_max(
                conn=self._conn,
                table_name=self._table_name,
                column=self._sk_column,
                schema=self._schema
            )

            if max <= 0:
                max = 1
                
            dataframe_size = dataframe.shape[0]
            
            sk_range = range(max, max + dataframe_size)

            dataframe[self._sk_column] = sk_range
        except ValueError as e:
            raise ValueError(f"Falha ao inserir {self._sk_column}: {e}")

        return dataframe
    