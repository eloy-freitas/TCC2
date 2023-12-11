import pandas as _pd
from sqlalchemy import text

from .base_transformer import BaseTransformer as _BaseTransformer


class DataframeToUpdate(_BaseTransformer):
    def __init__(
        self,
        columns_pk:list[str],
        columns_update:list[str],
        table_name:str,
        schema:str
     ):
        super().__init__()
        self._columns = columns_pk + columns_update
        self._columns_pk = columns_pk
        self._columns_update = columns_update
        self._table_name = table_name
        self._schema = schema

    def check_request(self, dataframe:_pd.DataFrame):
        return isinstance(dataframe, _pd.DataFrame)
    
    def handle_request(self, dataframe:_pd.DataFrame):
        try:
            update_template = "UPDATE {SCHEMA}.{TABLE_NAME} SET {VALUES} WHERE {WHERE}"
            rows = dataframe.T.to_dict()
            updates = []
            for row in rows.items():
                row = row[1] 
                where = [f"{k} = :{k}" for k,v in row.items() if k in self._columns_pk]
                sets = [f"{k} = :{k}" for k,v in row.items() if k in self._columns_update]

                where_str = "\nAND ".join(where)
                set_str = "\n, ".join(sets)

                update = text(update_template.format(
                    SCHEMA=self._schema,
                    TABLE_NAME=self._table_name,
                    VALUES=set_str,
                    WHERE=where_str
                ))
                
                updates.append((row, update))
            
        except TypeError as e:
            raise TypeError(f"Falha ao filtrar colunas: {e}")

        return updates
    