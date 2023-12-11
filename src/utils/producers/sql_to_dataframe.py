import pandas as _pd
from sqlalchemy.exc import SQLAlchemyError as _SQLAlchemyError
from sqlalchemy.engine import Engine as _Engine
from .base_producer import BaseProducer

class SQLToDataframe(BaseProducer):

    def __init__(
        self, 
        conn_input:_Engine, 
        query:str
    ) -> None:
        if not isinstance(conn_input, _Engine):
            raise TypeError('Objeto de conexão inválido')
        if not isinstance(query, str):
            raise TypeError('Query deve ser uma string')
        
        self._conn_input = conn_input
        self._query = query

    def extract(self):
        try:  
            return _pd.read_sql_query(sql=self._query, con=self._conn_input)
        except _SQLAlchemyError as e:
            raise _SQLAlchemyError(f"Falha ao extrair dados: {e}")