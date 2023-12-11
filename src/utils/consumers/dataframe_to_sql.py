import pandas as _pd
from sqlalchemy.engine import Engine as _Engine
from sqlalchemy.exc import SQLAlchemyError as _SQLAlchemyError

class DataframeToSQL:

    def __init__(
        self, 
        dataframe: _pd.DataFrame, 
        conn_output:_Engine, 
        table_name:str,
        chunksize:int,
        schema:str,
    ):
        if not isinstance(dataframe, _pd.DataFrame):
            raise ValueError("Dataset não é um dataframe")

        if not isinstance(conn_output, _Engine):
            raise ValueError("Objeto de conexão inválido")
    
        if not isinstance(table_name, str):
            raise ValueError("Nome da tabela deve ser uma string")
        
        if not isinstance(schema, str):
            raise ValueError("Schema deve ser uma string")

        if not isinstance(chunksize, int):
            raise ValueError("Cunksize deve ser um inteiro")

        self._dataframe = dataframe
        self._conn_output = conn_output
        self._table_name = table_name
        self._chunksize = chunksize
        self._schema = schema
        

    def load(self):
        try:
            with self._conn_output.connect() as conn:
                with conn.begin():
                    self._dataframe.to_sql(
                        name=self._table_name,
                        con=conn,
                        index=False,
                        if_exists='append',
                        chunksize=self._chunksize,
                        schema=self._schema
                    )
        except _SQLAlchemyError as e:
            raise _SQLAlchemyError(f"Falha ao carregar dados: {e}")