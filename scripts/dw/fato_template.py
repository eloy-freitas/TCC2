from sqlalchemy.engine import Engine as _Engine

from src.utils.transformers import *
from src.utils.producers.sql_to_dataframe import SQLToDataframe as _SQLToDataframe
from src.utils.consumers.dataframe_to_sql import DataframeToSQL as _DataframeToSQL
from src.utils.table.table_manager import TableManager as _TableManager


class FatoTemplate:

    def __init__(
        self,
        conn_input:_Engine,
        conn_output:_Engine,
        table_name:str,
        schema_output:str,
        query:str,
        dtypes:dict,
        chunksize:int,
        query_delete:str
    ) -> None:
        self._conn_input = conn_input
        self._conn_output = conn_output
        self._table_name = table_name
        self._schema_output = schema_output
        self._query = query
        self._dtypes = dtypes
        self._chunksize = chunksize
        self._query_delete = query_delete
        self.init_services()

    def init_services(self):
        transformers = [
            DtypesTransformer(self._dtypes),
            DTCargaTransformer()
        ]
        
        self._transform_service = TransformerService(transformers)
        self._table_manager = _TableManager()

    def extract(self):
        return _SQLToDataframe(self._conn_input, self._query).extract()

    def transform(self, dataframe):
        return self._transform_service.handle_request(dataframe)

    def load(self, dataframe):
        _DataframeToSQL(
            dataframe=dataframe,
            conn_output=self._conn_output,
            table_name=self._table_name, 
            schema=self._schema_output,
            chunksize=self._chunksize
        ).load()

    def delete(self):
        self._table_manager.execute_query(self._conn_output, self._query_delete)

    def run(self):
        self.delete()

        tbl = self.extract()

        tbl = self.transform(tbl)

        self.load(tbl)
        