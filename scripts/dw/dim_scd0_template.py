from sqlalchemy.engine import Engine as _Engine

from src.utils.transformers import *
from src.utils.producers.sql_to_dataframe import SQLToDataframe as _SQLToDataframe
from src.utils.consumers.dataframe_to_sql import DataframeToSQL as _DataframeToSQL

class DimSCD0Template:

    def __init__(
        self,
        conn_input:_Engine,
        conn_output:_Engine,
        table_name:str,
        sk_column:str,
        schema_output:str,
        query:str,
        dtypes:dict,
        chunksize:int
    ) -> None:
        self._conn_input = conn_input
        self._conn_output = conn_output
        self._table_name = table_name
        self._sk_column = sk_column
        self._schema_output = schema_output
        self._query = query
        self._dtypes = dtypes
        self._chunksize = chunksize
        self.init_services()

    def init_services(self):
        transformers = [
            SKTransformer(
                self._conn_output, 
                self._table_name, 
                self._sk_column, 
                self._schema_output
            ),
            DTCargaTransformer(),
            DtypesTransformer(self._dtypes),
        ]
        
        self._transform_service = TransformerService(transformers)

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

    def run(self):
        tbl = self.extract()

        tbl = self.transform(tbl)

        self.load(tbl)
