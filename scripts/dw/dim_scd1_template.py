from sqlalchemy.engine import Engine as _Engine

from src.utils.transformers import *
from src.utils.producers.sql_to_dataframe import SQLToDataframe as _SQLToDataframe
from src.utils.consumers.dataframe_to_sql import DataframeToSQL as _DataframeToSQL
from src.utils.table.table_manager import TableManager as _TableManager

class DimSCD1Template:

    def __init__(
        self,
        conn_input:_Engine,
        conn_output:_Engine,
        table_name:str,
        sk_column:str,
        schema_output:str,
        query_insert:str,
        dtypes:dict,
        chunksize:int,
        columns_update:list[str],
        columns_pk:list[str],
        query_update:str
    ) -> None:
        self._conn_input = conn_input
        self._conn_output = conn_output
        self._table_name = table_name
        self._sk_column = sk_column
        self._schema_output = schema_output
        self._query_insert = query_insert
        self._dtypes = dtypes
        self._chunksize = chunksize
        self._transformers = []
        self._query_update = query_update
        self._columns_pk = columns_pk
        self._columns_update = columns_update  
        self._select_columns = self._columns_pk + self._columns_update
        self._tbl_manager = _TableManager()

    def init_services(self, update=False):
        if update:
            transformers = [
                DTCargaTransformer(),
                DtypesTransformer(self._dtypes),
                FilterTransformer(self._select_columns),
                DataframeToUpdate(
                    self._columns_pk, 
                    self._columns_update, 
                    self._table_name, 
                    self._schema_output
                )
            ]            
        else:
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
        self.init_services()
        return _SQLToDataframe(self._conn_input, self._query_insert).extract()

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

    def update(self):
        self.init_services(update=True)

        tbl = _SQLToDataframe(self._conn_input, self._query_update).extract()
        
        updates = self.transform(tbl)
        
        if updates:
            for payload, query in updates: 
                self._tbl_manager.execute_update(self._conn_output, query, payload)

    def run(self):
        tbl = self.extract()
        tbl = self.transform(tbl)
        self.load(tbl)
        self.update()
