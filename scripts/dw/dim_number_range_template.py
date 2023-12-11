from sqlalchemy.engine import Engine as _Engine

from src.utils.transformers import *
from src.utils.producers.range_table_factory import RangeTableFactory as _RangeTableFactory
from src.utils.consumers.dataframe_to_sql import DataframeToSQL as _DataframeToSQL
from src.utils.table.table_manager import TableManager as _TableManager

class DimNumberRangeTemplate:

    def __init__(
        self,
        conn_output:_Engine,
        list_columns: list[dict],
        table_name:str,
        schema_output:str,
        chunksize:int
    ) -> None:
        self._conn_output = conn_output
        self._table_name = table_name
        self._schema_output = schema_output
        self._chunksize = chunksize
        self._list_columns = list_columns
        self.init_services()

    def init_services(self):
        transformers = [
            NumberRangeTransformers(),
            DTCargaTransformer()
        ]
        
        self._transform_service = TransformerService(transformers)

    def extract(self):
        return _RangeTableFactory(self._list_columns).extract()

    def transform(self, dataframe):
        return self._transform_service.handle_request(dataframe)

    def load(self, dataframe):
        count = _TableManager().count(self._conn_output, self._table_name, self._schema_output)
        if count == 3:
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
