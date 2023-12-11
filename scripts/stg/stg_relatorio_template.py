from sqlalchemy.engine import Engine as _Engine
from sqlalchemy.exc import SQLAlchemyError as _SQLAlchemyError

from src.utils.file.xlsx_reader import XLSXReader as _XLSXReader
from src.utils.table.table_manager import TableManager as _TableManager
from src.utils.consumers.dataframe_to_sql import DataframeToSQL as _DataframeToSQL
from src.utils.file.file_utils import FileUtils as _FileUtils


class STGRelatorioTemplate:
    def __init__(
        self, 
        base_path:str,
        conn_output:_Engine,
        table_name:str,
        schema:str,
        chunksize:int=1000,
        pattern:str='*'
    ):
        self._base_path = base_path
        self._conn_output = conn_output
        self._table_name = table_name
        self._chunksize = chunksize
        self._schema = schema
        self._manager = _TableManager()
        self._file_utils = _FileUtils()
        self._patterh = pattern

    def extract(self, path):
        tbl = None
        try:
            tbl = _XLSXReader(path).execute()
        except IOError as e:
            raise IOError(e)

        return tbl
    
    def load(self, dataframe):
        consumer = _DataframeToSQL(
            table_name=self._table_name,
            conn_output=self._conn_output,
            dataframe=dataframe,
            chunksize=1000,
            schema=self._schema
        )
        
        try:
            consumer.load()
        except _SQLAlchemyError as e:
            raise _SQLAlchemyError(e) 

    def run(self):
        paths = self._file_utils.get_xlsx_from_path(self._base_path, self._patterh)

        self._manager.truncate_table(self._conn_output, self._table_name, self._schema)

        for path in paths:
            try:
                tbl = self.extract(path)
                self.load(tbl)
            except _SQLAlchemyError as e:
                raise _SQLAlchemyError(e)
            except IOError as io:
                raise IOError(io)
                