from src.utils.connection.airflow_connection_factory import PostgresHook as _PostgresHook
from scripts.dw.dim_number_range_template import DimNumberRangeTemplate as _DimNumberRangeTemplate
from dags.base_operator_factory import BaseOperatorFactory as _BaseOperatorFactory


class DPeriodosCursadosFactory(_BaseOperatorFactory):
    def __init__(
        self, 
        conn_output:str,
        chunksize:int=1000,
        schema:str='dw'
    ):
        self._table_name= 'd_periodos_cursados'
        self._conn_output = conn_output
        self._chunksize=chunksize
        self._schema=schema
        super().__init__(f"dag_{self._table_name}")

    def prepare_callable(self):
        hook = _PostgresHook()
        
        conn_output = hook.create_postgres_engine(self._conn_output)

        tbl = [
            {'no_coluna': 'sk_periodos_cursados', 'inicio': 1, 'fim': 31, 'passo': 1},
            {'no_coluna': 'nu_periodos_cursados', 'inicio': 0, 'fim': 30, 'passo': 1},
        ]

        return _DimNumberRangeTemplate(
            conn_output=conn_output,
            list_columns=tbl,
            table_name=self._table_name,
            schema_output=self._schema,
            chunksize=self._chunksize
        )


# factory = DPeriodosCursadosFactory('conn_dbdw')

# globals()[factory.dag_id] = factory.get_dag()
