from src.utils.connection.airflow_connection_factory import PostgresHook as _PostgresHook
from scripts.stg.stg_relatorio_template import STGRelatorioTemplate
from dags.base_operator_factory import BaseOperatorFactory as _BaseOperatorFactory


class STGRelatorioFactory(_BaseOperatorFactory):
    def __init__(
        self, 
        conn_output:str, 
        base_path:str='./data',
        schema:str='stg', 
        table_name:str='stg_relatorio',
        description:str=""
    ):
        self._conn_output = conn_output
        self._base_path = base_path
        self._table_name = table_name
        self._schema = schema
        super().__init__(f"dag_{self._table_name}", description)

    def prepare_callable(self):
        hook = _PostgresHook()

        engine = hook.create_postgres_engine(self._conn_output)

        return STGRelatorioTemplate(
            conn_output=engine,
            base_path=self._base_path ,
            schema=self._schema,
            table_name=self._table_name,
        )


# factory = STGRelatorioFactory('conn_dbdw')

# globals()[factory.dag_id] = factory.get_dag()
