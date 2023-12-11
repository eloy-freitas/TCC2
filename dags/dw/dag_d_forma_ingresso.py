from src.utils.connection.airflow_connection_factory import PostgresHook as _PostgresHook
from scripts.dw.dim_scd0_template import DimSCD0Template as _DimSCD0Template
from dags.base_operator_factory import BaseOperatorFactory as _BaseOperatorFactory


class DFormaIngressoFactory(_BaseOperatorFactory):
    def __init__(
        self, 
        conn_input:str, 
        conn_output:str,
        chunksize:int=1000,
        schema:str='dw'
    ):
        self._table_name= 'd_forma_ingresso'
        self._conn_input = conn_input
        self._conn_output = conn_output
        self._chunksize=chunksize
        self._schema=schema
        super().__init__(f"dag_{self._table_name}")

    def prepare_callable(self):
        hook = _PostgresHook()

        conn_input = hook.create_postgres_engine(self._conn_input)
        
        conn_output = hook.create_postgres_engine(self._conn_output)

        self._sk_column = 'sk_forma_ingresso'
        self._dtypes = {'ds_forma_ingresso': 'string'}
        query = """
            select distinct ds_forma_ingresso from stg.v_ds_stg_relatorio
            except
            select ds_forma_ingresso from dw.d_forma_ingresso
        """

        return _DimSCD0Template(
            conn_input=conn_input,
            conn_output=conn_output,
            table_name=self._table_name,
            schema_output=self._schema,
            sk_column=self._sk_column,
            query=query, 
            dtypes=self._dtypes,
            chunksize=self._chunksize
        )


# factory = DFormaIngressoFactory('conn_dbdw', 'conn_dbdw')

# globals()[factory.dag_id] = factory.get_dag()
