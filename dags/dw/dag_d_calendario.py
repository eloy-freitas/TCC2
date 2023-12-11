from src.utils.connection.airflow_connection_factory import PostgresHook as _PostgresHook
from scripts.dw.dim_scd0_template import DimSCD0Template as _DimSCD0Template
from dags.base_operator_factory import BaseOperatorFactory as _BaseOperatorFactory


class DCalendarioFactory(_BaseOperatorFactory):
    def __init__(
        self, 
        conn_input:str, 
        conn_output:str,
        chunksize:int=1000,
        schema:str='dw'
    ):
        self._table_name= 'd_calendario'
        self._conn_input = conn_input
        self._conn_output = conn_output
        self._chunksize=chunksize
        self._schema=schema
        super().__init__(f"dag_{self._table_name}")

    def prepare_callable(self):
        hook = _PostgresHook()

        conn_input = hook.create_postgres_engine(self._conn_input)
        
        conn_output = hook.create_postgres_engine(self._conn_output)

        self._sk_column = 'sk_calendario'
        self._dtypes = {
			'nu_ano_referencia': 'int',
			'ds_periodo_referencia': 'string'
		}

        query = """
            select 
				nu_ano_ingresso nu_ano_referencia
				, ds_periodo_ingresso ds_periodo_referencia
			FROM stg.v_ds_stg_relatorio vdsr 
			where nu_ano_ingresso > 0 
				and nu_ano_ingresso is not null
				and ds_periodo_evasao <> 'Não Aplicável'
			UNION
			select 
				nu_ano_evasao  nu_ano_referencia
				, ds_periodo_evasao ds_periodo_referencia
			FROM stg.v_ds_stg_relatorio vdsr
			where nu_ano_evasao  > 0 
				and nu_ano_evasao  is not null
				and ds_periodo_evasao <> 'Não Aplicável'
			except 
			select 
				nu_ano_referencia
				, ds_periodo_referencia
			from dw.d_calendario sr 
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


# factory = DCalendarioFactory('conn_dbdw', 'conn_dbdw')

# globals()[factory.dag_id] = factory.get_dag()
