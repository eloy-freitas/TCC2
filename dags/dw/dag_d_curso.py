from src.utils.connection.airflow_connection_factory import PostgresHook as _PostgresHook
from scripts.dw.dim_scd1_template import DimSCD1Template as _DimSCD1Template
from dags.base_operator_factory import BaseOperatorFactory as _BaseOperatorFactory


class DCursoFactory(_BaseOperatorFactory):
    def __init__(
        self, 
        conn_input:str, 
        conn_output:str,
        chunksize:int=1000,
        schema:str='dw'
    ):
        self._table_name = 'd_curso'
        self._conn_input = conn_input
        self._conn_output = conn_output
        self._chunksize = chunksize
        self._schema = schema
        super().__init__(f"dag_{self._table_name}")

    def prepare_callable(self):
        hook = _PostgresHook()

        conn_input = hook.create_postgres_engine(self._conn_input)
        
        conn_output = hook.create_postgres_engine(self._conn_output)

        sk_column = 'sk_curso'
        dtypes = {
            'cd_curso': 'Int64',
            'no_curso': 'string',
            'no_centro_academico': 'string'
        }
        columns_pk = ['cd_curso']
        columns_update = ['no_curso', 'no_centro_academico', 'dt_carga']

        query_insert = """
           select distinct 
                r.cd_curso
                , r.no_curso 
                , r.no_centro_academico 
            from stg.v_ds_stg_relatorio r
            left join dw.d_curso dc
                on r.cd_curso = dc.cd_curso
            where dc.cd_curso is null
        """

        query_update = """
            with updates as (
                select distinct 
                    r.cd_curso
                    , r.no_curso 
                    , r.no_centro_academico 
                from stg.v_ds_stg_relatorio r 
                except
                select distinct 
                    dc.cd_curso
                    , dc.no_curso 
                    , dc.no_centro_academico 
                from dw.d_curso dc 
            )
            select distinct 
                r.cd_curso
                , r.no_curso 
                , r.no_centro_academico 
            from stg.v_ds_stg_relatorio r
            inner join updates dc
                on r.cd_curso = dc.cd_curso
        """



        return _DimSCD1Template(
           conn_input=conn_input,
           conn_output=conn_output,
           table_name=self._table_name,
           chunksize=self._chunksize,
           columns_pk=columns_pk,
           columns_update=columns_update,
           query_update=query_update,
           query_insert=query_insert,
           dtypes=dtypes,
           schema_output=self._schema,
           sk_column=sk_column,
        )


# factory = DCursoFactory('conn_dbdw', 'conn_dbdw')

# globals()[factory.dag_id] = factory.get_dag()
