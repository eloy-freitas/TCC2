from src.utils.connection.airflow_connection_factory import PostgresHook as _PostgresHook
from scripts.dw.dim_scd1_template import DimSCD1Template as _DimSCD1Template
from dags.base_operator_factory import BaseOperatorFactory as _BaseOperatorFactory


class DPessoaFactory(_BaseOperatorFactory):
    def __init__(
        self, 
        conn_input:str, 
        conn_output:str,
        chunksize:int=1000,
        schema:str='dw'
    ):
        self._table_name = 'd_pessoa'
        self._conn_input = conn_input
        self._conn_output = conn_output
        self._chunksize = chunksize
        self._schema = 'dw'
        self._schema=schema
        super().__init__(f"dag_{self._table_name}")

    def prepare_callable(self):
        hook = _PostgresHook()

        conn_input = hook.create_postgres_engine(self._conn_input)
        
        conn_output = hook.create_postgres_engine(self._conn_output)

        sk_column = 'sk_pessoa'
        dtypes = {
            'nu_cpf': 'string',
            'no_pessoa': 'string',
            'ds_genero': 'string',
            'dt_nascimento': 'datetime64[ns]',
            'ds_deficiencia': 'string',
            'ds_etinia': 'string',
            'ds_estado_civil': 'string',
            'no_municipio_naturalidade': 'string',
            'no_municipio_origem': 'string',
            'cd_uf': 'string',
        }
        columns_pk = ['nu_cpf']
        columns_update = [
            'no_pessoa',
            'ds_genero',
            'dt_nascimento',
            'ds_deficiencia',
            'ds_etinia',
            'ds_estado_civil',
            'no_municipio_naturalidade',
            'no_municipio_origem',
            'cd_uf',
            'dt_carga'
        ]

        query_insert = """
            select distinct 
                r.nu_cpf,
                coalesce(r.no_pessoa, 'Não Informado') no_pessoa,
                coalesce(r.ds_genero, 'Não Informado') ds_genero,
                coalesce(r.dt_nascimento, to_date('1900-01-01', 'yyyy-mm-dd')) dt_nascimento,
                coalesce(r.ds_deficiencia, 'Não Informado') ds_deficiencia,
                coalesce(r.ds_etinia, 'Não Informado') ds_etinia,
                coalesce(r.ds_estado_civil, 'Não Informado') ds_estado_civil,
                coalesce(r.no_municipio_naturalidade, 'Não Informado') no_municipio_naturalidade,
                coalesce(r.no_municipio_origem, 'Não Informado') no_municipio_origem,
                coalesce(r.cd_uf, '-2') cd_uf
            from stg.v_ds_stg_relatorio r
            left join dw.d_pessoa dc
                on r.nu_cpf = dc.nu_cpf
            where dc.nu_cpf is null
                and r.nu_cpf is not null
        """

        query_update = """
            with updates as (
                select distinct 
                    r.nu_cpf,
                    r.no_pessoa,
                    r.ds_genero,
                    r.dt_nascimento,
                    r.ds_deficiencia,
                    r.ds_etinia,
                    r.ds_estado_civil,
                    r.no_municipio_naturalidade,
                    r.no_municipio_origem,
                    r.cd_uf
                from stg.v_ds_stg_relatorio r
                except
                select distinct 
                    dc.nu_cpf,
                    dc.no_pessoa,
                    dc.ds_genero,
                    dc.dt_nascimento,
                    dc.ds_deficiencia,
                    dc.ds_etinia,
                    dc.ds_estado_civil,
                    dc.no_municipio_naturalidade,
                    dc.no_municipio_origem,
                    dc.cd_uf
                from dw.d_pessoa dc 
            )
            select distinct 
                r.nu_cpf,
                coalesce(r.no_pessoa, 'Não Informado') no_pessoa,
                coalesce(r.ds_genero, 'Não Informado') ds_genero,
                coalesce(r.dt_nascimento, to_date('1900-01-01', 'yyyy-mm-dd')) dt_nascimento,
                coalesce(r.ds_deficiencia, 'Não Informado') ds_deficiencia,
                coalesce(r.ds_etinia, 'Não Informado') ds_etinia,
                coalesce(r.ds_estado_civil, 'Não Informado') ds_estado_civil,
                coalesce(r.no_municipio_naturalidade, 'Não Informado') no_municipio_naturalidade,
                coalesce(r.no_municipio_origem, 'Não Informado') no_municipio_origem,
                coalesce(r.cd_uf, '-2') cd_uf
            from stg.v_ds_stg_relatorio r
            inner join updates dc
                on r.nu_cpf = dc.nu_cpf
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

# factory = DPessoaFactory('conn_dbdw', 'conn_dbdw')

# globals()[factory.dag_id] = factory.get_dag()
