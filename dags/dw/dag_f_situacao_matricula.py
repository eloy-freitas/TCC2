from src.utils.connection.airflow_connection_factory import PostgresHook as _PostgresHook
from scripts.dw.fato_template import FatoTemplate as _FatoTemplate
from dags.base_operator_factory import BaseOperatorFactory as _BaseOperatorFactory


class FSituacaoMatriculaFactory(_BaseOperatorFactory):
    def __init__(
        self, 
        conn_input:str, 
        conn_output:str,
        chunksize:int=1000,
        schema:str='dw'
    ):
        self._table_name= 'f_situacao_matricula'
        self._conn_input = conn_input
        self._conn_output = conn_output
        self._chunksize=chunksize
        self._schema=schema
        super().__init__(f"dag_{self._table_name}")

    def prepare_callable(self):
        hook = _PostgresHook()

        conn_input = hook.create_postgres_engine(self._conn_input)
        
        conn_output = hook.create_postgres_engine(self._conn_output)

        self._dtypes = {
            'nu_matricula': 'string',
            #'nu_ano_ingresso': 'Int64',
            #'ds_periodo_ingresso': 'string',
            'nu_cra': 'float64',
            'nu_crn': 'float64',
            'fl_cotista': 'string',
            #'nu_ano_evasao': 'Int64',
            #'ds_periodo_evasao': 'string',
        }

        query = """
            with periodos_cursados_base as (
                select 
                    nu_matricula 
                    , nu_ano_ingresso 
                    , ds_periodo_ingresso 
                    , nu_ano_evasao 
                    , ds_periodo_evasao 
                    , cd_curso 
                    , (coalesce(nu_ano_evasao, 0) - nu_ano_ingresso) * 2 nu_periodos_cursados
                from stg.v_ds_stg_relatorio f 
                inner join dw.d_pessoa dp 
                    on f.nu_cpf = dp.nu_cpf 
                )
                , periodos_cursados as (
                    select 
                        nu_matricula 
                        , nu_ano_ingresso 
                        , ds_periodo_ingresso 
                        , nu_ano_evasao 
                        , ds_periodo_evasao 
                        , cd_curso 
                        , case
                            when nu_periodos_cursados >= 0 
                                and ds_periodo_ingresso = ds_periodo_evasao
                            then nu_periodos_cursados + 1  
                            when nu_ano_evasao >= nu_ano_ingresso 
                                and ds_periodo_ingresso <> ds_periodo_evasao 
                                and ds_periodo_ingresso = '2º SEMESTRE'
                            then nu_periodos_cursados 
                            when nu_ano_evasao >= nu_ano_ingresso 
                                and ds_periodo_ingresso <> ds_periodo_evasao 
                                and ds_periodo_ingresso = '1º SEMESTRE'
                            then nu_periodos_cursados + 2
                        else -2
                    end nu_periodos_cursados_total
                from periodos_cursados_base 
            )
            select 
                coalesce(dp.sk_pessoa, -1) sk_pessoa
                , coalesce(dtc.sk_tipo_cota, -1) sk_tipo_cota 
                , coalesce(dfi.sk_forma_ingresso, -1) sk_forma_ingresso
                , coalesce(dc.sk_curso, -1) sk_curso
                , coalesce(dfe.sk_forma_evasao, -1) sk_forma_evasao
                , coalesce(d_evasao.sk_calendario, -2) sk_ano_evasao
                , coalesce(d_ingresso.sk_calendario, -2) sk_ano_ingresso
                , coalesce(dpc.sk_periodos_cursados, -2) sk_periodos_cursados 
                , f.nu_matricula
                , f.nu_cra 
                , f.nu_crn 
                , f.fl_cotista 
                , f.nu_periodos
                , f.nu_maximo_periodos
                , f.nu_ch_total_curso
                , f.nu_versao
            from stg.v_ds_stg_relatorio f
            inner join dw.d_pessoa dp 
                on f.nu_cpf = dp.nu_cpf 
            inner join dw.d_tipo_cota dtc 
                on f.ds_tipo_cota = dtc.ds_tipo_cota 
            inner join dw.d_forma_ingresso dfi 
                on f.ds_forma_ingresso = dfi.ds_forma_ingresso 
            inner join dw.d_curso dc 
                on f.cd_curso = dc.cd_curso 
            inner join dw.d_forma_evasao dfe 
                on f.ds_forma_evasao = dfe.ds_forma_evasao 
            left join dw.d_calendario d_ingresso
                on f.nu_ano_ingresso = d_ingresso.nu_ano_referencia 
                and f.ds_periodo_ingresso = d_ingresso.ds_periodo_referencia 
            left join dw.d_calendario d_evasao
                on f.nu_ano_evasao = d_evasao.nu_ano_referencia 
                and f.ds_periodo_evasao = d_evasao.ds_periodo_referencia
            left join periodos_cursados pc
                on f.nu_matricula = pc.nu_matricula
                and f.cd_curso = pc.cd_curso 
                and f.nu_ano_ingresso = pc.nu_ano_ingresso
                and f.ds_periodo_ingresso = pc.ds_periodo_ingresso
                and f.nu_ano_evasao = pc.nu_ano_evasao  
                and f.ds_periodo_evasao = pc.ds_periodo_evasao
            left join dw.d_periodos_cursados dpc 
                on pc.nu_periodos_cursados_total = dpc.nu_periodos_cursados 
            except 
            select
                sk_pessoa
                , sk_tipo_cota 
                , sk_forma_ingresso 
                , sk_curso 
                , sk_forma_evasao 
                , sk_ano_evasao
                , sk_ano_ingresso
                , sk_periodos_cursados
                , nu_matricula 
                , nu_cra 
                , nu_crn 
                , fl_cotista  
                , nu_periodos
                , nu_maximo_periodos
                , nu_ch_total_curso
                , nu_versao
            from dw.f_situacao_matricula
        """

        query_delete = """
            delete from dw.f_situacao_matricula f
            where f.nu_matricula in (
                select nu_matricula  from dw.f_situacao_matricula f 
                where f.sk_ano_evasao = -2
            )
        """

        return _FatoTemplate(
            conn_input=conn_input,
            conn_output=conn_output,
            table_name=self._table_name,
            schema_output=self._schema,
            query=query, 
            dtypes=self._dtypes,
            chunksize=self._chunksize,
            query_delete=query_delete
        )


# factory = FSituacaoMatriculaFactory('conn_dbdw', 'conn_dbdw')

# globals()[factory.dag_id] = factory.get_dag()
