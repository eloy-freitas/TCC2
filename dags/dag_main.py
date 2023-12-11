from airflow.models.dag import DAG
from datetime import datetime
from airflow.utils.task_group import TaskGroup

from dags.stg import *
from dags.dw import *


DAG_NAME='dag_etl_evasao'

class DAGMain():

    def __init__(self) -> None:
        self._list_stg = [
            STGRelatorioFactory(conn_output='conn_dbdw'),
        ]

        self._list_dim = [
            DFormaIngressoFactory(conn_input='conn_dbdw', conn_output='conn_dbdw'),
            DFormaEvasaoFactory(conn_input='conn_dbdw', conn_output='conn_dbdw'),
            DTipoCotaFactory(conn_input='conn_dbdw', conn_output='conn_dbdw'),
            DPessoaFactory(conn_input='conn_dbdw', conn_output='conn_dbdw'),
            DCursoFactory(conn_input='conn_dbdw', conn_output='conn_dbdw'),
            DCalendarioFactory(conn_input='conn_dbdw', conn_output='conn_dbdw'),
            DPeriodosCursadosFactory(conn_output='conn_dbdw')
        ]

        self._list_fato = [
            FSituacaoMatriculaFactory(conn_input='conn_dbdw', conn_output='conn_dbdw'),
        ]

    def get_dag(self):
        with DAG(
            DAG_NAME,
            description='DAG do fluxo de ETL do Datasouce de evasão na UFES campus Alegre',
            start_date=datetime(2023,11,15),
            catchup=False,
            max_active_runs=1
        ) as dag:

            with TaskGroup('stages') as stg:
                [t.get_task()  for t in self._list_stg]

            with TaskGroup('dimensões') as dw:
               [t.get_task()  for t in self._list_dim]
            
            with TaskGroup('fato') as fato:
               [t.get_task()  for t in self._list_fato]

            stg >> dw >> fato
            
        return dag
    

globals()[DAG_NAME] = DAGMain().get_dag()