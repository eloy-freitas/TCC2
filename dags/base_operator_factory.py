from abc import ABC, abstractmethod
from datetime import datetime


from airflow.models.dag import DAG as _DAG
from airflow.utils.task_group import TaskGroup as _TaskGroup
from airflow.models.baseoperator import BaseOperator as _BaseOperator
from airflow.operators.python import PythonOperator as _PythonOperator


class BaseOperatorFactory(ABC):
    def __init__(self, dag_id:str, description=""):
        self._dag_id = dag_id
        self._description=description

    @property
    def dag_id(self):
        return self._dag_id

    @property
    def description(self):
        return self._description
    
    @abstractmethod
    def prepare_callable(self)->object:...

    def get_task(self)->_BaseOperator:
        script = self.prepare_callable()

        task = _PythonOperator(
            task_id=self._table_name,
            python_callable=script.run
        )

        return task

    def get_dag(self)->_DAG:
        with _DAG(
            self.dag_id,
            description=self.description,
            catchup=False,
            start_date=datetime(2023,9,24)
        ) as dag:
            with _TaskGroup( 
                f'{self.dag_id}_group', 
                tooltip=f'Grupo de jobs da dag {self.dag_id}',
            ):
                self.get_task()

        return dag