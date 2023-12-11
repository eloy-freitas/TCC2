from airflow.hooks.base import BaseHook

from src.utils.connection.postgres_connection_factory import PostgresConnectionFactory

class PostgresHook(PostgresConnectionFactory):
        
    def __init__(self) -> None:
        super().__init__()

    def create_postgres_engine(self, conn_id:str):
        hook = BaseHook.get_connection(conn_id)
        engine_config = {
            'host':hook.host,
            'login':hook.login,
            'password':hook.get_password(),
            'port':hook.port,
            'database':hook.schema
        }

        print(engine_config)

        return self.create_engine(engine_config)