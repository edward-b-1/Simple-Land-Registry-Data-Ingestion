
import os

from lib_land_registry_data.logging import get_logger

logger = get_logger()


class EnvironmentVariables():

    def __init__(self) -> None:
        logger.info(f'read environment variables')

        postgres_host = os.environ['POSTGRES_HOST']
        postgres_user = os.environ['POSTGRES_USER']
        postgres_password = os.environ['POSTGRES_PASSWORD']
        postgres_database = os.environ['POSTGRES_DATABASE']
        logger.info(f'env: POSTGRES_HOST={postgres_host}')
        logger.info(f'env: POSTGRES_USER={postgres_user}')
        logger.info(f'env: POSTGRES_DATABASE={postgres_database}')

        self.postgres_host = postgres_host
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password
        self.postgres_database = postgres_database
        self.postgres_port = 5432

    def get_postgres_connection_string(self) -> str:
        postgres_user = self.postgres_user
        postgres_password = self.postgres_password
        postgres_host = self.postgres_host
        postgres_database = self.postgres_database
        postgres_connection_string = f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}/{postgres_database}'
        return postgres_connection_string

    def get_postgres_psycopg2_connection_string(self) -> str:
        postgres_user = self.postgres_user
        postgres_password = self.postgres_password
        postgres_host = self.postgres_host
        postgres_database = self.postgres_database
        postgres_connection_string = f'postgresql+psycopg2://{postgres_user}:{postgres_password}@{postgres_host}/{postgres_database}'
        return postgres_connection_string

    def get_postgres_psycopg3_connection_string(self) -> str:
        postgres_user = self.postgres_user
        postgres_password = self.postgres_password
        postgres_host = self.postgres_host
        postgres_database = self.postgres_database
        postgres_connection_string = f'postgresql+psycopg://{postgres_user}:{postgres_password}@{postgres_host}/{postgres_database}'
        return postgres_connection_string

    def get_postgres_psycopg3_connection_string_with_port(self) -> str:
        postgres_user = self.postgres_user
        postgres_password = self.postgres_password
        postgres_host = self.postgres_host
        postgres_database = self.postgres_database
        postgres_port = self.postgres_port
        postgres_connection_string = f'postgresql+psycopg://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}'
        return postgres_connection_string

    def get_psycopg3_postgres_connection_string_as_key_value_pairs(self) -> str:
        postgres_user = self.postgres_user
        postgres_password = self.postgres_password
        postgres_host = self.postgres_host
        postgres_database = self.postgres_database
        postgres_port = self.postgres_port
        postgres_connection_string = f'user={postgres_user} password={postgres_password} host={postgres_host} dbname={postgres_database} port={postgres_port}'
        return postgres_connection_string

    def get_postgres_host(self) -> str:
        return self.postgres_host
