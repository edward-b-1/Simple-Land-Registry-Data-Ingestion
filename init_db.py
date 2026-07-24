
from sqlalchemy import create_engine
from sqlalchemy import text

from lib_land_registry_data.lib_db import LandRegistryBase

from lib_land_registry_data.lib_env import EnvironmentVariables

from lib_land_registry_data.logging import set_logger_process_name
from lib_land_registry_data.logging import get_logger
from lib_land_registry_data.logging import create_stdout_log_handler


PROCESS_NAME = 'init_db'

set_logger_process_name(
    process_name=PROCESS_NAME,
)

logger = get_logger()
stdout_log_handler = create_stdout_log_handler()
logger.addHandler(stdout_log_handler)


def main():

    environment_variables = EnvironmentVariables()

    url = postgres_connection_string = environment_variables.get_postgres_psycopg3_connection_string()
    engine = create_engine(url)

    with engine.connect() as connection:
        connection.execute(text('create schema if not exists land_registry'))
        connection.commit()

    logger.info(f'list of tables')
    for table in LandRegistryBase.metadata.tables.keys():
        logger.info(f'{table}')

    LandRegistryBase.metadata.create_all(engine, checkfirst=True)
    logger.info(f'tables created (if they did not already exist)')


if __name__ == '__main__':
    main()
