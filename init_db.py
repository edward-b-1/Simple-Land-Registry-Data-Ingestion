
import argparse

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

SCHEMA_NAME = 'land_registry'


def resolve_table_keys(recreate: list[str]) -> list[str]:
    all_table_keys = list(LandRegistryBase.metadata.tables.keys())

    if len(recreate) == 0:
        return all_table_keys

    table_keys = []
    for table_name in recreate:
        if '.' in table_name:
            table_key = table_name
        else:
            table_key = f'{SCHEMA_NAME}.{table_name}'

        if table_key not in all_table_keys:
            raise ValueError(f'unknown table {table_name!r}, valid tables: {all_table_keys}')

        table_keys.append(table_key)

    return table_keys


def main(recreate: list[str]|None):

    environment_variables = EnvironmentVariables()

    url = postgres_connection_string = environment_variables.get_postgres_psycopg3_connection_string()
    engine = create_engine(url)

    with engine.connect() as connection:
        connection.execute(text(f'create schema if not exists {SCHEMA_NAME}'))
        connection.commit()

    logger.info(f'list of tables')
    for table in LandRegistryBase.metadata.tables.keys():
        logger.info(f'{table}')

    if recreate is not None:
        table_keys_to_drop = resolve_table_keys(recreate)
        for table_key in table_keys_to_drop:
            logger.info(f'drop table {table_key}')
            LandRegistryBase.metadata.tables[table_key].drop(engine, checkfirst=True)

    LandRegistryBase.metadata.create_all(engine, checkfirst=True)
    logger.info(f'tables created (if they did not already exist)')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='create the land registry schema and tables (idempotent)',
    )
    parser.add_argument(
        '--recreate',
        nargs='*',
        metavar='TABLE',
        default=None,
        help=(
            'drop and recreate tables (destroys their data); '
            'with no table names, all tables are recreated, '
            'otherwise only the named tables (e.g. pp_complete_data)'
        ),
    )
    args = parser.parse_args()

    main(recreate=args.recreate)
