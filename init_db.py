
import argparse

from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy import text

from lib_land_registry_data.lib_db import all_bases

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


def all_tables() -> dict[str, Table]:
    # keys are schema qualified, e.g. 'land_registry.pp_complete_data'
    tables = {}
    for base in all_bases:
        tables.update(base.metadata.tables)
    return tables


def all_schemas() -> list[str]:
    return sorted({table.schema for table in all_tables().values()})


def resolve_table_keys(recreate: list[str]) -> list[str]:
    tables = all_tables()
    all_table_keys = list(tables.keys())

    if len(recreate) == 0:
        table_keys = all_table_keys
    else:
        table_keys = []
        for table_name in recreate:
            if '.' in table_name:
                if table_name not in all_table_keys:
                    raise ValueError(f'unknown table {table_name!r}, valid tables: {all_table_keys}')
                table_key = table_name
            else:
                matches = [key for key in all_table_keys if key.endswith(f'.{table_name}')]
                if len(matches) == 0:
                    raise ValueError(f'unknown table {table_name!r}, valid tables: {all_table_keys}')
                if len(matches) > 1:
                    raise ValueError(f'ambiguous table {table_name!r}, use a schema qualified name: {matches}')
                table_key = matches[0]

            table_keys.append(table_key)

    # drop in reverse dependency order so foreign keys do not block the drop
    # (e.g. bank_of_england.iadb_observation before bank_of_england.iadb_series)
    drop_order = []
    for base in all_bases:
        drop_order.extend(table.key for table in reversed(base.metadata.sorted_tables))

    return [key for key in drop_order if key in table_keys]


def main(recreate: list[str]|None):

    environment_variables = EnvironmentVariables()

    url = postgres_connection_string = environment_variables.get_postgres_psycopg3_connection_string()
    engine = create_engine(url)

    with engine.connect() as connection:
        for schema in all_schemas():
            logger.info(f'create schema if not exists {schema}')
            connection.execute(text(f'create schema if not exists {schema}'))
        connection.commit()

    tables = all_tables()

    logger.info(f'list of tables')
    for table_key in tables.keys():
        logger.info(f'{table_key}')

    if recreate is not None:
        table_keys_to_drop = resolve_table_keys(recreate)
        for table_key in table_keys_to_drop:
            logger.info(f'drop table {table_key}')
            tables[table_key].drop(engine, checkfirst=True)

    for base in all_bases:
        base.metadata.create_all(engine, checkfirst=True)
    logger.info(f'tables created (if they did not already exist)')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='create all dataset schemas and tables (idempotent)',
    )
    parser.add_argument(
        '--recreate',
        nargs='*',
        metavar='TABLE',
        default=None,
        help=(
            'drop and recreate tables (destroys their data); '
            'with no table names, all tables are recreated, '
            'otherwise only the named tables (e.g. pp_complete_data, '
            'or schema qualified: bank_of_england.iadb_observation); '
            'tables referenced by a foreign key must be dropped together '
            '(e.g. --recreate iadb_observation iadb_series)'
        ),
    )
    args = parser.parse_args()

    main(recreate=args.recreate)
