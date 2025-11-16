
from sqlalchemy import create_engine
from sqlalchemy import text
from lib_land_registry_data.lib_db import LandRegistryBase

from lib_land_registry_data.lib_env import EnvironmentVariables


def main(recreate: bool):

    environment_variables = EnvironmentVariables()

    url = postgres_connection_string = environment_variables.get_postgres_psycopg3_connection_string()
    engine = create_engine(url)

    with engine.connect() as connection:
        connection.execute(text('create schema if not exists land_registry_simple'))
        connection.commit()

    print(f'list of tables')
    for table in LandRegistryBase.metadata.tables.keys():
        print(table)

    if recreate:
        LandRegistryBase.metadata.tables['land_registry_simple.pp_complete_data'].drop(engine)

    LandRegistryBase.metadata.tables['land_registry_simple.pp_complete_data'].create(engine)


if __name__ == '__main__':
    main(recreate=False)
