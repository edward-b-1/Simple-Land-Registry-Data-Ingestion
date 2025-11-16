
import tracemalloc
import time
import requests
import pandas
import io
from datetime import datetime
from datetime import date
from datetime import timezone
from datetime import timedelta

from sqlalchemy import Engine
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import psycopg2

from dataclasses import dataclass

from lib_land_registry_data.lib_env import EnvironmentVariables

from lib_land_registry_data.lib_dataframe import df_pp_complete_columns

from lib_land_registry_data.logging import set_logger_process_name
from lib_land_registry_data.logging import get_logger
from lib_land_registry_data.logging import create_stdout_log_handler
from lib_land_registry_data.logging import create_file_log_handler


PROCESS_NAME = 'simple_land_registry_data_ingestion'

set_logger_process_name(
    process_name=PROCESS_NAME,
)

logger = get_logger()
stdout_log_handler = create_stdout_log_handler()
file_log_handler = create_file_log_handler(
    logger_process_name=PROCESS_NAME,
    logger_file_datetime=datetime.now(timezone.utc).date(),
)
logger.addHandler(stdout_log_handler)
logger.addHandler(file_log_handler)


@dataclass
class ProcessMetadata():
    process_start_timestamp: datetime|None
    process_complete_timestamp: datetime|None
    process_duration: timedelta|None
    download_start_timestamp: datetime|None
    download_complete_timestamp: datetime|None
    download_duration: timedelta|None
    pandas_read_start_timestamp: datetime|None
    pandas_read_complete_timestamp: datetime|None
    pandas_read_duration: timedelta|None
    database_upload_start_timestamp: datetime|None
    database_upload_complete_timestamp: datetime|None
    database_upload_duration: timedelta|None


def download_pp_complete_and_upload_to_database(
    process_metadata: ProcessMetadata,
    postgres_engine: Engine,
) -> tuple[date, int]:

    pp_complete_data = download_data_to_memory_retry_wrapper(
        process_metadata=process_metadata,
    )

    download_size_bytes = len(pp_complete_data)

    (df, auto_date) = pandas_read(
        process_metadata=process_metadata,
        pp_complete_data=pp_complete_data,
    )

    environment_variables = EnvironmentVariables()

    #postgres_connection_string = environment_variables.get_postgres_connection_string()
    #postgres_connection_string = environment_variables.get_postgres_psycopg2_connection_string()
    #connection = psycopg2.connect(postgres_connection_string)
    connection = psycopg2.connect(
        host=environment_variables.postgres_host,
        user=environment_variables.postgres_user,
        password=environment_variables.postgres_password,
        dbname=environment_variables.postgres_database,
        port=5432,
    )
    cursor = connection.cursor()

    database_upload_start_timestamp = datetime.now(timezone.utc)
    process_metadata.database_upload_start_timestamp = database_upload_start_timestamp

    logger.info(f'load to sql database')
    logger.info(f'load start: {database_upload_start_timestamp}')

    buffer = io.StringIO()
    pandas_to_csv_start_timestamp = datetime.now(timezone.utc)
    df.to_csv(buffer, index=False, header=False) # TODO log this time separatly
    pandas_to_csv_end_timestamp = datetime.now(timezone.utc)
    pandas_to_csv_duration = pandas_to_csv_end_timestamp - pandas_to_csv_start_timestamp
    logger.info(f'pandas to_csv duration: {pandas_to_csv_duration}')
    logger.info(f'datetime now: {datetime.now(timezone.utc)}')
    buffer.seek(0)

    cursor.copy_expert(
        'COPY land_registry_simple.pp_complete_data FROM STDIN WITH (FORMAT csv)',
        buffer,
    )
    connection.commit()

    database_upload_complete_timestamp = datetime.now(timezone.utc)
    database_upload_duration = database_upload_complete_timestamp - database_upload_start_timestamp
    process_metadata.database_upload_complete_timestamp = database_upload_complete_timestamp
    process_metadata.database_upload_duration = database_upload_duration

    logger.info(f'load sql database complete')
    logger.info(f'database upload duration: {database_upload_duration}')

    #database_upload(
    #    process_metadata,
    #    postgres_engine=postgres_engine,
    #    df=df,
    #)

    return (auto_date, download_size_bytes)


def download_data_to_memory_retry_wrapper(
    process_metadata: ProcessMetadata,
    max_retries=3,
) -> bytes:

    url = 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.txt'

    fail_count = 0
    while True:
        logger.info(f'try to run download')

        try:
            logger.info(f'download data to memory')
            pp_complete_data = download_data_to_memory(
                process_metadata=process_metadata,
                url=url,
            )
            logger.info(f'download data to memory complete')
            break

        except Exception as error:
            logger.error(f'{error}')
            logger.exception(error)

            fail_count += 1
            logger.warning(f'fail_count={fail_count}')
            if fail_count > max_retries:
                logger.error(f'download failed after {fail_count} retries, give up')
                raise
                return False
            else:
                #logger.warning(f'download failed, retry in 1h, number of failures: {fail_count}')
                logger.warning(f'download failed, retry in 10 seconds, number of failures: {fail_count}')
                #time_1_hour = 3600
                #time.sleep(time_1_hour)
                time_10_seconds = 10
                time.sleep(time_10_seconds)
                continue

    return pp_complete_data


def download_data_to_memory(
    process_metadata: ProcessMetadata,
    url: str,
) -> bytes:

    download_start_timestamp = datetime.now(timezone.utc)
    process_metadata.download_start_timestamp = download_start_timestamp

    logger.info(f'downloading from {url}')
    logger.info(f'download starting: {download_start_timestamp}')

    response = requests.get(url, allow_redirects=True)

    download_complete_timestamp = datetime.now(timezone.utc)
    download_duration = download_complete_timestamp - download_start_timestamp
    process_metadata.download_complete_timestamp = download_complete_timestamp
    process_metadata.download_duration = download_duration

    logger.info(f'status_code={response.status_code}')
    pp_complete_data = response.content
    pp_complete_data_MB = len(pp_complete_data) / (1024 * 1024)

    if response.status_code == 200:
        logger.info(f'download complete: {download_complete_timestamp}')
        logger.info(f'download duration: {download_duration}')
        logger.info(f'download size: {pp_complete_data_MB:.1f} MB')
    else:
        logger.error(f'download failure: {download_duration}')
        raise RuntimeError(f'request failure {response.status_code}')

    return pp_complete_data


def pandas_read(
    process_metadata: ProcessMetadata,
    pp_complete_data: bytes,
) -> tuple[pandas.DataFrame, date]:

    pandas_read_start_timestamp = datetime.now(timezone.utc)
    process_metadata.pandas_read_start_timestamp = pandas_read_start_timestamp

    logger.info(f'load pandas DataFrame')
    logger.info(f'pandas read start: {pandas_read_start_timestamp}')

    df = pandas.read_csv(
        io.BytesIO(pp_complete_data),
        header=None,
    )
    pandas_read_complete_timestamp = datetime.now(timezone.utc)
    pandas_read_duration = pandas_read_complete_timestamp - pandas_read_start_timestamp
    process_metadata.pandas_read_complete_timestamp = pandas_read_complete_timestamp
    process_metadata.pandas_read_duration = pandas_read_duration

    logger.info(f'done reading data')
    logger.info(f'pandas read duration: {pandas_read_duration}')

    logger.info(f'get column names')
    df.columns = df_pp_complete_columns

    logger.info(f'convert column transaction_date to datetime')
    df['transaction_date'] = pandas.to_datetime(
        arg=df['transaction_date'],
        utc=True,
        format='%Y-%m-%d %H:%M',
    )

    logger.info(f'get auto_date')
    auto_date = df['transaction_date'].max()
    auto_date = (
        date(
            year=auto_date.year,
            month=auto_date.month,
            day=auto_date.day,
        )
    )
    logger.info(f'auto_date={auto_date}')

    return (df, auto_date)


def database_upload(
    process_metadata: ProcessMetadata,
    postgres_engine: Engine,
    df: pandas.DataFrame,
) -> None:

    database_upload_start_timestamp = datetime.now(timezone.utc)
    process_metadata.database_upload_start_timestamp = database_upload_start_timestamp

    logger.info(f'load to sql database')
    logger.info(f'load start: {database_upload_start_timestamp}')

    df.to_sql(
        name='pp_complete_data',
        schema='land_registry_simple',
        con=postgres_engine,
        if_exists='replace',
        index=False,
        #chunksize=100000,
        method='multi',
    )
    database_upload_complete_timestamp = datetime.now(timezone.utc)
    database_upload_duration = database_upload_complete_timestamp - database_upload_start_timestamp
    process_metadata.database_upload_complete_timestamp = database_upload_complete_timestamp
    process_metadata.database_upload_duration = database_upload_duration

    logger.info(f'load sql database complete')
    logger.info(f'database upload duration: {database_upload_duration}')


def update_pp_complete_metadata(
    process_metadata: ProcessMetadata,
    postgres_engine: Engine,
    auto_date: date,
    download_size_bytes: int,
) -> None:

    with Session(postgres_engine) as session:
        row = PPCompleteMetadata(
            auto_date=auto_date,
            download_size_bytes=download_size_bytes,
            process_start_timestamp=process_start_timestamp,
            process_complete_timestamp=process_end_timestamp,
            process_duration=process_duration,
            download_start_timestamp=download_start_timestamp,
            download_complete_timestamp=download_complete_timestamp,
            download_duration=download_duration,
            pandas_read_start_timestamp=pandas_read_start_timestamp,
            pandas_read_complete_timestamp=pandas_read_complete_timestamp,
            pandas_read_duration=pandas_read_duration,
            database_upload_start_timestamp=database_upload_start_timestamp,
            database_upload_complete_timestamp=database_upload_complete_timestamp,
            database_upload_duration=database_upload_duration,
        )
        session.add(row)
        session.commit()


def main():

    logger.info(f'{PROCESS_NAME} start')

    environment_variables = EnvironmentVariables()

    logger.info(f'create database engine: postgres_host={environment_variables.get_postgres_host()}')
    postgres_connection_string = environment_variables.get_postgres_connection_string()
    postgres_connection_string = environment_variables.get_postgres_psycopg2_connection_string()
    postgres_engine = create_engine(
        postgres_connection_string,
        #fast_executemany=True,
    )

    (current, peak) = tracemalloc.get_traced_memory()
    current_MB = current / (1024 * 1024)
    peak_MB = peak / (1024 * 1024)
    logger.info(f'process start')
    logger.info(f'tracemalloc memory:')
    logger.info(f'current: {current_MB:.1f} MB, peak: {peak_MB:.1f} MB')

    process_metadata = ProcessMetadata(
        process_start_timestamp=None,
        process_complete_timestamp=None,
        process_duration=None,
        download_start_timestamp=None,
        download_complete_timestamp=None,
        download_duration=None,
        pandas_read_start_timestamp=None,
        pandas_read_complete_timestamp=None,
        pandas_read_duration=None,
        database_upload_start_timestamp=None,
        database_upload_complete_timestamp=None,
        database_upload_duration=None
    )

    (
        auto_date,
        download_size_bytes,
    ) = download_pp_complete_and_upload_to_database(
        process_metadata=process_metadata,
        postgres_engine=postgres_engine,
    )

    update_pp_complete_metadata(
        process_metadata=process_metadata,
        postgres_engine=postgres_engine,
        auto_date=auto_date,
        download_size_bytes=download_size_bytes,
    )

    (current, peak) = tracemalloc.get_traced_memory()
    current_MB = current / (1024 * 1024)
    peak_MB = peak / (1024 * 1024)
    logger.info(f'process finished')
    logger.info(f'tracemalloc memory:')
    logger.info(f'current: {current_MB:.1f} MB, peak: {peak_MB:.1f} MB')


if __name__ == '__main__':
    tracemalloc.start()
    main()
    tracemalloc.stop()