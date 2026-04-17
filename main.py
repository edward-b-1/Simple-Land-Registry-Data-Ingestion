
import os
import tempfile
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

import psycopg

from dataclasses import dataclass

from lib_land_registry_data.lib_env import EnvironmentVariables

from lib_land_registry_data.lib_dataframe import df_pp_complete_columns

from lib_land_registry_data.logging import set_logger_process_name
from lib_land_registry_data.logging import get_logger
from lib_land_registry_data.logging import create_stdout_log_handler
from lib_land_registry_data.logging import create_file_log_handler

from lib_land_registry_data.lib_db import PPCompleteMetadata


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
    pandas_datetime_convert_start_timestamp: datetime|None
    pandas_datetime_convert_complete_timestamp: datetime|None
    pandas_datetime_convert_duration: timedelta|None
    pandas_write_start_timestamp: datetime|None
    pandas_write_complete_timestamp: datetime|None
    pandas_write_duration: timedelta|None
    database_upload_start_timestamp: datetime|None
    database_upload_complete_timestamp: datetime|None
    database_upload_duration: timedelta|None


def download_pp_complete_and_upload_to_database(
    process_metadata: ProcessMetadata,
    postgres_connection_string: str,
) -> tuple[date, int]:

    temp_file_path = download_data_to_disk_retry_wrapper(
        process_metadata=process_metadata,
    )
    download_size_MB = int(os.path.getsize(temp_file_path) / (1024 * 1024))

    try:
        auto_date = pandas_read_and_database_upload(
            process_metadata=process_metadata,
            postgres_connection_string=postgres_connection_string,
            temp_file_path=temp_file_path,
        )
    finally:
        os.unlink(temp_file_path)

    return (auto_date, download_size_MB)


def download_data_to_disk_retry_wrapper(
    process_metadata: ProcessMetadata,
    max_retries=3,
) -> str:

    url = 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.txt'

    fail_count = 0
    while True:
        logger.info(f'try to run download')

        try:
            logger.info(f'download data to disk')
            temp_file_path = download_data_to_disk(
                process_metadata=process_metadata,
                url=url,
            )
            logger.info(f'download data to disk complete')
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

    return temp_file_path


def download_data_to_disk(
    process_metadata: ProcessMetadata,
    url: str,
) -> str:

    download_start_timestamp = datetime.now(timezone.utc)

    logger.info(f'downloading from {url}')
    logger.info(f'download starting: {download_start_timestamp}')

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.txt')
    tmp.close()

    with requests.get(url, allow_redirects=True, stream=True) as response:
        logger.info(f'status_code={response.status_code}')

        if response.status_code != 200:
            download_complete_timestamp = datetime.now(timezone.utc)
            download_duration = download_complete_timestamp - download_start_timestamp
            logger.error(f'download failure: {download_duration}')
            os.unlink(tmp.name)
            raise RuntimeError(f'request failure {response.status_code}')

        with open(tmp.name, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8*1024*1024):
                f.write(chunk)

    download_complete_timestamp = datetime.now(timezone.utc)
    download_duration = download_complete_timestamp - download_start_timestamp
    process_metadata.download_duration = download_duration

    download_size_MB = os.path.getsize(tmp.name) / (1024 * 1024)
    logger.info(f'download complete: {download_complete_timestamp}')
    logger.info(f'download duration: {download_duration}')
    logger.info(f'download size: {download_size_MB:.1f} MB')

    return tmp.name


def pandas_read_and_database_upload(
    process_metadata: ProcessMetadata,
    postgres_connection_string: str,
    temp_file_path: str,
) -> date:

    chunk_size = 50000

    logger.info(f'load pandas DataFrame in chunks (chunk size {chunk_size}) and upload to database')

    auto_date = None

    pandas_read_duration = timedelta(0)
    pandas_datetime_convert_duration = timedelta(0)
    pandas_write_duration = timedelta(0)
    database_upload_duration = timedelta(0)

    sub_process_start_timestamp = datetime.now(timezone.utc)
    logger.info(f'start timestamp: {sub_process_start_timestamp}')

    with psycopg.connect(postgres_connection_string) as connection:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE land_registry_simple.pp_complete_data")

            columns = '(transaction_unique_id, price, transaction_date, postcode, property_type, new_tag, lease, primary_address_object_name, secondary_address_object_name, street, locality, town_city, district, county, ppd_cat, record_op)'

            with cursor.copy(f"COPY land_registry_simple.pp_complete_data {columns} FROM STDIN WITH (FORMAT csv, NULL '\\N')") as copy:

                pandas_read_start_timestamp = datetime.now(timezone.utc)

                for chunk in pandas.read_csv(
                    temp_file_path,
                    header=None,
                    dtype=str,
                    keep_default_na=False,
                    chunksize=chunk_size,
                ):
                    pandas_read_complete_timestamp = datetime.now(timezone.utc)
                    pandas_read_duration += pandas_read_complete_timestamp - pandas_read_start_timestamp
                    process_metadata.pandas_read_duration = pandas_read_duration

                    pandas_datetime_convert_start_timestamp = datetime.now(timezone.utc)

                    chunk.columns = df_pp_complete_columns
                    chunk['transaction_date'] = pandas.to_datetime(
                        arg=chunk['transaction_date'],
                        utc=True,
                        format='%Y-%m-%d %H:%M',
                    )

                    pandas_datetime_convert_complete_timestamp = datetime.now(timezone.utc)
                    pandas_datetime_convert_duration += pandas_datetime_convert_complete_timestamp - pandas_datetime_convert_start_timestamp
                    process_metadata.pandas_datetime_convert_duration = pandas_datetime_convert_duration

                    auto_date_chunk = chunk['transaction_date'].max()
                    if auto_date is None or auto_date_chunk > auto_date:
                        auto_date = auto_date_chunk

                    pandas_write_start_timestamp = datetime.now(timezone.utc)

                    buffer = io.StringIO()
                    chunk.to_csv(buffer, index=False, header=False)

                    pandas_write_complete_timestamp = datetime.now(timezone.utc)
                    pandas_write_duration += pandas_write_complete_timestamp - pandas_write_start_timestamp
                    process_metadata.pandas_write_duration = pandas_write_duration

                    database_upload_start_timestamp = datetime.now(timezone.utc)

                    copy.write(buffer.getvalue())

                    database_upload_complete_timestamp = datetime.now(timezone.utc)
                    database_upload_duration += database_upload_complete_timestamp - database_upload_start_timestamp
                    process_metadata.database_upload_duration = database_upload_duration

                    pandas_read_start_timestamp = datetime.now(timezone.utc)

        connection.commit()

    sub_process_end_timestamp = datetime.now(timezone.utc)

    logger.info(f'done reading pandas DataFrame and database upload')
    logger.info(f'complete timestamp: {sub_process_end_timestamp}')
    logger.info(f'pandas read duration: {pandas_read_duration}')
    logger.info(f'datetime convert duration: {pandas_datetime_convert_duration}')
    logger.info(f'pandas write duration: {pandas_write_duration}')
    logger.info(f'database upload duration: {database_upload_duration}')

    auto_date = (
        date(
            year=auto_date.year,
            month=auto_date.month,
            day=auto_date.day,
        )
    )
    logger.info(f'auto_date={auto_date}')

    return auto_date


def update_pp_complete_metadata(
    process_metadata: ProcessMetadata,
    postgres_engine: Engine,
    auto_date: date,
    download_size_MB: int,
) -> None:

    with Session(postgres_engine) as session:
        row = PPCompleteMetadata(
            auto_date=auto_date,
            download_size_MB=int(download_size_MB),
            process_start_timestamp=process_metadata.process_start_timestamp,
            process_complete_timestamp=process_metadata.process_complete_timestamp,
            process_duration=process_metadata.process_duration,
            download_duration=process_metadata.download_duration,
            pandas_read_duration=process_metadata.pandas_read_duration,
            pandas_datetime_convert_duration=process_metadata.pandas_datetime_convert_duration,
            pandas_write_duration=process_metadata.pandas_write_duration,
            database_upload_duration=process_metadata.database_upload_duration,
        )
        session.add(row)
        session.commit()


def main():

    process_start_timestamp = datetime.now(timezone.utc)

    logger.info(f'{PROCESS_NAME} start')

    environment_variables = EnvironmentVariables()

    logger.info(f'postgres_host: {environment_variables.get_postgres_host()}')
    postgres_connection_string = environment_variables.get_psycopg3_postgres_connection_string_as_key_value_pairs()
    logger.info(f'connecting to postgres using psycopg3')
    logger.info(f'{postgres_connection_string}')

    logger.info(f'process start: {datetime.now(timezone.utc)}')

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
        pandas_datetime_convert_start_timestamp=None,
        pandas_datetime_convert_complete_timestamp=None,
        pandas_datetime_convert_duration=None,
        pandas_write_start_timestamp=None,
        pandas_write_complete_timestamp=None,
        pandas_write_duration=None,
        database_upload_start_timestamp=None,
        database_upload_complete_timestamp=None,
        database_upload_duration=None
    )

    (
        auto_date,
        download_size_MB,
    ) = download_pp_complete_and_upload_to_database(
        process_metadata=process_metadata,
        postgres_connection_string=postgres_connection_string,
    )

    logger.info(f'create database engine: postgres_host={environment_variables.get_postgres_host()}')
    postgres_connection_string = environment_variables.get_postgres_psycopg3_connection_string()
    logger.info(f'connecting to postgres using sqlalchemy')
    logger.info(f'{postgres_connection_string}')
    postgres_engine = create_engine(
        postgres_connection_string,
        #fast_executemany=True,
        #executmany_mode='batch',
    )

    logger.info(f'postgres engine:')
    logger.info(f'dialect: {postgres_engine.dialect}')
    logger.info(f'dialect api: {postgres_engine.dialect.dbapi}')
    logger.info(f'dialect api __name__: {postgres_engine.dialect.dbapi.__name__}')

    process_complete_timestamp = datetime.now(timezone.utc)
    process_duration = process_complete_timestamp - process_start_timestamp

    process_metadata.process_start_timestamp = process_start_timestamp
    process_metadata.process_complete_timestamp = process_complete_timestamp
    process_metadata.process_duration = process_duration

    update_pp_complete_metadata(
        process_metadata=process_metadata,
        postgres_engine=postgres_engine,
        auto_date=auto_date,
        download_size_MB=download_size_MB,
    )

    logger.info(f'process finished: {datetime.now(timezone.utc)}')
    logger.info(f'duration: {process_duration}')


if __name__ == '__main__':
    main()