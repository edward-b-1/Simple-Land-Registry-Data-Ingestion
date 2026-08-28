
import io
import time
import requests
import pandas
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

from lib_land_registry_data.logging import set_logger_process_name
from lib_land_registry_data.logging import get_logger
from lib_land_registry_data.logging import create_stdout_log_handler
from lib_land_registry_data.logging import create_file_log_handler

from lib_land_registry_data.lib_db import IADBMetadata

from lib_land_registry_data.lib_boe_series import SeriesConfig
from lib_land_registry_data.lib_boe_series import BOE_SERIES_CONFIG
from lib_land_registry_data.lib_boe_series import FREQUENCY_MONTHLY


PROCESS_NAME = 'simple_bank_of_england_rates_ingestion'

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


# Bank of England Interactive Statistical Database (IADB) CSV export
BOE_IADB_URL = 'https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp'

# first month of the Land Registry Price Paid data
BOE_DATE_FROM = '01/Jan/1995'

# the IADB answers HTTP 403 "Access Denied" to the default python-requests
# User-Agent, so send a browser-like one
HTTP_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) Simple-Land-Registry-Data-Ingestion',
}
HTTP_TIMEOUT_SECONDS = (10, 120)

# values the IADB uses for a missing observation; stored as NULL
MISSING_VALUES = {'', '..'}

SCHEMA_NAME = 'bank_of_england'


@dataclass
class ProcessMetadata():
    process_start_timestamp: datetime|None = None
    process_complete_timestamp: datetime|None = None
    process_duration: timedelta|None = None
    download_duration: timedelta = timedelta(0)
    download_size_bytes: int = 0
    parse_duration: timedelta = timedelta(0)
    database_upload_duration: timedelta|None = None


# one downloaded and parsed series, ready for upload
@dataclass
class SeriesData():
    config: SeriesConfig
    description: str
    observations_df: pandas.DataFrame # columns: observation_date, value (str or None)
    download_timestamp: datetime

    @property
    def observation_count(self) -> int:
        return int(self.observations_df['value'].notna().sum())

    @property
    def missing_count(self) -> int:
        return int(self.observations_df['value'].isna().sum())

    @property
    def first_observation_date(self) -> date|None:
        present = self.observations_df[self.observations_df['value'].notna()]
        return None if len(present) == 0 else present['observation_date'].min()

    @property
    def last_observation_date(self) -> date|None:
        present = self.observations_df[self.observations_df['value'].notna()]
        return None if len(present) == 0 else present['observation_date'].max()

    @property
    def latest_value(self) -> str|None:
        present = self.observations_df[self.observations_df['value'].notna()]
        return None if len(present) == 0 else present.iloc[-1]['value']


def build_request_params(
    series_code: str,
    date_from: str,
) -> dict[str, str]:

    # CSVF=TT: a SERIES,DESCRIPTION header block followed by the data block
    # (CSVF=TN would return the data block only)
    return {
        'csv.x': 'yes',
        'Datefrom': date_from,
        'Dateto': 'now',
        'SeriesCodes': series_code,
        'CSVF': 'TT',
        'UsingCodes': 'Y',
        'VPD': 'Y',
        'VFD': 'N',
    }


def download_csv_retry_wrapper(
    process_metadata: ProcessMetadata,
    params: dict[str, str],
    max_retries=3,
) -> str:

    fail_count = 0
    while True:
        logger.info(f'try to run download')

        try:
            logger.info(f'download csv')
            csv_text = download_csv(
                process_metadata=process_metadata,
                url=BOE_IADB_URL,
                params=params,
            )
            logger.info(f'download csv complete')
            break

        except Exception as error:
            logger.error(f'{error}')
            logger.exception(error)

            fail_count += 1
            logger.warning(f'fail_count={fail_count}')
            if fail_count > max_retries:
                logger.error(f'download failed after {fail_count} retries, give up')
                raise
            else:
                logger.warning(f'download failed, retry in 10 seconds, number of failures: {fail_count}')
                time_10_seconds = 10
                time.sleep(time_10_seconds)
                continue

    return csv_text


def download_csv(
    process_metadata: ProcessMetadata,
    url: str,
    params: dict[str, str],
) -> str:

    download_start_timestamp = datetime.now(timezone.utc)

    logger.info(f'downloading from {url}')
    logger.info(f'series code: {params["SeriesCodes"]}')
    logger.info(f'date from: {params["Datefrom"]}')
    logger.info(f'download starting: {download_start_timestamp}')

    # an unknown series code is answered with a 302 redirect to an error page,
    # so do not follow redirects and treat anything other than 200 as failure
    response = requests.get(
        url,
        params=params,
        headers=HTTP_HEADERS,
        allow_redirects=False,
        timeout=HTTP_TIMEOUT_SECONDS,
    )

    logger.info(f'status_code={response.status_code}')
    logger.info(f'content-type={response.headers.get("Content-Type")}')

    if response.status_code != 200:
        download_complete_timestamp = datetime.now(timezone.utc)
        download_duration = download_complete_timestamp - download_start_timestamp
        logger.error(f'download failure: {download_duration}')
        location = response.headers.get('Location')
        raise RuntimeError(
            f'request failure {response.status_code} for series {params["SeriesCodes"]} location={location!r} '
            f'(302 usually means an unknown series code, 403 a rejected User-Agent)'
        )

    csv_text = response.text

    if not csv_text.startswith('SERIES,DESCRIPTION'):
        raise RuntimeError(f'unexpected response body (first 200 chars): {csv_text[:200]!r}')

    download_complete_timestamp = datetime.now(timezone.utc)
    download_duration = download_complete_timestamp - download_start_timestamp
    process_metadata.download_duration += download_duration
    process_metadata.download_size_bytes += len(response.content)

    logger.info(f'download complete: {download_complete_timestamp}')
    logger.info(f'download duration: {download_duration}')
    logger.info(f'download size: {len(response.content)} bytes')

    return csv_text


def parse_iadb_csv(
    csv_text: str,
    config: SeriesConfig,
) -> tuple[str, pandas.DataFrame]:

    lines = csv_text.splitlines()

    if len(lines) == 0 or lines[0] != 'SERIES,DESCRIPTION':
        raise RuntimeError(f'{config.code}: unexpected first line: {lines[:1]!r}')

    data_start = None
    for index, line in enumerate(lines):
        if line.startswith('DATE,'):
            data_start = index
            break

    if data_start is None:
        raise RuntimeError(f'{config.code}: no DATE header line found in csv')

    # header block: one unquoted `CODE,description` line, the description is
    # free text so only split on the first comma
    header_lines = [line for line in lines[1:data_start] if line.strip() != '']
    if len(header_lines) != 1:
        raise RuntimeError(f'{config.code}: expected one series in header block, got {header_lines!r}')

    series_code, _, description = header_lines[0].partition(',')
    if series_code.strip() != config.code:
        raise RuntimeError(f'{config.code}: header block is for series {series_code!r}')

    # data block: DATE column then the single series column
    data_df = pandas.read_csv(
        io.StringIO('\n'.join(lines[data_start:])),
        dtype=str,
        keep_default_na=False,
    )

    if list(data_df.columns) != ['DATE', config.code]:
        raise RuntimeError(f'{config.code}: unexpected data columns {list(data_df.columns)!r}')

    observations_df = pandas.DataFrame(
        {
            'observation_date': pandas.to_datetime(data_df['DATE'], format='%d %b %Y').dt.date,
            'value': data_df[config.code].str.strip(),
        }
    )

    # missing observations are stored as NULL
    observations_df.loc[observations_df['value'].isin(MISSING_VALUES), 'value'] = None

    # validation only, the value is kept as text for COPY
    pandas.to_numeric(observations_df['value'], errors='raise')

    observations_df = observations_df.sort_values('observation_date').reset_index(drop=True)

    return (description.strip(), observations_df)


def validate_series_data(
    series_data: SeriesData,
) -> None:

    config = series_data.config
    observations_df = series_data.observations_df

    if series_data.observation_count == 0:
        raise RuntimeError(f'{config.code}: no observations with a value')

    duplicates = observations_df.duplicated(subset=['observation_date'])
    if duplicates.any():
        raise RuntimeError(f'{config.code}: duplicate observation_date rows: {int(duplicates.sum())}')

    # monthly series are expected on month-end dates; a mismatch is not fatal
    # but worth knowing about
    if config.frequency == FREQUENCY_MONTHLY:
        is_month_end = pandas.to_datetime(observations_df['observation_date']).dt.is_month_end
        if not is_month_end.all():
            logger.warning(f'{config.code}: {int((~is_month_end).sum())} observations are not on a month-end date')


def download_and_parse_series(
    process_metadata: ProcessMetadata,
    config: SeriesConfig,
) -> SeriesData:

    logger.info(f'series {config.code} -> {SCHEMA_NAME}.{config.table_name}')

    params = build_request_params(
        series_code=config.code,
        date_from=BOE_DATE_FROM,
    )

    csv_text = download_csv_retry_wrapper(
        process_metadata=process_metadata,
        params=params,
    )
    download_timestamp = datetime.now(timezone.utc)

    parse_start_timestamp = datetime.now(timezone.utc)

    (
        description,
        observations_df,
    ) = parse_iadb_csv(
        csv_text=csv_text,
        config=config,
    )

    series_data = SeriesData(
        config=config,
        description=description,
        observations_df=observations_df,
        download_timestamp=download_timestamp,
    )

    validate_series_data(series_data)

    parse_complete_timestamp = datetime.now(timezone.utc)
    process_metadata.parse_duration += parse_complete_timestamp - parse_start_timestamp

    logger.info(
        f'{config.code}: {config.frequency} {config.category} '
        f'{series_data.observation_count} observations, {series_data.missing_count} missing, '
        f'{series_data.first_observation_date} -> {series_data.last_observation_date}, '
        f'latest value {series_data.latest_value} '
        f'({description})'
    )

    return series_data


def build_series_catalogue(
    all_series_data: list[SeriesData],
) -> pandas.DataFrame:

    return pandas.DataFrame(
        [
            {
                'series_code': series_data.config.code,
                'table_name': series_data.config.table_name,
                'description': series_data.description,
                'frequency': series_data.config.frequency,
                'category': series_data.config.category,
                'first_observation_date': series_data.first_observation_date,
                'last_observation_date': series_data.last_observation_date,
                'observation_count': series_data.observation_count,
                'missing_count': series_data.missing_count,
                'download_timestamp': series_data.download_timestamp.isoformat(),
            }
            for series_data in all_series_data
        ]
    )


def upload_to_database(
    process_metadata: ProcessMetadata,
    postgres_connection_string: str,
    all_series_data: list[SeriesData],
) -> None:

    database_upload_start_timestamp = datetime.now(timezone.utc)
    logger.info(f'database upload starting: {database_upload_start_timestamp}')

    catalogue_df = build_series_catalogue(all_series_data)

    table_names = [f'{SCHEMA_NAME}.{series_data.config.table_name}' for series_data in all_series_data]
    table_names.append(f'{SCHEMA_NAME}.iadb_series')

    # single transaction: truncate every table then reload, so readers never
    # see a half loaded state and a failed upload leaves the previous data in
    # place. Empty csv fields are NULL under FORMAT csv
    with psycopg.connect(postgres_connection_string) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {', '.join(table_names)}")

            for series_data in all_series_data:
                table_name = f'{SCHEMA_NAME}.{series_data.config.table_name}'
                buffer = io.StringIO()
                series_data.observations_df.to_csv(buffer, index=False, header=False, na_rep='')
                with cursor.copy(f"COPY {table_name} (observation_date, value) FROM STDIN WITH (FORMAT csv)") as copy:
                    copy.write(buffer.getvalue())
                logger.info(f'{table_name}: {len(series_data.observations_df)} rows')

            catalogue_columns = '(series_code, table_name, description, frequency, category, first_observation_date, last_observation_date, observation_count, missing_count, download_timestamp)'
            buffer = io.StringIO()
            catalogue_df.to_csv(buffer, index=False, header=False, na_rep='')
            with cursor.copy(f"COPY {SCHEMA_NAME}.iadb_series {catalogue_columns} FROM STDIN WITH (FORMAT csv)") as copy:
                copy.write(buffer.getvalue())
            logger.info(f'{SCHEMA_NAME}.iadb_series: {len(catalogue_df)} rows')

        connection.commit()

    database_upload_complete_timestamp = datetime.now(timezone.utc)
    database_upload_duration = database_upload_complete_timestamp - database_upload_start_timestamp
    process_metadata.database_upload_duration = database_upload_duration

    logger.info(f'database upload complete: {database_upload_complete_timestamp}')
    logger.info(f'database upload duration: {database_upload_duration}')


def update_iadb_metadata(
    process_metadata: ProcessMetadata,
    postgres_engine: Engine,
    date_from: date,
    series_count: int,
    observation_count: int,
) -> None:

    with Session(postgres_engine) as session:
        row = IADBMetadata(
            date_from=date_from,
            series_count=series_count,
            observation_count=observation_count,
            download_size_bytes=process_metadata.download_size_bytes,
            process_start_timestamp=process_metadata.process_start_timestamp,
            process_complete_timestamp=process_metadata.process_complete_timestamp,
            process_duration=process_metadata.process_duration,
            download_duration=process_metadata.download_duration,
            parse_duration=process_metadata.parse_duration,
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
    postgres_connection_string_redacted = environment_variables.get_psycopg3_postgres_connection_string_as_key_value_pairs_redacted()
    logger.info(f'connecting to postgres using psycopg3')
    logger.info(f'{postgres_connection_string_redacted}')

    logger.info(f'process start: {process_start_timestamp}')

    process_metadata = ProcessMetadata()

    logger.info(f'{len(BOE_SERIES_CONFIG)} series to download')

    all_series_data = [
        download_and_parse_series(
            process_metadata=process_metadata,
            config=config,
        )
        for config in BOE_SERIES_CONFIG
    ]

    total_observation_count = sum(series_data.observation_count for series_data in all_series_data)
    total_missing_count = sum(series_data.missing_count for series_data in all_series_data)
    logger.info(f'total download size: {process_metadata.download_size_bytes} bytes')
    logger.info(f'total download duration: {process_metadata.download_duration}')
    logger.info(f'total parse duration: {process_metadata.parse_duration}')
    logger.info(f'total observations: {total_observation_count} ({total_missing_count} missing)')

    upload_to_database(
        process_metadata=process_metadata,
        postgres_connection_string=postgres_connection_string,
        all_series_data=all_series_data,
    )

    logger.info(f'create database engine: postgres_host={environment_variables.get_postgres_host()}')
    postgres_connection_string = environment_variables.get_postgres_psycopg3_connection_string()
    postgres_connection_string_redacted = environment_variables.get_postgres_psycopg3_connection_string_redacted()
    logger.info(f'connecting to postgres using sqlalchemy')
    logger.info(f'{postgres_connection_string_redacted}')
    postgres_engine = create_engine(postgres_connection_string)

    process_complete_timestamp = datetime.now(timezone.utc)
    process_duration = process_complete_timestamp - process_start_timestamp

    process_metadata.process_start_timestamp = process_start_timestamp
    process_metadata.process_complete_timestamp = process_complete_timestamp
    process_metadata.process_duration = process_duration

    update_iadb_metadata(
        process_metadata=process_metadata,
        postgres_engine=postgres_engine,
        date_from=datetime.strptime(BOE_DATE_FROM, '%d/%b/%Y').date(),
        series_count=len(all_series_data),
        observation_count=total_observation_count,
    )

    logger.info(f'process finished: {datetime.now(timezone.utc)}')
    logger.info(f'duration: {process_duration}')


if __name__ == '__main__':
    main()
