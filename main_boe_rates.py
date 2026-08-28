
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

FREQUENCY_DAILY = 'daily'
FREQUENCY_MONTHLY = 'monthly'

CATEGORY_BANK_RATE = 'bank_rate'
CATEGORY_QUOTED_MORTGAGE_RATE = 'quoted_mortgage_rate'
CATEGORY_EFFECTIVE_MORTGAGE_RATE = 'effective_mortgage_rate'


@dataclass(frozen=True)
class SeriesConfig():
    code: str
    frequency: str
    category: str


# the official description of each series is taken from the IADB response,
# the comments here are only for the reader
SERIES_CONFIG: list[SeriesConfig] = [
    SeriesConfig('IUDBEDR',  FREQUENCY_DAILY,   CATEGORY_BANK_RATE),               # official Bank Rate
    SeriesConfig('IUMABEDR', FREQUENCY_MONTHLY, CATEGORY_BANK_RATE),               # monthly average of Bank Rate
    SeriesConfig('IUMBV34',  FREQUENCY_MONTHLY, CATEGORY_QUOTED_MORTGAGE_RATE),    # 2 year fixed, 75% LTV
    SeriesConfig('IUMB482',  FREQUENCY_MONTHLY, CATEGORY_QUOTED_MORTGAGE_RATE),    # 2 year fixed, 90% LTV
    SeriesConfig('IUMBV37',  FREQUENCY_MONTHLY, CATEGORY_QUOTED_MORTGAGE_RATE),    # 3 year fixed, 75% LTV
    SeriesConfig('IUMBV42',  FREQUENCY_MONTHLY, CATEGORY_QUOTED_MORTGAGE_RATE),    # 5 year fixed, 75% LTV
    SeriesConfig('IUMBV48',  FREQUENCY_MONTHLY, CATEGORY_QUOTED_MORTGAGE_RATE),    # 2 year variable, 75% LTV
    SeriesConfig('IUMB479',  FREQUENCY_MONTHLY, CATEGORY_QUOTED_MORTGAGE_RATE),    # 2 year variable, 90% LTV
    SeriesConfig('IUMBV24',  FREQUENCY_MONTHLY, CATEGORY_QUOTED_MORTGAGE_RATE),    # lifetime tracker
    SeriesConfig('IUMTLMV',  FREQUENCY_MONTHLY, CATEGORY_QUOTED_MORTGAGE_RATE),    # revert-to-rate (standard variable rate)
    SeriesConfig('CFMHSDE',  FREQUENCY_MONTHLY, CATEGORY_EFFECTIVE_MORTGAGE_RATE), # outstanding stock of loans secured on dwellings
    SeriesConfig('CFMBJ39',  FREQUENCY_MONTHLY, CATEGORY_EFFECTIVE_MORTGAGE_RATE), # new advances, floating rate
    SeriesConfig('CFMBJ42',  FREQUENCY_MONTHLY, CATEGORY_EFFECTIVE_MORTGAGE_RATE), # new advances, initial fixation <= 1 year
    SeriesConfig('CFMBJ43',  FREQUENCY_MONTHLY, CATEGORY_EFFECTIVE_MORTGAGE_RATE), # new advances, initial fixation > 1 year <= 5 years
    SeriesConfig('CFMBJ44',  FREQUENCY_MONTHLY, CATEGORY_EFFECTIVE_MORTGAGE_RATE), # new advances, initial fixation > 5 years <= 10 years
    SeriesConfig('CFMBJ45',  FREQUENCY_MONTHLY, CATEGORY_EFFECTIVE_MORTGAGE_RATE), # new advances, initial fixation > 10 years
]

# values the IADB uses for a missing observation
MISSING_VALUES = {'', '..'}


@dataclass
class ProcessMetadata():
    process_start_timestamp: datetime|None = None
    process_complete_timestamp: datetime|None = None
    process_duration: timedelta|None = None
    download_duration: timedelta|None = None
    download_size_bytes: int|None = None
    parse_duration: timedelta|None = None
    database_upload_duration: timedelta|None = None


def build_request_params(
    series_codes: list[str],
    date_from: str,
) -> dict[str, str]:

    # CSVF=TT: a SERIES,DESCRIPTION header block followed by the data block
    # (CSVF=TN would return the data block only)
    return {
        'csv.x': 'yes',
        'Datefrom': date_from,
        'Dateto': 'now',
        'SeriesCodes': ','.join(series_codes),
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
    logger.info(f'series codes: {params["SeriesCodes"]}')
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
            f'request failure {response.status_code} location={location!r} '
            f'(302 usually means an unknown series code, 403 a rejected User-Agent)'
        )

    csv_text = response.text

    if not csv_text.startswith('SERIES,DESCRIPTION'):
        raise RuntimeError(f'unexpected response body (first 200 chars): {csv_text[:200]!r}')

    download_complete_timestamp = datetime.now(timezone.utc)
    download_duration = download_complete_timestamp - download_start_timestamp
    process_metadata.download_duration = download_duration
    process_metadata.download_size_bytes = len(response.content)

    logger.info(f'download complete: {download_complete_timestamp}')
    logger.info(f'download duration: {download_duration}')
    logger.info(f'download size: {len(response.content)} bytes')

    return csv_text


def parse_iadb_csv(
    csv_text: str,
) -> tuple[pandas.DataFrame, pandas.DataFrame]:

    lines = csv_text.splitlines()

    if len(lines) == 0 or lines[0] != 'SERIES,DESCRIPTION':
        raise RuntimeError(f'unexpected first line: {lines[:1]!r}')

    data_start = None
    for index, line in enumerate(lines):
        if line.startswith('DATE,'):
            data_start = index
            break

    if data_start is None:
        raise RuntimeError(f'no DATE header line found in csv')

    # header block: one unquoted `CODE,description` line per series, the
    # description is free text so only split on the first comma
    series_rows = []
    for line in lines[1:data_start]:
        if line.strip() == '':
            continue
        series_code, _, description = line.partition(',')
        series_rows.append(
            {
                'series_code': series_code.strip(),
                'description': description.strip(),
            }
        )

    series_df = pandas.DataFrame(series_rows, columns=['series_code', 'description'])

    # data block: DATE column then one column per series code
    data_df = pandas.read_csv(
        io.StringIO('\n'.join(lines[data_start:])),
        dtype=str,
        keep_default_na=False,
    )

    observations_df = data_df.melt(
        id_vars=['DATE'],
        var_name='series_code',
        value_name='value',
    )

    observations_df['value'] = observations_df['value'].str.strip()
    observations_df = observations_df[~observations_df['value'].isin(MISSING_VALUES)].copy()

    # validation only, the value is kept as text for COPY
    pandas.to_numeric(observations_df['value'], errors='raise')

    observations_df['observation_date'] = pandas.to_datetime(
        observations_df['DATE'],
        format='%d %b %Y',
    ).dt.date

    observations_df = observations_df[['series_code', 'observation_date', 'value']]
    observations_df = observations_df.sort_values(['series_code', 'observation_date']).reset_index(drop=True)

    return (series_df, observations_df)


def validate_parsed_data(
    series_df: pandas.DataFrame,
    observations_df: pandas.DataFrame,
    series_config: list[SeriesConfig],
) -> None:

    configured_codes = {config.code for config in series_config}
    header_codes = set(series_df['series_code'])
    data_codes = set(observations_df['series_code'])

    if header_codes != configured_codes:
        raise RuntimeError(
            f'series codes in csv header block do not match configuration: '
            f'missing={sorted(configured_codes - header_codes)} unexpected={sorted(header_codes - configured_codes)}'
        )

    if data_codes != configured_codes:
        raise RuntimeError(
            f'series with observations do not match configuration: '
            f'missing={sorted(configured_codes - data_codes)} unexpected={sorted(data_codes - configured_codes)}'
        )

    duplicates = observations_df.duplicated(subset=['series_code', 'observation_date'])
    if duplicates.any():
        raise RuntimeError(f'duplicate (series_code, observation_date) rows: {int(duplicates.sum())}')

    # monthly series are expected on month-end dates; a mismatch is not fatal
    # but worth knowing about
    monthly_codes = {config.code for config in series_config if config.frequency == FREQUENCY_MONTHLY}
    monthly_df = observations_df[observations_df['series_code'].isin(monthly_codes)]
    is_month_end = pandas.to_datetime(monthly_df['observation_date']).dt.is_month_end
    if not is_month_end.all():
        not_month_end = monthly_df[~is_month_end]
        logger.warning(f'{len(not_month_end)} monthly observations are not on a month-end date')
        for series_code, count in not_month_end['series_code'].value_counts().items():
            logger.warning(f'{series_code}: {count} observations not on a month-end date')


def build_series_rows(
    series_df: pandas.DataFrame,
    observations_df: pandas.DataFrame,
    series_config: list[SeriesConfig],
    download_timestamp: datetime,
) -> pandas.DataFrame:

    config_df = pandas.DataFrame(
        [
            {
                'series_code': config.code,
                'frequency': config.frequency,
                'category': config.category,
            }
            for config in series_config
        ]
    )

    summary_df = (
        observations_df
        .groupby('series_code')
        .agg(
            first_observation_date=('observation_date', 'min'),
            last_observation_date=('observation_date', 'max'),
            observation_count=('observation_date', 'count'),
        )
        .reset_index()
    )

    series_rows_df = (
        config_df
        .merge(series_df, on='series_code', how='left')
        .merge(summary_df, on='series_code', how='left')
    )
    series_rows_df['download_timestamp'] = download_timestamp.isoformat()

    return series_rows_df[
        [
            'series_code',
            'description',
            'frequency',
            'category',
            'first_observation_date',
            'last_observation_date',
            'observation_count',
            'download_timestamp',
        ]
    ]


def log_series_summary(
    series_rows_df: pandas.DataFrame,
    observations_df: pandas.DataFrame,
) -> None:

    latest_df = (
        observations_df
        .sort_values('observation_date')
        .groupby('series_code')
        .last()
    )

    logger.info(f'series summary:')
    for row in series_rows_df.itertuples(index=False):
        latest = latest_df.loc[row.series_code]
        logger.info(
            f'{row.series_code}: {row.frequency} {row.category} '
            f'{row.observation_count} observations '
            f'{row.first_observation_date} -> {row.last_observation_date} '
            f'latest value {latest["value"]} '
            f'({row.description})'
        )
    logger.info(f'total observations: {len(observations_df)}')


def upload_to_database(
    process_metadata: ProcessMetadata,
    postgres_connection_string: str,
    series_rows_df: pandas.DataFrame,
    observations_df: pandas.DataFrame,
) -> None:

    database_upload_start_timestamp = datetime.now(timezone.utc)
    logger.info(f'database upload starting: {database_upload_start_timestamp}')

    # single transaction: truncate both tables (one statement because of the
    # foreign key) then reload, so readers never see an empty table and a
    # failed upload leaves the previous data in place
    with psycopg.connect(postgres_connection_string) as connection:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE bank_of_england.iadb_observation, bank_of_england.iadb_series")

            series_columns = '(series_code, description, frequency, category, first_observation_date, last_observation_date, observation_count, download_timestamp)'
            buffer = io.StringIO()
            series_rows_df.to_csv(buffer, index=False, header=False)
            with cursor.copy(f"COPY bank_of_england.iadb_series {series_columns} FROM STDIN WITH (FORMAT csv)") as copy:
                copy.write(buffer.getvalue())

            observation_columns = '(series_code, observation_date, value)'
            buffer = io.StringIO()
            observations_df.to_csv(buffer, index=False, header=False)
            with cursor.copy(f"COPY bank_of_england.iadb_observation {observation_columns} FROM STDIN WITH (FORMAT csv)") as copy:
                copy.write(buffer.getvalue())

        connection.commit()

    database_upload_complete_timestamp = datetime.now(timezone.utc)
    database_upload_duration = database_upload_complete_timestamp - database_upload_start_timestamp
    process_metadata.database_upload_duration = database_upload_duration

    logger.info(f'database upload complete: {database_upload_complete_timestamp}')
    logger.info(f'database upload duration: {database_upload_duration}')
    logger.info(f'uploaded {len(series_rows_df)} series and {len(observations_df)} observations')


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

    series_codes = [config.code for config in SERIES_CONFIG]
    params = build_request_params(
        series_codes=series_codes,
        date_from=BOE_DATE_FROM,
    )

    csv_text = download_csv_retry_wrapper(
        process_metadata=process_metadata,
        params=params,
    )
    download_timestamp = datetime.now(timezone.utc)

    parse_start_timestamp = datetime.now(timezone.utc)
    logger.info(f'parse csv')

    (
        series_df,
        observations_df,
    ) = parse_iadb_csv(
        csv_text=csv_text,
    )

    validate_parsed_data(
        series_df=series_df,
        observations_df=observations_df,
        series_config=SERIES_CONFIG,
    )

    series_rows_df = build_series_rows(
        series_df=series_df,
        observations_df=observations_df,
        series_config=SERIES_CONFIG,
        download_timestamp=download_timestamp,
    )

    parse_complete_timestamp = datetime.now(timezone.utc)
    process_metadata.parse_duration = parse_complete_timestamp - parse_start_timestamp
    logger.info(f'parse csv complete, duration: {process_metadata.parse_duration}')

    log_series_summary(
        series_rows_df=series_rows_df,
        observations_df=observations_df,
    )

    upload_to_database(
        process_metadata=process_metadata,
        postgres_connection_string=postgres_connection_string,
        series_rows_df=series_rows_df,
        observations_df=observations_df,
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
        series_count=len(series_rows_df),
        observation_count=len(observations_df),
    )

    logger.info(f'process finished: {datetime.now(timezone.utc)}')
    logger.info(f'duration: {process_duration}')


if __name__ == '__main__':
    main()
