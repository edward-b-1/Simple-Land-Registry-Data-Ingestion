
import os
import io
import re
import csv
import time
import zipfile
import tempfile
import requests
import pandas
from datetime import datetime
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

from lib_land_registry_data.lib_db import NSPLPostcode
from lib_land_registry_data.lib_db import NSPLMetadata


PROCESS_NAME = 'simple_ons_nspl_ingestion'

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


# The National Statistics Postcode Lookup (NSPL) is published quarterly by
# the ONS on the Open Geography Portal, which is an ArcGIS Hub. Each release
# is a separate item; the latest is found through the ArcGIS search API and
# downloaded as a zip (~190 MB) containing Data/NSPL_<MON>_<YYYY>_UK.csv and
# a Documents/ folder of "names and codes" lookups.
ARCGIS_SEARCH_URL = 'https://www.arcgis.com/sharing/rest/search'
ARCGIS_ITEM_DATA_URL = 'https://www.arcgis.com/sharing/rest/content/items/{item_id}/data'
ARCGIS_SEARCH_QUERY = 'title:"National Statistics Postcode Lookup" AND owner:ONSGeography_data AND type:"CSV Collection"'
NSPL_TITLE_PATTERN = re.compile(r'^National Statistics Postcode Lookup \(([A-Za-z]+ [0-9]{4})\)')

# set to pin a release instead of resolving the latest one
NSPL_ARCGIS_ITEM_ID = os.environ.get('NSPL_ARCGIS_ITEM_ID')

HTTP_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) Simple-Land-Registry-Data-Ingestion',
}
HTTP_TIMEOUT_SECONDS = (10, 300)

SCHEMA_NAME = 'ons'
POSTCODE_TABLE = f'{SCHEMA_NAME}.nspl_postcode'
LOOKUP_TABLE = f'{SCHEMA_NAME}.nspl_code_lookup'

CHUNK_SIZE = 200_000

# NSPL columns that do not carry a geography vintage
FIXED_COLUMNS = {'pcd7', 'pcd8', 'pcds', 'dointr', 'doterm', 'usrtypind', 'east1m', 'north1m', 'gridind', 'lat', 'long'}

# the NSPL's "not available" coordinates
MISSING_LATITUDE = '99.999999'


@dataclass
class ProcessMetadata():
    process_start_timestamp: datetime|None = None
    process_complete_timestamp: datetime|None = None
    process_duration: timedelta|None = None
    download_duration: timedelta|None = None
    download_size_bytes: int|None = None
    database_upload_duration: timedelta|None = None


@dataclass
class NSPLRelease():
    item_id: str
    release_name: str
    source_url: str


# --- release discovery ---------------------------------------------------

def resolve_release() -> NSPLRelease:

    if NSPL_ARCGIS_ITEM_ID:
        logger.info(f'using pinned NSPL_ARCGIS_ITEM_ID={NSPL_ARCGIS_ITEM_ID}')
        return NSPLRelease(
            item_id=NSPL_ARCGIS_ITEM_ID,
            release_name='pinned',
            source_url=ARCGIS_ITEM_DATA_URL.format(item_id=NSPL_ARCGIS_ITEM_ID),
        )

    logger.info(f'searching the ArcGIS hub for the latest NSPL release')
    response = requests.get(
        ARCGIS_SEARCH_URL,
        params={
            'q': ARCGIS_SEARCH_QUERY,
            'f': 'json',
            'num': 25,
            'sortField': 'modified',
            'sortOrder': 'desc',
        },
        headers=HTTP_HEADERS,
        timeout=HTTP_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    results = response.json().get('results', [])

    for result in results:
        title = result.get('title', '')
        match = NSPL_TITLE_PATTERN.match(title)
        if match is None or 'User Guide' in title:
            continue
        release = NSPLRelease(
            item_id=result['id'],
            release_name=match.group(1),
            source_url=ARCGIS_ITEM_DATA_URL.format(item_id=result['id']),
        )
        logger.info(f'latest NSPL release: {release.release_name} (item {release.item_id}, title {title!r})')
        return release

    raise RuntimeError(f'no NSPL release found among {len(results)} search results')


# --- download ------------------------------------------------------------

def download_zip_retry_wrapper(
    process_metadata: ProcessMetadata,
    url: str,
    max_retries=3,
) -> str:

    fail_count = 0
    while True:
        logger.info(f'try to run download')

        try:
            logger.info(f'download zip to disk')
            temp_file_path = download_zip(
                process_metadata=process_metadata,
                url=url,
            )
            logger.info(f'download zip to disk complete')
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

    return temp_file_path


def download_zip(
    process_metadata: ProcessMetadata,
    url: str,
) -> str:

    download_start_timestamp = datetime.now(timezone.utc)

    logger.info(f'downloading from {url}')
    logger.info(f'download starting: {download_start_timestamp}')

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
    tmp.close()

    with requests.get(url, headers=HTTP_HEADERS, allow_redirects=True, stream=True, timeout=HTTP_TIMEOUT_SECONDS) as response:
        logger.info(f'status_code={response.status_code}')
        logger.info(f'content-type={response.headers.get("Content-Type")}')
        logger.info(f'content-disposition={response.headers.get("Content-Disposition")}')

        if response.status_code != 200:
            os.unlink(tmp.name)
            raise RuntimeError(f'request failure {response.status_code}')

        with open(tmp.name, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8*1024*1024):
                f.write(chunk)

    download_complete_timestamp = datetime.now(timezone.utc)
    download_duration = download_complete_timestamp - download_start_timestamp
    download_size_bytes = os.path.getsize(tmp.name)
    process_metadata.download_duration = download_duration
    process_metadata.download_size_bytes = download_size_bytes

    logger.info(f'download complete: {download_complete_timestamp}')
    logger.info(f'download duration: {download_duration}')
    logger.info(f'download size: {download_size_bytes / (1024 * 1024):.1f} MB')

    return tmp.name


# --- postcode csv ---------------------------------------------------------

def find_postcode_csv_member(archive: zipfile.ZipFile) -> str:

    candidates = [
        name for name in archive.namelist()
        if re.fullmatch(r'Data/NSPL_[A-Z]+_[0-9]{4}_UK\.csv', name)
    ]
    if len(candidates) != 1:
        raise RuntimeError(f'expected one Data/NSPL_<MON>_<YYYY>_UK.csv in the zip, found {candidates}')

    logger.info(f'postcode csv: {candidates[0]} ({archive.getinfo(candidates[0]).file_size / (1024 * 1024):.1f} MB)')
    return candidates[0]


# NSPL column names carry the vintage of each geography (lad25cd, lsoa21cd,
# imd20ind ...); map them to the stable names used by NSPLPostcode
def map_column_name(name: str) -> str:

    if name in FIXED_COLUMNS:
        return name

    match = re.fullmatch(r'imd[0-9]{2}ind', name)
    if match:
        return 'imd_rank'

    match = re.fullmatch(r'lep[0-9]{2}cd([12])', name)
    if match:
        return f'lep{match.group(1)}_code'

    match = re.fullmatch(r'([a-z]+)[0-9]{2}(cd|ind)', name)
    if match:
        prefix, kind = match.groups()
        return f'{prefix}_code' if kind == 'cd' else f'{prefix}_ind'

    raise RuntimeError(f'unrecognised NSPL column {name!r}')


def validate_columns(mapped_columns: list[str]) -> None:

    expected = {column.name for column in NSPLPostcode.__table__.columns}
    actual = set(mapped_columns)

    if actual != expected:
        raise RuntimeError(
            f'NSPL columns do not match the nspl_postcode model: '
            f'missing={sorted(expected - actual)} unexpected={sorted(actual - expected)}'
        )

    if len(mapped_columns) != len(actual):
        raise RuntimeError(f'duplicate mapped columns: {mapped_columns}')


# YYYYMM -> YYYY-MM-01, '' -> NULL
def convert_year_month(series: pandas.Series) -> pandas.Series:
    return series.where(series.str.len() == 6, '').str.replace(r'^([0-9]{4})([0-9]{2})$', r'\1-\2-01', regex=True)


def transform_chunk(chunk: pandas.DataFrame) -> pandas.DataFrame:

    chunk['dointr'] = convert_year_month(chunk['dointr'])
    chunk['doterm'] = convert_year_month(chunk['doterm'])

    missing_coordinates = chunk['lat'] == MISSING_LATITUDE
    chunk.loc[missing_coordinates, ['lat', 'long']] = ''

    # the IMD rank is 0 where there is none (Channel Islands, Isle of Man)
    chunk.loc[chunk['imd_rank'] == '0', 'imd_rank'] = ''

    return chunk


# --- lookups --------------------------------------------------------------

def read_lookup_document(archive: zipfile.ZipFile, name: str) -> list[dict]:

    raw = archive.read(name)
    try:
        text = raw.decode('utf-8-sig')
    except UnicodeDecodeError:
        text = raw.decode('latin-1')

    reader = csv.reader(io.StringIO(text))
    header = [column.strip() for column in next(reader)]

    code_index = None
    for index, column in enumerate(header):
        if re.fullmatch(r'[A-Z]+[0-9]{2}(CD|IND)', column):
            code_index = index
            break

    if code_index is None:
        logger.info(f'skip lookup document without a <PREFIX><YY>CD column: {name}')
        return []

    prefix = re.match(r'[A-Z]+', header[code_index]).group(0)
    name_index = next((i for i, c in enumerate(header) if re.fullmatch(rf'{prefix}[0-9]{{2}}(NM|DESC)', c)), None)
    welsh_index = next((i for i, c in enumerate(header) if re.fullmatch(rf'{prefix}[0-9]{{2}}NMW', c)), None)

    if name_index is None:
        logger.info(f'skip lookup document without a name column for {prefix}: {name}')
        return []

    lookup = prefix.lower()
    file_name = os.path.basename(name)
    if re.search(r'\bSC\b', file_name):
        lookup += '_sc'
    elif re.search(r'\bNI\b', file_name):
        lookup += '_ni'

    rows = []
    for row in reader:
        if len(row) <= max(code_index, name_index) or row[code_index].strip() == '':
            continue
        rows.append(
            {
                'lookup': lookup,
                'code': row[code_index].strip(),
                'name': row[name_index].strip(),
                'name_welsh': row[welsh_index].strip() if welsh_index is not None and len(row) > welsh_index and row[welsh_index].strip() != '' else None,
                'source_file': file_name,
            }
        )

    logger.info(f'lookup {lookup}: {len(rows)} codes from {file_name}')
    return rows


def read_all_lookups(archive: zipfile.ZipFile) -> pandas.DataFrame:

    # only the "names and codes" documents: the IMD lookups and the ITL
    # lookup also have <PREFIX><YY>CD columns but for older geography
    # vintages (LSOA11CD ...) and would shadow the current names
    rows = []
    for name in archive.namelist():
        if name.startswith('Documents/') and name.lower().endswith('.csv') and 'names and codes' in name.lower():
            rows.extend(read_lookup_document(archive, name))
        elif name.startswith('Documents/') and name.lower().endswith('.csv'):
            logger.info(f'skip document (not a names and codes file): {name}')

    lookups_df = pandas.DataFrame(rows, columns=['lookup', 'code', 'name', 'name_welsh', 'source_file'])
    before = len(lookups_df)
    lookups_df = lookups_df.drop_duplicates(subset=['lookup', 'code'], keep='first')
    if len(lookups_df) != before:
        logger.warning(f'dropped {before - len(lookups_df)} duplicate (lookup, code) rows')

    logger.info(f'{len(lookups_df)} lookup rows across {lookups_df["lookup"].nunique()} lookups')
    return lookups_df


# --- upload ---------------------------------------------------------------

def upload_to_database(
    process_metadata: ProcessMetadata,
    postgres_connection_string: str,
    archive: zipfile.ZipFile,
    csv_member: str,
    lookups_df: pandas.DataFrame,
) -> tuple[int, int, str]:

    database_upload_start_timestamp = datetime.now(timezone.utc)
    logger.info(f'database upload starting: {database_upload_start_timestamp}')

    postcode_count = 0
    live_postcode_count = 0
    csv_header = None

    # single transaction: truncate then reload both tables, so readers never
    # see a half loaded state. Empty csv fields are NULL under FORMAT csv
    with psycopg.connect(postgres_connection_string) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f'TRUNCATE TABLE {POSTCODE_TABLE}, {LOOKUP_TABLE}')

            with archive.open(csv_member) as csv_file:
                text_file = io.TextIOWrapper(csv_file, encoding='utf-8')

                # read the header line first so the COPY column list is known
                # before the COPY starts; the rest of the stream is then read
                # in chunks inside the COPY block (same pattern as main.py)
                csv_header = text_file.readline().strip()
                mapped_columns = [map_column_name(column) for column in csv_header.split(',')]
                validate_columns(mapped_columns)
                logger.info(f'csv header: {csv_header}')

                chunks = pandas.read_csv(
                    text_file,
                    header=None,
                    names=mapped_columns,
                    dtype=str,
                    keep_default_na=False,
                    chunksize=CHUNK_SIZE,
                )

                copy_columns = '(' + ', '.join(mapped_columns) + ')'
                with cursor.copy(f'COPY {POSTCODE_TABLE} {copy_columns} FROM STDIN WITH (FORMAT csv)') as copy:
                    for chunk in chunks:
                        chunk = transform_chunk(chunk)

                        postcode_count += len(chunk)
                        live_postcode_count += int((chunk['doterm'] == '').sum())

                        buffer = io.StringIO()
                        chunk.to_csv(buffer, index=False, header=False)
                        copy.write(buffer.getvalue())
                        logger.info(f'{POSTCODE_TABLE}: {postcode_count} rows so far')

            lookup_columns = '(lookup, code, name, name_welsh, source_file)'
            buffer = io.StringIO()
            lookups_df.to_csv(buffer, index=False, header=False, na_rep='')
            with cursor.copy(f'COPY {LOOKUP_TABLE} {lookup_columns} FROM STDIN WITH (FORMAT csv)') as lookup_copy:
                lookup_copy.write(buffer.getvalue())

        connection.commit()

    database_upload_complete_timestamp = datetime.now(timezone.utc)
    database_upload_duration = database_upload_complete_timestamp - database_upload_start_timestamp
    process_metadata.database_upload_duration = database_upload_duration

    logger.info(f'database upload complete: {database_upload_complete_timestamp}')
    logger.info(f'database upload duration: {database_upload_duration}')
    logger.info(f'{POSTCODE_TABLE}: {postcode_count} postcodes ({live_postcode_count} live)')
    logger.info(f'{LOOKUP_TABLE}: {len(lookups_df)} rows')

    return (postcode_count, live_postcode_count, csv_header)


def log_summary(postgres_connection_string: str) -> None:

    with psycopg.connect(postgres_connection_string) as connection:
        rows = connection.execute(f"""
            select coalesce(l.name, p.ctry_code), count(*), count(*) filter (where p.doterm is null)
            from {POSTCODE_TABLE} p
            left join {LOOKUP_TABLE} l on l.lookup = 'ctry' and l.code = p.ctry_code
            group by 1 order by 2 desc
        """).fetchall()
        logger.info(f'postcodes by country (total, live):')
        for name, total, live in rows:
            logger.info(f'{name}: {total}, {live}')

        rows = connection.execute(f"""
            select l.name, count(*) filter (where p.doterm is null)
            from {POSTCODE_TABLE} p
            join {LOOKUP_TABLE} l on l.lookup = 'rgn' and l.code = p.rgn_code
            group by 1 order by 2 desc
        """).fetchall()
        logger.info(f'live postcodes by English region:')
        for name, live in rows:
            logger.info(f'{name}: {live}')

        missing_coordinates = connection.execute(f'select count(*) from {POSTCODE_TABLE} where lat is null').fetchone()[0]
        logger.info(f'postcodes without coordinates: {missing_coordinates}')


def update_nspl_metadata(
    process_metadata: ProcessMetadata,
    postgres_engine: Engine,
    release: NSPLRelease,
    csv_member: str,
    csv_header: str,
    postcode_count: int,
    live_postcode_count: int,
    lookup_count: int,
) -> None:

    with Session(postgres_engine) as session:
        row = NSPLMetadata(
            release_name=release.release_name,
            arcgis_item_id=release.item_id,
            source_url=release.source_url,
            csv_file_name=csv_member,
            csv_header=csv_header,
            download_size_bytes=process_metadata.download_size_bytes,
            postcode_count=postcode_count,
            live_postcode_count=live_postcode_count,
            lookup_count=lookup_count,
            process_start_timestamp=process_metadata.process_start_timestamp,
            process_complete_timestamp=process_metadata.process_complete_timestamp,
            process_duration=process_metadata.process_duration,
            download_duration=process_metadata.download_duration,
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

    release = resolve_release()

    temp_file_path = download_zip_retry_wrapper(
        process_metadata=process_metadata,
        url=release.source_url,
    )

    try:
        with zipfile.ZipFile(temp_file_path) as archive:
            csv_member = find_postcode_csv_member(archive)

            if release.release_name == 'pinned':
                match = re.fullmatch(r'Data/NSPL_([A-Z]+)_([0-9]{4})_UK\.csv', csv_member)
                release.release_name = f'{match.group(1).title()} {match.group(2)}'

            lookups_df = read_all_lookups(archive)

            (
                postcode_count,
                live_postcode_count,
                csv_header,
            ) = upload_to_database(
                process_metadata=process_metadata,
                postgres_connection_string=postgres_connection_string,
                archive=archive,
                csv_member=csv_member,
                lookups_df=lookups_df,
            )
    finally:
        os.unlink(temp_file_path)

    log_summary(
        postgres_connection_string=postgres_connection_string,
    )

    logger.info(f'create database engine: postgres_host={environment_variables.get_postgres_host()}')
    sqlalchemy_connection_string = environment_variables.get_postgres_psycopg3_connection_string()
    sqlalchemy_connection_string_redacted = environment_variables.get_postgres_psycopg3_connection_string_redacted()
    logger.info(f'connecting to postgres using sqlalchemy')
    logger.info(f'{sqlalchemy_connection_string_redacted}')
    postgres_engine = create_engine(sqlalchemy_connection_string)

    process_complete_timestamp = datetime.now(timezone.utc)
    process_duration = process_complete_timestamp - process_start_timestamp

    process_metadata.process_start_timestamp = process_start_timestamp
    process_metadata.process_complete_timestamp = process_complete_timestamp
    process_metadata.process_duration = process_duration

    update_nspl_metadata(
        process_metadata=process_metadata,
        postgres_engine=postgres_engine,
        release=release,
        csv_member=csv_member,
        csv_header=csv_header,
        postcode_count=postcode_count,
        live_postcode_count=live_postcode_count,
        lookup_count=len(lookups_df),
    )

    logger.info(f'process finished: {datetime.now(timezone.utc)}')
    logger.info(f'duration: {process_duration}')


if __name__ == '__main__':
    main()
