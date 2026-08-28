
from datetime import datetime
from datetime import timezone

from sqlalchemy import create_engine

import psycopg

from lib_land_registry_data.lib_env import EnvironmentVariables

from lib_land_registry_data.logging import set_logger_process_name
from lib_land_registry_data.logging import get_logger
from lib_land_registry_data.logging import create_stdout_log_handler
from lib_land_registry_data.logging import create_file_log_handler

from lib_land_registry_data.lib_db import PPTransactions


PROCESS_NAME = 'simple_land_registry_pp_transactions'

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


SCHEMA_NAME = 'land_registry'
SOURCE_TABLE = f'{SCHEMA_NAME}.pp_complete_data'
TARGET_TABLE = f'{SCHEMA_NAME}.pp_transactions'
MARKET_VIEW = f'{SCHEMA_NAME}.pp_market_transactions'

# provisional thresholds for is_plausible_price (see TODO.md: these need a
# proper study). Below the minimum are nominal transfers, part shares and
# data errors; above the maximum are portfolio / commercial deals
PLAUSIBLE_PRICE_MIN = 10_000
PLAUSIBLE_PRICE_MAX = 50_000_000

# the sort-heavy window functions below benefit from more memory than the
# postgres default (4MB); this is a per-session setting
WORK_MEM = '512MB'


# pp-complete.txt is a resolved snapshot: every row is an addition. The
# monthly update files also carry change (C) and delete (D) records, which
# this step does not handle, so fail loudly if they ever appear
def check_source_table(cursor: psycopg.Cursor) -> int:

    row_count = cursor.execute(f'select count(*) from {SOURCE_TABLE}').fetchone()[0]
    logger.info(f'{SOURCE_TABLE}: {row_count} rows')

    if row_count == 0:
        raise RuntimeError(f'{SOURCE_TABLE} is empty, run main.py first')

    non_add_count = cursor.execute(f"select count(*) from {SOURCE_TABLE} where record_op <> 'A'").fetchone()[0]
    if non_add_count != 0:
        raise RuntimeError(f'{SOURCE_TABLE} has {non_add_count} rows with record_op other than A, which this step does not resolve')

    null_cat_count = cursor.execute(f'select count(*) from {SOURCE_TABLE} where ppd_cat is null').fetchone()[0]
    if null_cat_count != 0:
        raise RuntimeError(f'{SOURCE_TABLE} has {null_cat_count} rows with a null ppd_cat')

    return row_count


BUILD_SQL = f"""
insert into {TARGET_TABLE} (
    transaction_unique_id,
    price,
    transaction_date,
    transaction_month,
    postcode,
    postcode_area,
    postcode_district,
    postcode_sector,
    property_type,
    is_new_build,
    tenure,
    is_leasehold,
    primary_address_object_name,
    secondary_address_object_name,
    street,
    locality,
    town_city,
    district,
    county,
    ppd_cat,
    property_key,
    is_plausible_price,
    duplicate_count,
    is_multi_sale_same_day,
    is_market_transaction
)
with base as (
    -- type fixes and postcode normalisation; row_hash identifies exact
    -- duplicates (every field except the id) with a narrow sort key
    select
        transaction_unique_id,
        price,
        transaction_date::date as transaction_date,
        nullif(btrim(postcode), '') as postcode,
        property_type,
        new_tag,
        lease,
        primary_address_object_name as paon,
        secondary_address_object_name as saon,
        street,
        locality,
        town_city,
        district,
        county,
        ppd_cat,
        md5(concat_ws('|',
            price::text, transaction_date::date::text, btrim(postcode), property_type, new_tag, lease,
            primary_address_object_name, secondary_address_object_name, street, locality,
            town_city, district, county, ppd_cat
        )) as row_hash
    from {SOURCE_TABLE}
),
deduplicated as (
    -- collapse exact duplicates onto the lowest transaction id
    select
        *,
        count(*) over (partition by row_hash) as duplicate_count,
        row_number() over (partition by row_hash order by transaction_unique_id) as duplicate_rank
    from base
),
keyed as (
    select
        *,
        case
            when postcode is not null and paon <> '' then postcode || '|' || paon || '|' || saon
        end as property_key
    from deduplicated
    where duplicate_rank = 1
),
flagged as (
    select
        *,
        price between %(price_min)s and %(price_max)s as is_plausible_price,
        case
            when property_key is not null then count(*) over (partition by property_key, transaction_date) > 1
            else false
        end as is_multi_sale_same_day
    from keyed
)
select
    transaction_unique_id,
    price,
    transaction_date,
    date_trunc('month', transaction_date)::date as transaction_month,
    postcode,
    substring(postcode from '^[A-Z]{{1,2}}') as postcode_area,
    split_part(postcode, ' ', 1) as postcode_district,
    left(postcode, length(postcode) - 2) as postcode_sector,
    property_type,
    new_tag = 'Y' as is_new_build,
    lease as tenure,
    case lease when 'L' then true when 'F' then false end as is_leasehold,
    paon,
    saon,
    street,
    locality,
    town_city,
    district,
    county,
    ppd_cat,
    property_key,
    is_plausible_price,
    duplicate_count,
    is_multi_sale_same_day,
    ppd_cat = 'A' and is_plausible_price and not is_multi_sale_same_day as is_market_transaction
from flagged
"""


def build_pp_transactions(
    postgres_connection_string: str,
    sqlalchemy_connection_string: str,
) -> int:

    # indexes are dropped for the bulk insert and rebuilt afterwards; the
    # truncate + insert happen in one transaction so readers never see an
    # empty table
    engine = create_engine(sqlalchemy_connection_string)
    indexes = list(PPTransactions.__table__.indexes)

    for index in indexes:
        logger.info(f'drop index {index.name}')
        index.drop(engine, checkfirst=True)

    with psycopg.connect(postgres_connection_string) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"set work_mem = '{WORK_MEM}'")

            source_row_count = check_source_table(cursor)

            build_start_timestamp = datetime.now(timezone.utc)
            logger.info(f'build {TARGET_TABLE} starting: {build_start_timestamp}')

            cursor.execute(f'truncate table {TARGET_TABLE}')
            cursor.execute(
                BUILD_SQL,
                {
                    'price_min': PLAUSIBLE_PRICE_MIN,
                    'price_max': PLAUSIBLE_PRICE_MAX,
                },
            )
            target_row_count = cursor.rowcount

            build_complete_timestamp = datetime.now(timezone.utc)
            logger.info(f'build complete: {build_complete_timestamp}')
            logger.info(f'build duration: {build_complete_timestamp - build_start_timestamp}')
            logger.info(f'{TARGET_TABLE}: {target_row_count} rows ({source_row_count - target_row_count} duplicate rows collapsed)')

        connection.commit()

    for index in indexes:
        index_start_timestamp = datetime.now(timezone.utc)
        logger.info(f'create index {index.name}')
        index.create(engine)
        logger.info(f'create index {index.name} duration: {datetime.now(timezone.utc) - index_start_timestamp}')

    with psycopg.connect(postgres_connection_string, autocommit=True) as connection:
        logger.info(f'analyze {TARGET_TABLE}')
        connection.execute(f'analyze {TARGET_TABLE}')

        logger.info(f'create or replace view {MARKET_VIEW}')
        connection.execute(f'create or replace view {MARKET_VIEW} as select * from {TARGET_TABLE} where is_market_transaction')

    return target_row_count


def log_summary(postgres_connection_string: str) -> None:

    with psycopg.connect(postgres_connection_string) as connection:
        row = connection.execute(f"""
            select
                count(*),
                count(*) filter (where ppd_cat = 'A'),
                count(*) filter (where ppd_cat = 'B'),
                count(*) filter (where postcode is null),
                count(*) filter (where property_key is null),
                count(*) filter (where not is_plausible_price),
                count(*) filter (where duplicate_count > 1),
                sum(duplicate_count - 1),
                count(*) filter (where is_multi_sale_same_day),
                count(*) filter (where is_market_transaction),
                min(transaction_date),
                max(transaction_date)
            from {TARGET_TABLE}
        """).fetchone()

    (
        total, cat_a, cat_b, no_postcode, no_property_key, implausible,
        had_duplicates, duplicates_collapsed, multi_sale, market, min_date, max_date,
    ) = row

    logger.info(f'summary of {TARGET_TABLE}:')
    logger.info(f'rows: {total} ({min_date} -> {max_date})')
    logger.info(f'ppd_cat A: {cat_a}, B: {cat_b}')
    logger.info(f'no postcode: {no_postcode}, no property_key: {no_property_key}')
    logger.info(f'implausible price (outside {PLAUSIBLE_PRICE_MIN}..{PLAUSIBLE_PRICE_MAX}): {implausible}')
    logger.info(f'rows that had exact duplicates: {had_duplicates} ({duplicates_collapsed} duplicate rows collapsed)')
    logger.info(f'multi sale same day: {multi_sale}')
    logger.info(f'market transactions ({MARKET_VIEW}): {market}')


def main():

    process_start_timestamp = datetime.now(timezone.utc)

    logger.info(f'{PROCESS_NAME} start')

    environment_variables = EnvironmentVariables()

    logger.info(f'postgres_host: {environment_variables.get_postgres_host()}')
    postgres_connection_string = environment_variables.get_psycopg3_postgres_connection_string_as_key_value_pairs()
    postgres_connection_string_redacted = environment_variables.get_psycopg3_postgres_connection_string_as_key_value_pairs_redacted()
    logger.info(f'connecting to postgres using psycopg3')
    logger.info(f'{postgres_connection_string_redacted}')

    build_pp_transactions(
        postgres_connection_string=postgres_connection_string,
        sqlalchemy_connection_string=environment_variables.get_postgres_psycopg3_connection_string(),
    )

    log_summary(
        postgres_connection_string=postgres_connection_string,
    )

    process_complete_timestamp = datetime.now(timezone.utc)
    logger.info(f'process finished: {process_complete_timestamp}')
    logger.info(f'duration: {process_complete_timestamp - process_start_timestamp}')


if __name__ == '__main__':
    main()
