
from datetime import datetime
from datetime import timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import psycopg

from lib_land_registry_data.lib_env import EnvironmentVariables

from lib_land_registry_data.logging import set_logger_process_name
from lib_land_registry_data.logging import get_logger
from lib_land_registry_data.logging import create_stdout_log_handler
from lib_land_registry_data.logging import create_file_log_handler

from lib_land_registry_data.lib_db import PPAddressValidation
from lib_land_registry_data.lib_db import PPAddressValidationSummary


PROCESS_NAME = 'simple_land_registry_pp_validation'

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


SOURCE_TABLE = 'land_registry.pp_transactions'
NSPL_TABLE = 'ons.nspl_postcode'
LOOKUP_TABLE = 'ons.nspl_code_lookup'
TARGET_TABLE = 'land_registry.pp_address_validation'
ISSUES_VIEW = 'land_registry.pp_address_issues'

WORK_MEM = '512MB'

# thresholds
POSTCODE_DATE_TOLERANCE = "interval '1 year'"   # NSPL dates are month granularity and registration lags completion
NAME_SHARE_THRESHOLD = 0.01                     # a district / county / town used by less than 1% of the sales in its group
NAME_GROUP_MIN_SALES = 100                      # ... provided the group has at least this many sales
STREET_POSTCODE_MIN_SALES = 10                  # a street used once at a postcode with at least this many sales

# checks that count towards issue_count (the others are informational)
ISSUE_CHECKS = [
    'postcode_missing',
    'postcode_not_in_nspl',
    'postcode_terminated_before_sale',
    'district_unusual_for_lad_year',
    'county_unusual_for_lad_year',
    'town_unusual_for_postcode_district',
    'street_unusual_for_postcode',
    'address_unparsed',
    'property_type_conflicts_address',
]

# postcode_introduced_after_sale is informational: 1% of sales, almost all
# 1995-1999, carry a postcode the NSPL introduced later — Royal Mail recodes
# (e.g. the 1998 York YO8 recode) that the Land Registry applied to the
# address, not bad rows
INFORMATIONAL_CHECKS = [
    'postcode_introduced_after_sale',
    'postcode_no_coordinates',
    'district_matches_lad',
    'street_missing',
    'flat_without_identifier',
]

ALL_CHECKS = ISSUE_CHECKS + INFORMATIONAL_CHECKS


# upper case, '&' -> 'AND', punctuation removed, 'X, City of' -> 'CITY OF X',
# so that the PPD district spelling can be compared with the NSPL name
def normalise_area_name_sql(expression: str) -> str:
    return f"""
    btrim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        upper({expression}),
        '^(.*), (CITY|COUNTY|BOROUGH|ROYAL BOROUGH|ISLE) OF$', '\\2 OF \\1'),
        '&', 'AND', 'g'),
        '[^A-Z ]', ' ', 'g'),
        ' +', ' ', 'g'))
    """


def check_prerequisites(cursor: psycopg.Cursor) -> int:

    source_count = cursor.execute(f'select count(*) from {SOURCE_TABLE}').fetchone()[0]
    if source_count == 0:
        raise RuntimeError(f'{SOURCE_TABLE} is empty, run main_pp_transactions.py first')

    nspl_count = cursor.execute(f'select count(*) from {NSPL_TABLE}').fetchone()[0]
    if nspl_count == 0:
        raise RuntimeError(f'{NSPL_TABLE} is empty, run main_nspl.py first')

    logger.info(f'{SOURCE_TABLE}: {source_count} rows, {NSPL_TABLE}: {nspl_count} rows')
    return source_count


BUILD_SQL = f"""
insert into {TARGET_TABLE} (
    transaction_unique_id,
    postcode_missing,
    postcode_not_in_nspl,
    postcode_introduced_after_sale,
    postcode_terminated_before_sale,
    postcode_no_coordinates,
    district_matches_lad,
    district_unusual_for_lad_year,
    county_unusual_for_lad_year,
    town_unusual_for_postcode_district,
    street_missing,
    street_unusual_for_postcode,
    address_unparsed,
    flat_without_identifier,
    property_type_conflicts_address,
    issue_count,
    nspl_lad_code,
    nspl_lad_name
)
with t as (
    select
        s.transaction_unique_id,
        s.postcode,
        s.postcode_district,
        s.transaction_date,
        extract(year from s.transaction_date)::int as sale_year,
        s.district,
        s.county,
        s.town_city,
        s.street,
        s.address_pattern,
        s.is_flat_like,
        s.flat_number,
        s.flat_description,
        s.property_type,
        n.pcds is not null as in_nspl,
        n.dointr,
        n.doterm,
        n.lat,
        n.lad_code,
        l.name as lad_name
    from {SOURCE_TABLE} s
    left join {NSPL_TABLE} n on n.pcds = s.postcode
    left join {LOOKUP_TABLE} l on l.lookup = 'lad' and l.code = n.lad_code
),
-- how often each district / county name is used within a local authority and year
district_share as (
    select lad_code, sale_year, district,
           count(*) as n, sum(count(*)) over (partition by lad_code, sale_year) as total
    from t where lad_code is not null
    group by 1, 2, 3
),
county_share as (
    select lad_code, sale_year, county,
           count(*) as n, sum(count(*)) over (partition by lad_code, sale_year) as total
    from t where lad_code is not null
    group by 1, 2, 3
),
-- how often each town is used within a postcode district
town_share as (
    select postcode_district, town_city,
           count(*) as n, sum(count(*)) over (partition by postcode_district) as total
    from t where postcode_district is not null
    group by 1, 2
),
-- how often each street is used at a postcode
street_share as (
    select postcode, street,
           count(*) as n, sum(count(*)) over (partition by postcode) as total
    from t where postcode is not null
    group by 1, 2
),
checks as (
    select
        t.transaction_unique_id,
        t.postcode is null as postcode_missing,
        t.postcode is not null and not t.in_nspl as postcode_not_in_nspl,
        coalesce(t.in_nspl and t.dointr > t.transaction_date + {POSTCODE_DATE_TOLERANCE}, false) as postcode_introduced_after_sale,
        coalesce(t.in_nspl and t.doterm < t.transaction_date - {POSTCODE_DATE_TOLERANCE}, false) as postcode_terminated_before_sale,
        t.in_nspl and t.lat is null as postcode_no_coordinates,
        t.lad_name is not null and (
            {normalise_area_name_sql('t.district')} = {normalise_area_name_sql('t.lad_name')}
            or {normalise_area_name_sql('t.district')} = {normalise_area_name_sql("regexp_replace(t.lad_name, ', (City|County) of$', '')")}
        ) as district_matches_lad,
        coalesce(ds.total >= {NAME_GROUP_MIN_SALES} and ds.n::numeric / ds.total < {NAME_SHARE_THRESHOLD}, false) as district_unusual_for_lad_year,
        coalesce(cs.total >= {NAME_GROUP_MIN_SALES} and cs.n::numeric / cs.total < {NAME_SHARE_THRESHOLD}, false) as county_unusual_for_lad_year,
        coalesce(ts.total >= {NAME_GROUP_MIN_SALES} and ts.n::numeric / ts.total < {NAME_SHARE_THRESHOLD}, false) as town_unusual_for_postcode_district,
        t.street = '' as street_missing,
        coalesce(t.street <> '' and ss.total >= {STREET_POSTCODE_MIN_SALES} and ss.n = 1, false) as street_unusual_for_postcode,
        t.address_pattern ~ '^(P_EMPTY|P_OTHER)/' or t.address_pattern ~ '/S_OTHER$' as address_unparsed,
        t.is_flat_like and t.flat_number is null and t.flat_description is null as flat_without_identifier,
        t.is_flat_like and t.property_type <> 'F' as property_type_conflicts_address,
        t.lad_code,
        t.lad_name
    from t
    left join district_share ds on ds.lad_code = t.lad_code and ds.sale_year = t.sale_year and ds.district = t.district
    left join county_share cs on cs.lad_code = t.lad_code and cs.sale_year = t.sale_year and cs.county = t.county
    left join town_share ts on ts.postcode_district = t.postcode_district and ts.town_city = t.town_city
    left join street_share ss on ss.postcode = t.postcode and ss.street = t.street
)
select
    transaction_unique_id,
    postcode_missing,
    postcode_not_in_nspl,
    postcode_introduced_after_sale,
    postcode_terminated_before_sale,
    postcode_no_coordinates,
    district_matches_lad,
    district_unusual_for_lad_year,
    county_unusual_for_lad_year,
    town_unusual_for_postcode_district,
    street_missing,
    street_unusual_for_postcode,
    address_unparsed,
    flat_without_identifier,
    property_type_conflicts_address,
    {' + '.join(f'{check}::int' for check in ISSUE_CHECKS)} as issue_count,
    lad_code,
    lad_name
from checks
"""


def build_validation(
    postgres_connection_string: str,
    sqlalchemy_connection_string: str,
) -> int:

    engine = create_engine(sqlalchemy_connection_string)
    indexes = list(PPAddressValidation.__table__.indexes)

    for index in indexes:
        logger.info(f'drop index {index.name}')
        index.drop(engine, checkfirst=True)

    with psycopg.connect(postgres_connection_string) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"set work_mem = '{WORK_MEM}'")
            # parallel query needs shared memory (/dev/shm) that a default
            # docker container does not have much of; the query is fine without
            cursor.execute('set max_parallel_workers_per_gather = 0')

            check_prerequisites(cursor)

            build_start_timestamp = datetime.now(timezone.utc)
            logger.info(f'build {TARGET_TABLE} starting: {build_start_timestamp}')

            cursor.execute(f'truncate table {TARGET_TABLE}')
            cursor.execute(BUILD_SQL)
            row_count = cursor.rowcount

            build_complete_timestamp = datetime.now(timezone.utc)
            logger.info(f'build complete: {build_complete_timestamp}')
            logger.info(f'build duration: {build_complete_timestamp - build_start_timestamp}')
            logger.info(f'{TARGET_TABLE}: {row_count} rows')

        connection.commit()

    for index in indexes:
        logger.info(f'create index {index.name}')
        index.create(engine)

    with psycopg.connect(postgres_connection_string, autocommit=True) as connection:
        logger.info(f'analyze {TARGET_TABLE}')
        connection.execute(f'analyze {TARGET_TABLE}')

        validation_columns = ', '.join(
            f'v.{column.name}' for column in PPAddressValidation.__table__.columns
            if column.name != 'transaction_unique_id'
        )
        logger.info(f'create or replace view {ISSUES_VIEW}')
        connection.execute(f"""
            create or replace view {ISSUES_VIEW} as
            select t.*, {validation_columns}
            from {SOURCE_TABLE} t
            join {TARGET_TABLE} v using (transaction_unique_id)
            where v.issue_count > 0
        """)

    return row_count


def summarise(
    postgres_connection_string: str,
    sqlalchemy_connection_string: str,
    run_timestamp: datetime,
) -> None:

    with psycopg.connect(postgres_connection_string) as connection:
        connection.execute('set max_parallel_workers_per_gather = 0')
        counts = connection.execute(f"""
            select count(*), {', '.join(f'count(*) filter (where {check})' for check in ALL_CHECKS)},
                   count(*) filter (where issue_count > 0),
                   count(*) filter (where issue_count > 1)
            from {TARGET_TABLE}
        """).fetchone()

    total = counts[0]
    check_counts = dict(zip(ALL_CHECKS, counts[1:1 + len(ALL_CHECKS)]))
    any_issue, multiple_issues = counts[1 + len(ALL_CHECKS):]

    logger.info(f'validation summary ({total} rows):')
    for check in ISSUE_CHECKS:
        logger.info(f'{check}: {check_counts[check]} ({100.0 * check_counts[check] / total:.3f}%)')
    logger.info(f'rows with any issue: {any_issue} ({100.0 * any_issue / total:.3f}%), with more than one: {multiple_issues}')
    logger.info(f'informational:')
    for check in INFORMATIONAL_CHECKS:
        logger.info(f'{check}: {check_counts[check]} ({100.0 * check_counts[check] / total:.3f}%)')

    engine = create_engine(sqlalchemy_connection_string)
    with Session(engine) as session:
        for check in ALL_CHECKS:
            session.add(PPAddressValidationSummary(
                run_timestamp=run_timestamp,
                check_name=check,
                flagged_count=check_counts[check],
                total_count=total,
            ))
        session.add(PPAddressValidationSummary(
            run_timestamp=run_timestamp,
            check_name='any_issue',
            flagged_count=any_issue,
            total_count=total,
        ))
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

    sqlalchemy_connection_string = environment_variables.get_postgres_psycopg3_connection_string()

    build_validation(
        postgres_connection_string=postgres_connection_string,
        sqlalchemy_connection_string=sqlalchemy_connection_string,
    )

    summarise(
        postgres_connection_string=postgres_connection_string,
        sqlalchemy_connection_string=sqlalchemy_connection_string,
        run_timestamp=process_start_timestamp,
    )

    process_complete_timestamp = datetime.now(timezone.utc)
    logger.info(f'process finished: {process_complete_timestamp}')
    logger.info(f'duration: {process_complete_timestamp - process_start_timestamp}')


if __name__ == '__main__':
    main()
