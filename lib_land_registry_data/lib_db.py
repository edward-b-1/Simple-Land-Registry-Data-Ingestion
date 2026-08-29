
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import BigInteger
from sqlalchemy import Index
from sqlalchemy import Numeric
from sqlalchemy import String

from sqlalchemy.orm import DeclarativeBase

from datetime import date
from datetime import datetime
from datetime import timedelta

from decimal import Decimal

from typing import Optional

from lib_land_registry_data.lib_boe_series import SeriesConfig
from lib_land_registry_data.lib_boe_series import BOE_SERIES_CONFIG


class LandRegistryBase(DeclarativeBase):
    __table_args__ = {'schema': 'land_registry'}


class PPCompleteData(LandRegistryBase):

    __tablename__ = 'pp_complete_data'

    price_paid_data_id: Mapped[int] = mapped_column(primary_key=True)
    transaction_unique_id: Mapped[str]
    price: Mapped[int]
    transaction_date: Mapped[datetime]
    postcode: Mapped[str]
    property_type: Mapped[str]
    new_tag: Mapped[str]
    lease: Mapped[str]
    primary_address_object_name: Mapped[str]
    secondary_address_object_name: Mapped[str]
    street: Mapped[str]
    locality: Mapped[str]
    town_city: Mapped[str]
    district: Mapped[str]
    county: Mapped[str]
    ppd_cat: Mapped[Optional[str]] # TODO: should this be optional?
    record_op: Mapped[str]


# auto_date is the most recent date observed in the transaction_date column
# it can be used to estimate hold old the data is
class PPCompleteMetadata(LandRegistryBase):

    __tablename__ = 'pp_complete_metadata'

    pp_complete_metadata_id: Mapped[int] = mapped_column(primary_key=True)
    download_size_MB: Mapped[int] = mapped_column(BigInteger)
    auto_date: Mapped[date]
    process_start_timestamp: Mapped[datetime]
    process_complete_timestamp: Mapped[datetime]
    process_duration: Mapped[timedelta]
    download_duration: Mapped[timedelta]
    pandas_read_duration: Mapped[timedelta]
    pandas_datetime_convert_duration: Mapped[timedelta]
    pandas_write_duration: Mapped[timedelta]
    database_upload_duration: Mapped[timedelta]


# Bank of England data lives in its own schema because it does not come from
# the UK Land Registry. Tables in different schemas of the same database join
# freely with schema-qualified names (or via search_path).
class BankOfEnglandBase(DeclarativeBase):
    __table_args__ = {'schema': 'bank_of_england'}


# catalogue: one row per series downloaded from the Bank of England
# Interactive Database (IADB); truncated and reloaded on every run.
# `description` is the official text from the IADB, `table_name`,
# `frequency` and `category` come from lib_boe_series.py
class IADBSeries(BankOfEnglandBase):

    __tablename__ = 'iadb_series'

    series_code: Mapped[str] = mapped_column(String(16), primary_key=True)
    table_name: Mapped[str] = mapped_column(String(64))
    description: Mapped[str]
    frequency: Mapped[str] = mapped_column(String(16)) # 'daily' | 'monthly'
    category: Mapped[str] = mapped_column(String(32)) # 'bank_rate' | 'quoted_mortgage_rate' | 'effective_mortgage_rate'
    first_observation_date: Mapped[Optional[date]] # first / last date with a value
    last_observation_date: Mapped[Optional[date]]
    observation_count: Mapped[int] # rows with a value
    missing_count: Mapped[int] # rows the IADB reports as missing ('..'), stored with value NULL
    download_timestamp: Mapped[datetime]


# one narrow table per series: `bank_of_england.<table_name> (observation_date,
# value)`. Rows only exist inside the period the IADB reports for the series;
# within that period a missing observation ('..' in the download) is a row
# with value NULL. Monthly series are stamped on the last day of the month,
# the daily Bank Rate exists on business days only
class IADBSeriesObservationMixin():

    observation_date: Mapped[date] = mapped_column(primary_key=True)
    value: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6))


def _create_iadb_series_table(config: SeriesConfig) -> type[BankOfEnglandBase]:
    class_name = f'IADBSeriesTable_{config.code}'
    return type(
        class_name,
        (IADBSeriesObservationMixin, BankOfEnglandBase),
        {
            '__tablename__': config.table_name,
            '__doc__': f'Bank of England IADB series {config.code}',
        },
    )


# series code -> model class, e.g. iadb_series_tables['IUMBV34']
iadb_series_tables: dict[str, type[BankOfEnglandBase]] = {
    config.code: _create_iadb_series_table(config)
    for config in BOE_SERIES_CONFIG
}


# one row per ingestion run (append only), same idea as pp_complete_metadata
class IADBMetadata(BankOfEnglandBase):

    __tablename__ = 'iadb_metadata'

    iadb_metadata_id: Mapped[int] = mapped_column(primary_key=True)
    date_from: Mapped[date]
    series_count: Mapped[int]
    observation_count: Mapped[int]
    download_size_bytes: Mapped[int] = mapped_column(BigInteger)
    process_start_timestamp: Mapped[datetime]
    process_complete_timestamp: Mapped[datetime]
    process_duration: Mapped[timedelta]
    download_duration: Mapped[timedelta]
    parse_duration: Mapped[timedelta]
    database_upload_duration: Mapped[timedelta]


# Office for National Statistics data (the National Statistics Postcode
# Lookup, NSPL) in its own schema
class ONSBase(DeclarativeBase):
    __table_args__ = {'schema': 'ons'}


# one row per postcode (current and terminated) from the NSPL. The NSPL's
# column names carry the vintage of each geography (lad25cd, lsoa21cd ...);
# they are mapped to stable names here and the raw header is recorded in
# nspl_metadata. Codes such as S99999999 / E99999999 are the NSPL's own
# "not applicable" values and are kept as-is
class NSPLPostcode(ONSBase):

    __tablename__ = 'nspl_postcode'
    __table_args__ = (
        Index('ix_nspl_postcode_lad_code', 'lad_code'),
        Index('ix_nspl_postcode_lsoa_code', 'lsoa_code'),
        {'schema': 'ons'},
    )

    pcds: Mapped[str] = mapped_column(String(8), primary_key=True) # 'AB1 0AA', same form as pp_transactions.postcode
    pcd7: Mapped[str] = mapped_column(String(7))
    pcd8: Mapped[str] = mapped_column(String(8))
    dointr: Mapped[Optional[date]] # date of introduction (first of month)
    doterm: Mapped[Optional[date]] # date of termination, NULL when live
    usrtypind: Mapped[Optional[str]] = mapped_column(String(1)) # 0 small user, 1 large user
    east1m: Mapped[Optional[int]] # OS grid reference, NULL when not available
    north1m: Mapped[Optional[int]]
    gridind: Mapped[Optional[str]] = mapped_column(String(1)) # grid reference positional quality
    lat: Mapped[Optional[Decimal]] = mapped_column(Numeric(9, 6)) # NULL when not available
    long: Mapped[Optional[Decimal]] = mapped_column(Numeric(9, 6))
    oa_code: Mapped[Optional[str]] = mapped_column(String(9)) # census output area
    cty_code: Mapped[Optional[str]] = mapped_column(String(9)) # county
    ced_code: Mapped[Optional[str]] = mapped_column(String(9)) # county electoral division
    lad_code: Mapped[Optional[str]] = mapped_column(String(9)) # local authority district
    wd_code: Mapped[Optional[str]] = mapped_column(String(9)) # electoral ward
    nhser_code: Mapped[Optional[str]] = mapped_column(String(9)) # NHS England region
    ctry_code: Mapped[Optional[str]] = mapped_column(String(9)) # country
    rgn_code: Mapped[Optional[str]] = mapped_column(String(9)) # region (England only)
    pcon_code: Mapped[Optional[str]] = mapped_column(String(9)) # Westminster constituency
    ttwa_code: Mapped[Optional[str]] = mapped_column(String(9)) # travel to work area
    itl_code: Mapped[Optional[str]] = mapped_column(String(9)) # international territorial level
    npark_code: Mapped[Optional[str]] = mapped_column(String(9)) # national park
    lsoa_code: Mapped[Optional[str]] = mapped_column(String(9)) # lower layer super output area
    msoa_code: Mapped[Optional[str]] = mapped_column(String(9)) # middle layer super output area
    wz_code: Mapped[Optional[str]] = mapped_column(String(9)) # workplace zone
    sicbl_code: Mapped[Optional[str]] = mapped_column(String(9)) # sub integrated care board location
    bua_code: Mapped[Optional[str]] = mapped_column(String(9)) # built up area
    ruc_ind: Mapped[Optional[str]] = mapped_column(String(8)) # rural urban classification
    oac_ind: Mapped[Optional[str]] = mapped_column(String(3)) # output area classification
    lep1_code: Mapped[Optional[str]] = mapped_column(String(9)) # local enterprise partnership
    lep2_code: Mapped[Optional[str]] = mapped_column(String(9))
    pfa_code: Mapped[Optional[str]] = mapped_column(String(9)) # police force area
    imd_rank: Mapped[Optional[int]] # index of multiple deprivation rank of the LSOA
    icb_code: Mapped[Optional[str]] = mapped_column(String(9)) # integrated care board


# names for the codes above, from the "names and codes" documents shipped
# in the NSPL zip. `lookup` is the geography prefix ('lad', 'rgn', 'lsoa',
# 'ruc' ...), suffixed '_sc' / '_ni' for the Scottish / Northern Irish
# variants of a classification
class NSPLCodeLookup(ONSBase):

    __tablename__ = 'nspl_code_lookup'

    lookup: Mapped[str] = mapped_column(String(16), primary_key=True)
    code: Mapped[str] = mapped_column(String(16), primary_key=True)
    name: Mapped[str]
    name_welsh: Mapped[Optional[str]]
    source_file: Mapped[str]


# one row per ingestion run (append only)
class NSPLMetadata(ONSBase):

    __tablename__ = 'nspl_metadata'

    nspl_metadata_id: Mapped[int] = mapped_column(primary_key=True)
    release_name: Mapped[str] # e.g. 'May 2026'
    arcgis_item_id: Mapped[str]
    source_url: Mapped[str]
    csv_file_name: Mapped[str]
    csv_header: Mapped[str] # the raw column names, which carry the geography vintages
    download_size_bytes: Mapped[int] = mapped_column(BigInteger)
    postcode_count: Mapped[int]
    live_postcode_count: Mapped[int]
    lookup_count: Mapped[int]
    process_start_timestamp: Mapped[datetime]
    process_complete_timestamp: Mapped[datetime]
    process_duration: Mapped[timedelta]
    download_duration: Mapped[timedelta]
    database_upload_duration: Mapped[timedelta]


# every declarative base (one per data source); init_db.py creates the schema
# and tables for all of them
all_bases: list[type[DeclarativeBase]] = [LandRegistryBase, BankOfEnglandBase, ONSBase]


# cleaned, analysis-ready copy of pp_complete_data, rebuilt by
# main_pp_transactions.py after every load. Exact duplicate rows are collapsed
# (duplicate_count keeps the count); the flags mark rows that price indices
# and repeat-sales analyses should exclude. The raw table is left untouched
class PPTransactions(LandRegistryBase):

    __tablename__ = 'pp_transactions'
    __table_args__ = (
        Index('ix_pp_transactions_transaction_date', 'transaction_date'),
        Index('ix_pp_transactions_transaction_month', 'transaction_month'),
        Index('ix_pp_transactions_postcode', 'postcode'),
        Index('ix_pp_transactions_property_key', 'property_key'),
        Index('ix_pp_transactions_property_key_normalised', 'property_key_normalised'),
        Index('ix_pp_transactions_ppd_cat_transaction_date', 'ppd_cat', 'transaction_date'),
        {'schema': 'land_registry'},
    )

    transaction_unique_id: Mapped[str] = mapped_column(primary_key=True)
    price: Mapped[int] = mapped_column(BigInteger)
    transaction_date: Mapped[date]
    transaction_month: Mapped[date] # first day of the month, for joins to monthly series
    postcode: Mapped[Optional[str]] # trimmed, NULL when missing
    postcode_area: Mapped[Optional[str]] # 'CR'
    postcode_district: Mapped[Optional[str]] # 'CR4'
    postcode_sector: Mapped[Optional[str]] # 'CR4 4'
    property_type: Mapped[str] # D / S / T / F / O
    is_new_build: Mapped[bool]
    tenure: Mapped[str] # F / L / U (unknown)
    is_leasehold: Mapped[Optional[bool]] # NULL when tenure is unknown
    primary_address_object_name: Mapped[str]
    secondary_address_object_name: Mapped[str]
    street: Mapped[str]
    locality: Mapped[str]
    town_city: Mapped[str]
    district: Mapped[str]
    county: Mapped[str]
    ppd_cat: Mapped[str] # A = standard price paid, B = additional (repossessions, non-private, type O)
    # PAON / SAON normalised into parts, see lib_address.py
    address_pattern: Mapped[str] = mapped_column(String(32)) # which PAON/SAON rule fired, e.g. 'P_NAME_NUMBER/S_FLAT'
    is_flat_like: Mapped[bool] # property_type F, or the address itself says flat/apartment
    building_number: Mapped[Optional[str]] # street number '12A' or range '17-19'
    building_name: Mapped[Optional[str]] # 'MILNER COURT'
    flat_number: Mapped[Optional[str]] # flats only: '3', 'D', 'G.03'
    flat_description: Mapped[Optional[str]] # flats only: 'FIRST FLOOR FLAT'
    unit_description: Mapped[Optional[str]] # non-flats: 'UNIT 4', 'GARAGE 2'
    plot_number: Mapped[Optional[str]] # 'PLOT 4' -> '4'
    property_key: Mapped[Optional[str]] # raw: postcode|PAON|SAON, NULL when postcode or PAON missing
    property_key_normalised: Mapped[Optional[str]] # postcode|number-or-name|flat, from the normalised parts
    is_plausible_price: Mapped[bool] # see PLAUSIBLE_PRICE_* in main_pp_transactions.py
    duplicate_count: Mapped[int] # rows in pp_complete_data collapsed into this one
    is_multi_sale_same_day: Mapped[bool] # same property_key sold more than once on the date
    is_market_transaction: Mapped[bool] # ppd_cat A, plausible price, single sale that day
