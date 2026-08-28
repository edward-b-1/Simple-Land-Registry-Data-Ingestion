
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import BigInteger
from sqlalchemy import ForeignKey
from sqlalchemy import Numeric
from sqlalchemy import String

from sqlalchemy.orm import DeclarativeBase

from datetime import date
from datetime import datetime
from datetime import timedelta

from decimal import Decimal

from typing import Optional


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


# one row per series downloaded from the Bank of England Interactive Database
# (IADB); truncated and reloaded on every run. `description` is the official
# text from the IADB, `frequency` and `category` come from the series config
# in main_boe_rates.py
class IADBSeries(BankOfEnglandBase):

    __tablename__ = 'iadb_series'

    series_code: Mapped[str] = mapped_column(String(16), primary_key=True)
    description: Mapped[str]
    frequency: Mapped[str] = mapped_column(String(16)) # 'daily' | 'monthly'
    category: Mapped[str] = mapped_column(String(32)) # 'bank_rate' | 'quoted_mortgage_rate' | 'effective_mortgage_rate'
    first_observation_date: Mapped[date]
    last_observation_date: Mapped[date]
    observation_count: Mapped[int]
    download_timestamp: Mapped[datetime]


# long format: one row per (series, date) with a non-missing value. Monthly
# series are stamped on the last day of the month, the daily Bank Rate on
# business days only
class IADBObservation(BankOfEnglandBase):

    __tablename__ = 'iadb_observation'

    series_code: Mapped[str] = mapped_column(
        String(16),
        ForeignKey('bank_of_england.iadb_series.series_code'),
        primary_key=True,
    )
    observation_date: Mapped[date] = mapped_column(primary_key=True)
    value: Mapped[Decimal] = mapped_column(Numeric(12, 6))


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


# every declarative base (one per data source); init_db.py creates the schema
# and tables for all of them
all_bases: list[type[DeclarativeBase]] = [LandRegistryBase, BankOfEnglandBase]
