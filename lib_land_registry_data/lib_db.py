
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import BigInteger

from sqlalchemy.orm import DeclarativeBase

from datetime import date
from datetime import datetime
from datetime import timedelta

from typing import Optional


class LandRegistryBase(DeclarativeBase):
    __table_args__ = {'schema': 'land_registry_simple'}


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
    #download_start_timestamp: Mapped[datetime]
    #download_complete_timestamp: Mapped[datetime]
    download_duration: Mapped[timedelta]
    #pandas_read_start_timestamp: Mapped[datetime]
    #pandas_read_complete_timestamp: Mapped[datetime]
    pandas_read_duration: Mapped[timedelta]
    #pandas_datetime_convert_start_timestamp: Mapped[datetime]
    #pandas_datetime_convert_complete_timestamp: Mapped[datetime]
    pandas_datetime_convert_duration: Mapped[timedelta]
    #pandas_write_start_timestamp: Mapped[datetime]
    #pandas_write_complete_timestamp: Mapped[datetime]
    pandas_write_duration: Mapped[timedelta]
    #database_upload_start_timestamp: Mapped[datetime]
    #database_upload_complete_timestamp: Mapped[datetime]
    database_upload_duration: Mapped[timedelta]


class TestTable(LandRegistryBase):

    __tablename__ = 'test_table'

    test_table_id: Mapped[int] = mapped_column(primary_key=True)
    string_column: Mapped[str]
    int_column: Mapped[int]