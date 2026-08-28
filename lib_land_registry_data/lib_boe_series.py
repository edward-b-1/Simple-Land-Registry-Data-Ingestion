
from dataclasses import dataclass


# Bank of England Interactive Statistical Database (IADB) series loaded by
# main_boe_rates.py. Each series is downloaded on its own and lands in its own
# narrow table `bank_of_england.<table_name> (observation_date, value)`; the
# tables are generated from this list in lib_db.py. The official description
# of each series is taken from the IADB response, the comments here are only
# for the reader.

FREQUENCY_DAILY = 'daily'
FREQUENCY_MONTHLY = 'monthly'

CATEGORY_BANK_RATE = 'bank_rate'
CATEGORY_QUOTED_MORTGAGE_RATE = 'quoted_mortgage_rate'
CATEGORY_EFFECTIVE_MORTGAGE_RATE = 'effective_mortgage_rate'


@dataclass(frozen=True)
class SeriesConfig():
    code: str
    table_name: str
    frequency: str
    category: str


BOE_SERIES_CONFIG: list[SeriesConfig] = [
    SeriesConfig('IUDBEDR',  'bank_rate',                          FREQUENCY_DAILY,   CATEGORY_BANK_RATE),               # official Bank Rate
    SeriesConfig('IUMABEDR', 'bank_rate_monthly_average',          FREQUENCY_MONTHLY, CATEGORY_BANK_RATE),               # monthly average of Bank Rate
    SeriesConfig('IUMBV34',  'mortgage_2y_fixed_75_ltv',           FREQUENCY_MONTHLY, CATEGORY_QUOTED_MORTGAGE_RATE),    # 2 year fixed, 75% LTV
    SeriesConfig('IUMB482',  'mortgage_2y_fixed_90_ltv',           FREQUENCY_MONTHLY, CATEGORY_QUOTED_MORTGAGE_RATE),    # 2 year fixed, 90% LTV
    SeriesConfig('IUMBV37',  'mortgage_3y_fixed_75_ltv',           FREQUENCY_MONTHLY, CATEGORY_QUOTED_MORTGAGE_RATE),    # 3 year fixed, 75% LTV
    SeriesConfig('IUMBV42',  'mortgage_5y_fixed_75_ltv',           FREQUENCY_MONTHLY, CATEGORY_QUOTED_MORTGAGE_RATE),    # 5 year fixed, 75% LTV
    SeriesConfig('IUMBV48',  'mortgage_2y_variable_75_ltv',        FREQUENCY_MONTHLY, CATEGORY_QUOTED_MORTGAGE_RATE),    # 2 year variable, 75% LTV
    SeriesConfig('IUMB479',  'mortgage_2y_variable_90_ltv',        FREQUENCY_MONTHLY, CATEGORY_QUOTED_MORTGAGE_RATE),    # 2 year variable, 90% LTV
    SeriesConfig('IUMBV24',  'mortgage_lifetime_tracker',          FREQUENCY_MONTHLY, CATEGORY_QUOTED_MORTGAGE_RATE),    # lifetime tracker
    SeriesConfig('IUMTLMV',  'mortgage_standard_variable_rate',    FREQUENCY_MONTHLY, CATEGORY_QUOTED_MORTGAGE_RATE),    # revert-to-rate (standard variable rate)
    SeriesConfig('CFMHSDE',  'effective_rate_outstanding_stock',   FREQUENCY_MONTHLY, CATEGORY_EFFECTIVE_MORTGAGE_RATE), # outstanding stock of loans secured on dwellings
    SeriesConfig('CFMBJ39',  'effective_rate_new_floating',        FREQUENCY_MONTHLY, CATEGORY_EFFECTIVE_MORTGAGE_RATE), # new advances, floating rate
    SeriesConfig('CFMBJ42',  'effective_rate_new_fixed_le_1y',     FREQUENCY_MONTHLY, CATEGORY_EFFECTIVE_MORTGAGE_RATE), # new advances, initial fixation <= 1 year
    SeriesConfig('CFMBJ43',  'effective_rate_new_fixed_1y_to_5y',  FREQUENCY_MONTHLY, CATEGORY_EFFECTIVE_MORTGAGE_RATE), # new advances, initial fixation > 1 year <= 5 years
    SeriesConfig('CFMBJ44',  'effective_rate_new_fixed_5y_to_10y', FREQUENCY_MONTHLY, CATEGORY_EFFECTIVE_MORTGAGE_RATE), # new advances, initial fixation > 5 years <= 10 years
    SeriesConfig('CFMBJ45',  'effective_rate_new_fixed_gt_10y',    FREQUENCY_MONTHLY, CATEGORY_EFFECTIVE_MORTGAGE_RATE), # new advances, initial fixation > 10 years
]
