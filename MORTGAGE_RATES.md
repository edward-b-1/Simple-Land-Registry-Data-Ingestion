# Mortgage rate data for analysis against Price Paid data

Analysis against the Bank of England base rate is possible, but what matters
to buyers is the mortgage rate they actually pay. Although those rates come
from high-street lenders, the Bank of England already collects them from the
lenders and publishes aggregates for free, so no scraping of individual banks
is needed.

All of the series below are downloaded by `main_boe_rates.py`, one request
per series, into one narrow table each in the `bank_of_england` schema
(`<table> (observation_date, value)`, missing observations stored as NULL),
with `bank_of_england.iadb_series` as the catalogue (see the README).

## Bank of England quoted and effective rates (the main source)

The BoE Interactive Statistical Database (IADB, Bankstats tables G1.3 / G1.4)
has two families of monthly series, all compiled from lender submissions and
downloadable as CSV.

### Bank Rate

| Series | Table | Frequency | From |
|---|---|---|---|
| IUDBEDR — Official Bank Rate | `bank_rate` | daily (business days) | 1995 |
| IUMABEDR — Monthly average of official Bank Rate | `bank_rate_monthly_average` | monthly | 1995 |

### Quoted household interest rates

The average *advertised* rate across major lenders for a given product
(the descriptions are abridged from the official IADB text).

| Product | Series | Table | From |
|---|---|---|---|
| 2-year fixed, 75% LTV | IUMBV34 | `mortgage_2y_fixed_75_ltv` | 1995 |
| 2-year fixed, 90% LTV | IUMB482 | `mortgage_2y_fixed_90_ltv` | 2008 |
| 3-year fixed, 75% LTV | IUMBV37 | `mortgage_3y_fixed_75_ltv` | 1995 |
| 5-year fixed, 75% LTV | IUMBV42 | `mortgage_5y_fixed_75_ltv` | 1995 |
| 2-year variable, 75% LTV | IUMBV48 | `mortgage_2y_variable_75_ltv` | 1997 |
| 2-year variable, 90% LTV | IUMB479 | `mortgage_2y_variable_90_ltv` | 2008 (51 missing months) |
| Lifetime tracker | IUMBV24 | `mortgage_lifetime_tracker` | 1997 (ends March 2025) |
| Revert-to-rate (standard variable rate) | IUMTLMV | `mortgage_standard_variable_rate` | 1995 |

The start dates of the core series line up almost exactly with the Price Paid
data (1995).

### Effective interest rates

The rates *actually paid*, weighted by lending volume. These are the best
measure of what buyers in a given month actually signed up to.

| Measure | Series | Table | From |
|---|---|---|---|
| Outstanding stock of loans secured on dwellings | CFMHSDE | `effective_rate_outstanding_stock` | 1999 |
| New advances, floating rate | CFMBJ39 | `effective_rate_new_floating` | 2004 |
| New advances, initial fixation ≤ 1 year | CFMBJ42 | `effective_rate_new_fixed_le_1y` | 2004 (ends 2015) |
| New advances, initial fixation > 1 year ≤ 5 years | CFMBJ43 | `effective_rate_new_fixed_1y_to_5y` | 2004 (ends 2015) |
| New advances, initial fixation > 5 years ≤ 10 years | CFMBJ44 | `effective_rate_new_fixed_5y_to_10y` | 2004 (ends 2015) |
| New advances, initial fixation > 10 years | CFMBJ45 | `effective_rate_new_fixed_gt_10y` | 2004 (ends 2015) |

The fixation-band split of new advances stops at the end of 2015 in the IADB;
a replacement series for total new advances post-2015 is still to be
identified.

Both families are far more useful than the base rate alone, because the
spread between base rate and mortgage rates is a story in itself: it blew out
in 2008–09 (base rate fell to 0.5% but mortgage rates barely moved), narrowed
through the 2010s, and lagged again in 2022.

### Accessing the IADB programmatically

`https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp`
with query parameters `csv.x=yes`, `Datefrom=01/Jan/1995`, `Dateto=now`,
`SeriesCodes=<comma separated codes>`, `CSVF=TT`, `UsingCodes=Y`, `VPD=Y`,
`VFD=N` returns a CSV with a `SERIES,DESCRIPTION` header block followed by
the data. Two quirks: the default `python-requests` User-Agent is rejected
with HTTP 403 (a browser-like one works), and an unknown series code is
answered with a 302 redirect to an error page rather than an error status.
Missing values are empty or `..`.

## Supporting sources

- **MLAR** (BoE/FCA Mortgage Lenders & Administrators Return) — quarterly
  aggregates from 2007: new advances by LTV band, income multiple, fixed vs
  variable, first-time-buyer share. Allows modelling the *typical borrower*,
  not just the typical rate.
- **BoE mortgage approvals** (series LPMVTVX) — monthly volumes; a well-known
  leading indicator of transactions by roughly 2–3 months. Worth joining
  against monthly transaction counts from the Price Paid data.
- **Moneyfacts** — the source of "average 2-year fix hits X%" press headlines.
  Daily and granular, but commercial; only historical headline figures are
  free.
- **UK Finance** (formerly the Council of Mortgage Lenders) — lending and
  affordability statistics, partly free.

Loan-level data (FCA Product Sales Data) exists but is not public, so the
BoE aggregates are the practical ceiling.

## What it unlocks

The most interesting derived measure is **payment-to-income** rather than
price-to-income:

1. Take the regional median price for the month.
2. Assume 75% LTV and a 25-year repayment term at that month's 2-year fixed
   quoted rate.
3. Compute the monthly repayment.
4. Divide by regional median earnings (ONS ASHE).

This is how affordability actually feels to a buyer. It explains why prices
kept rising through the 2010s despite high price-to-earnings ratios (cheap
money) and why 2022–23 bit so hard.

## Modelling caveats

- Since roughly 2010 most UK borrowers are on 2- or 5-year fixes, so rate
  changes hit existing owners with a lag. The *new-business* rate is what
  matters for transaction prices; the *outstanding-stock* rate matters for
  forced-sale pressure.
- Completions lag mortgage offers by a few months, so match the rate at
  approximately `transaction_date − 3 months`.
- Monthly series are stamped on the last day of the month in the database, so
  join on `date_trunc('month', ...)`.
