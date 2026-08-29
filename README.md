# Simple-Land-Registry-Data-Ingestion
Simple version of Land Registry Data Ingestion

Downloads the UK Land Registry Price Paid dataset (`pp-complete.txt`, ~5 GB)
and syncs it into a Postgres database, together with supporting datasets for
analysis. Each dataset is loaded by its own one-shot process so they can be
refreshed independently:

| Dataset | Compose service | Schema | Script |
|---|---|---|---|
| Land Registry Price Paid data | `ingestion` | `land_registry` | `main.py` |
| Bank of England interest rates (Bank Rate, quoted and effective mortgage rates) | `boe-ingestion` | `bank_of_england` | `main_boe_rates.py` |
| ONS National Statistics Postcode Lookup (postcode → coordinates, LSOA, local authority, region, rural/urban) | `ons-ingestion` | `ons` | `main_nspl.py` |

# Running with Docker (recommended)

Postgres runs in a local Docker container. Its data is stored in a bind-mounted
project directory (`./postgres_data/`) rather than a Docker named volume, as a
local directory is easier to manage and migrate. Each ingestion process runs
as a one-shot container.

```shell
# start the database (stays running in the background)
docker compose up -d postgres

# run the Land Registry data sync (creates schema/tables if needed, then downloads and loads the data)
docker compose run --rm ingestion

# run the Bank of England interest rate sync (a few seconds)
docker compose run --rm boe-ingestion

# run the ONS postcode lookup sync (~190 MB download, a couple of minutes)
docker compose run --rm ons-ingestion
```

Both services share one image. Compose does not rebuild it automatically, so
after changing any of the python code add `--build`:

```shell
docker compose run --build --rm boe-ingestion
```

Configuration lives in `docker-compose.yml` and is read from a local `.env`
file (gitignored). Create it from the committed template and set your own
values, e.g. a password and, if host port 5432 is already in use, another port:

```shell
cp .env.example .env
```

Shell environment variables take precedence over `.env`, so one-off overrides
also work:

```shell
POSTGRES_HOST_PORT=5433 docker compose up -d postgres
```

Log files are written to `./logs/` on the host (the ingestion processes also
log to stdout, so `docker logs` works too).

To connect to the dockerized database from the host:

```shell
psql -h localhost -p 5432 -U postgres postgres
```

To drop and recreate tables (e.g. after changing a model in `lib_db.py`).
Bare table names are looked up across all schemas:

```shell
docker compose run --rm ingestion python /app/init_db.py --recreate                                  # all tables
docker compose run --rm ingestion python /app/init_db.py --recreate pp_complete_data                 # named tables only
docker compose run --rm ingestion python /app/init_db.py --recreate bank_of_england.iadb_series      # schema qualified
```

The database data lives in `./postgres_data/` (gitignored), so it survives
`docker system prune` and can be migrated by copying the project directory.
Deleting that directory and re-running the steps above rebuilds the whole
database from scratch.

To stop the database:

```shell
docker compose down
```

# Cleaned Price Paid data

`main.py` loads `pp-complete.txt` as-is into `land_registry.pp_complete_data`
(the audit copy, no indexes). `main_pp_transactions.py` then rebuilds
`land_registry.pp_transactions`, the analysis-ready version, from it; the
`ingestion` service runs both. It can also be run on its own after changing
the cleaning rules:

```shell
docker compose run --rm ingestion python /app/main_pp_transactions.py
```

What the cleaning does (details and counts in `ANALYSIS_IDEAS.md`, open
questions in `TODO.md`):

- `transaction_date` becomes a `date`; `transaction_month` (first of the
  month) is added for joins to monthly series.
- Postcodes are trimmed and empty ones stored as NULL; `postcode_area`,
  `postcode_district` and `postcode_sector` are derived as stable geography
  (`district` / `county` names change over time).
- `is_new_build` and `is_leasehold` booleans (`tenure` keeps the raw
  F / L / U code, `is_leasehold` is NULL for U).
- PAON / SAON are split into `building_number`, `building_name`,
  `flat_number` and `flat_description` (flat-like rows), `unit_description`
  (the rest) and `plot_number`, with `address_pattern` recording which rule
  fired (rules and examples in `lib_land_registry_data/lib_address.py`).
  `is_flat_like` is true when `property_type` is F *or* the address itself
  says flat/apartment — the source sometimes types a flat as a house.
- `property_key` = raw `postcode|PAON|SAON`, and `property_key_normalised` =
  `postcode|number-or-name|flat` from the split parts, for repeat-sales
  matching (both NULL when the postcode is missing).
- Exact duplicate rows (identical apart from the id, typically one row per
  title in a portfolio sale) are collapsed onto one row, with
  `duplicate_count` recording how many there were.
- Flags: `is_plausible_price` (provisional thresholds, see `TODO.md`),
  `is_multi_sale_same_day` (same property sold more than once on the date),
  and `is_market_transaction` = category A, plausible price, single sale.
- The view `land_registry.pp_market_transactions` selects
  `is_market_transaction` rows and is the default input for price analysis.

# Bank of England data

`main_boe_rates.py` downloads a fixed list of series from the Bank of England
Interactive Statistical Database, one request per series (see
`lib_land_registry_data/lib_boe_series.py` for the list and
`MORTGAGE_RATES.md` for what they are), and reloads the `bank_of_england`
schema on every run:

- One narrow table per series, `<table_name> (observation_date, value)`,
  e.g. `bank_rate` (daily official Bank Rate), `bank_rate_monthly_average`,
  `mortgage_2y_fixed_75_ltv`, `mortgage_5y_fixed_75_ltv`,
  `mortgage_standard_variable_rate`, `effective_rate_outstanding_stock`, ...
  Rows exist only for the period the Bank of England reports the series;
  within that period a missing observation is a row with `value` NULL.
  Monthly series are stamped on the last day of the month; the daily Bank
  Rate is present on business days only.
- `iadb_series` — the catalogue: one row per series with its Bank of England
  code, table name, official description, frequency (`daily` / `monthly`),
  category (`bank_rate`, `quoted_mortgage_rate`, `effective_mortgage_rate`),
  first/last date with a value, and observation / missing counts.
- `iadb_metadata` — one row per run (append only).

To add a series, add one line to `BOE_SERIES_CONFIG` (code, table name,
frequency, category) and run `init_db.py`; the table model is generated from
the list and the description is taken from the Bank of England response.

# ONS postcode data

`main_nspl.py` finds the latest quarterly release of the ONS National
Statistics Postcode Lookup on the Open Geography Portal (an ArcGIS hub, found
through its search API; set `NSPL_ARCGIS_ITEM_ID` to pin a release instead),
downloads the zip and reloads the `ons` schema on every run:

- `nspl_postcode` — one row per postcode, current and terminated (`doterm`
  is NULL for live postcodes), with grid reference, latitude/longitude
  (NULL when the ONS has none), and the code of every geography the NSPL
  carries: output area, LSOA/MSOA, local authority (`lad_code`), county,
  ward, region (`rgn_code`, England only), country, constituency, travel to
  work area, built-up area, rural/urban classification (`ruc_ind`), IMD
  rank, and more. The NSPL's column names include the vintage of each
  geography (`lad25cd`, `lsoa21cd`); they are mapped to stable names and the
  raw header is kept in `nspl_metadata.csv_header`.
- `nspl_code_lookup` — names for the codes, from the "names and codes"
  documents in the zip: `lookup` is the geography (`lad`, `rgn`, `ctry`,
  `cty`, `lsoa`, `msoa`, `pcon`, `ruc`, ...), plus `code`, `name` and the
  Welsh name where given.
- `nspl_metadata` — one row per run (append only).

`pcds` has the same single-space form as `pp_transactions.postcode`, so the
join is a plain equality; include terminated postcodes, since older sales
were registered against postcodes that have since been retired.

`nspl_postcode` columns (the NSPL name is in brackets; `*_code` values are
GSS codes, look them up in `nspl_code_lookup` with the same prefix):

| column | meaning |
|---|---|
| `pcds`, `pcd7`, `pcd8` | the postcode in single-space, 7-character and 8-character fixed-width forms |
| `dointr`, `doterm` | month the postcode was introduced / terminated (NULL = live) |
| `usrtypind` | 0 = small user (normal), 1 = large user (single organisation) |
| `east1m`, `north1m`, `gridind` | OS grid reference in metres and its positional quality (1 = within the building, 8 = postcode centroid, 9 = none) |
| `lat`, `long` | WGS84 coordinates of the postcode centroid, NULL when `gridind` = 9 |
| `oa_code` (oa21cd) | census output area, the smallest statistical area |
| `lsoa_code`, `msoa_code` (lsoa21cd, msoa21cd) | lower / middle layer super output areas (~1,500 / ~7,500 people) |
| `lad_code` (lad25cd) | local authority district |
| `cty_code`, `ced_code` (cty25cd, ced25cd) | county and county electoral division (E99999999 where none) |
| `wd_code` (wd25cd) | electoral ward |
| `rgn_code` (rgn25cd) | region — England only, W99999999 etc. elsewhere |
| `ctry_code` (ctry25cd) | country (E92000001 England, W92000004 Wales, ...) |
| `pcon_code` (pcon24cd) | Westminster parliamentary constituency |
| `ttwa_code` (ttwa15cd) | travel to work area |
| `itl_code` (itl25cd) | international territorial level 3 (successor to NUTS3) |
| `bua_code` (bua24cd) | built-up area |
| `npark_code` (npark16cd) | national park |
| `ruc_ind` (ruc21ind) | rural/urban classification, e.g. UN1 = urban nearer a major town |
| `oac_ind` (oac11ind) | 2011 output area classification (demographic cluster, e.g. 5B3) |
| `imd_rank` (imd20ind) | index of multiple deprivation rank of the LSOA, 1 = most deprived, per country (England 1–32,844) |
| `wz_code` (wz11cd) | workplace zone |
| `nhser_code`, `icb_code`, `sicbl_code` | NHS England region, integrated care board, sub-ICB location |
| `lep1_code`, `lep2_code` | local enterprise partnership(s) |
| `pfa_code` (pfa23cd) | police force area |

# Address and postcode validation

`main_pp_validation.py` (compose service `pp-validation`, needs both the
Price Paid and NSPL data loaded) checks every row of `pp_transactions`
against the NSPL and against the other sales at the same place, and writes
one row per transaction to `land_registry.pp_address_validation`:

- postcode checks: missing, unknown to the NSPL, terminated more than a
  year before the sale; and, informationally, introduced more than a year
  after the sale (Royal Mail recodes that the Land Registry applied to old
  sales, concentrated in 1995–1999) and no coordinates;
- geography name checks: `district` / `county` used by fewer than 1% of the
  sales in the same local authority and year, `town_city` used by fewer than
  1% of the sales in the same postcode district, and whether `district`
  equals the NSPL local authority name after normalisation;
- address checks: street used once at a postcode with 10+ sales, street
  missing, PAON/SAON that fitted no pattern, flat without any identifier,
  address says flat but `property_type` says house.

`issue_count` sums the checks that indicate a bad row (the purely
informational ones are excluded), the view
`land_registry.pp_address_issues` joins the flagged rows back to their
transactions, and `pp_address_validation_summary` keeps the counts per
check per run.

# Querying across datasets

All datasets live in one database in separate schemas, so they join directly
using schema-qualified names, e.g. monthly median price against the quoted
2-year fixed rate (75% LTV):

```sql
with monthly_price as (
    select date_trunc('month', transaction_date)::date as month,
           percentile_cont(0.5) within group (order by price) as median_price,
           count(*) as transactions
    from land_registry.pp_complete_data
    where ppd_cat = 'A'
    group by 1
)
select p.month, p.median_price, p.transactions, r.value as two_year_fixed_75_ltv
from monthly_price p
left join bank_of_england.mortgage_2y_fixed_75_ltv r
  on date_trunc('month', r.observation_date)::date = p.month
order by p.month desc;
```

To drop the schema prefixes, set the search path for the session or make it
the default for the role:

```sql
set search_path to land_registry, bank_of_england, public;
alter role land_registry set search_path = land_registry, bank_of_england, public;
```

# Running natively (without Docker)

The project is managed with [uv](https://docs.astral.sh/uv/).

```shell
uv sync
direnv allow   # exports POSTGRES_HOST / POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_DATABASE from .envrc
uv run python init_db.py
uv run python main.py
uv run python main_boe_rates.py
```
