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
Bare table names are looked up across all schemas; tables linked by a foreign
key have to be dropped together:

```shell
docker compose run --rm ingestion python /app/init_db.py --recreate                                  # all tables
docker compose run --rm ingestion python /app/init_db.py --recreate pp_complete_data                 # named tables only
docker compose run --rm ingestion python /app/init_db.py --recreate iadb_observation iadb_series     # foreign key pair
docker compose run --rm ingestion python /app/init_db.py --recreate bank_of_england.iadb_metadata    # schema qualified
```

The database data lives in `./postgres_data/` (gitignored), so it survives
`docker system prune` and can be migrated by copying the project directory.
Deleting that directory and re-running the steps above rebuilds the whole
database from scratch.

To stop the database:

```shell
docker compose down
```

# Bank of England data

`main_boe_rates.py` downloads a fixed list of series from the Bank of England
Interactive Statistical Database (see `SERIES_CONFIG` in the script and
`MORTGAGE_RATES.md` for what they are) and reloads three tables in the
`bank_of_england` schema on every run:

- `iadb_series` — one row per series: code, the official Bank of England
  description, frequency (`daily` / `monthly`), category (`bank_rate`,
  `quoted_mortgage_rate`, `effective_mortgage_rate`) and the date range loaded.
- `iadb_observation` — long format, one row per series and date with a
  non-missing value. Monthly series are stamped on the last day of the month;
  the daily Bank Rate is present on business days only.
- `iadb_metadata` — one row per run (append only).

To add a series, add one line to `SERIES_CONFIG`; the description is taken
from the Bank of England response.

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
left join bank_of_england.iadb_observation r
  on r.series_code = 'IUMBV34'
 and date_trunc('month', r.observation_date)::date = p.month
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
