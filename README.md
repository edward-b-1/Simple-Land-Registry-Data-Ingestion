# Simple-Land-Registry-Data-Ingestion
Simple version of Land Registry Data Ingestion

Downloads the UK Land Registry Price Paid dataset (`pp-complete.txt`, ~5 GB)
and syncs it into a Postgres database.

# Running with Docker (recommended)

Postgres runs in a local Docker container. Its data is stored in a bind-mounted
project directory (`./postgres_data/`) rather than a Docker named volume, as a
local directory is easier to manage and migrate. The ingestion process runs as
a one-shot container.

```shell
# start the database (stays running in the background)
docker compose up -d postgres

# run the data sync (creates schema/tables if needed, then downloads and loads the data)
docker compose run --rm ingestion
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

Log files are written to `./logs/` on the host (the ingestion process also
logs to stdout, so `docker logs` works too).

To connect to the dockerized database from the host:

```shell
psql -h localhost -p 5432 -U postgres postgres
```

To drop and recreate tables (e.g. after changing a model in `lib_db.py`):

```shell
docker compose run --rm ingestion python /app/init_db.py --recreate                   # all tables
docker compose run --rm ingestion python /app/init_db.py --recreate pp_complete_data  # named tables only
```

The database data lives in `./postgres_data/` (gitignored), so it survives
`docker system prune` and can be migrated by copying the project directory.
Deleting that directory and re-running the steps above rebuilds the whole
database from scratch.

To stop the database:

```shell
docker compose down
```

# Running natively (without Docker)

The project is managed with [uv](https://docs.astral.sh/uv/).

```shell
uv sync
direnv allow   # exports POSTGRES_HOST / POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_DATABASE from .envrc
uv run python init_db.py
uv run python main.py
```
