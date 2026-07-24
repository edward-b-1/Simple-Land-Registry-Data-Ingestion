# TODO

- Remove obsolete test scaffolding: `test.py`, `create_test_table.py`, `test_file.csv`
  and the `TestTable` class in `lib_land_registry_data/lib_db.py` (was prototyping
  for the psycopg3 `COPY FROM STDIN` bulk-load approach now used by `main.py`;
  `test.py` contains placeholder credentials)
- Remove or archive `main_minimal.py` (early non-chunked version of the pipeline,
  contains a hardcoded password)
- Decide whether the `typeguard` dependency stays (only used for `@typechecked`
  decorators in `lib_land_registry_data/logging.py`)
- Consider replacing the `create_table_*.py` scripts with `init_db.py` (idempotent,
  used by the docker ingestion container)
- Remove the old docker named volume once migrated to the `./postgres_data/` bind
  mount: `docker volume rm simple-land-registry-data-ingestion_postgres_data`
- `lib_land_registry_data/lib_datetime_not_used.py` is unused (per its name)
