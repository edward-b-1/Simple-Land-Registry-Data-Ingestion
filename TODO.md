# TODO

- Decide whether the `typeguard` dependency stays (only used for `@typechecked`
  decorators in `lib_land_registry_data/logging.py`)
- `main_minimal.py` (kept as a quick manual test against the throw-away
  dockerized database) has a hardcoded connection string pointing at
  `192.168.1.232`; update it to read the `POSTGRES_*` environment variables
