# Simple-Land-Registry-Data-Ingestion
Simple version of Land Registry Data Ingestion

# Instructions

```shell
pip3 -m venv venv
source ./venv/bin/activate
pip3 install sqlalchemy typeguard psycopg2-binary requests pandas
direnv allow
python3 create_table_pp_complete_data.py
python3 create_table_pp_complete_metdata.py
python3 main.py
```