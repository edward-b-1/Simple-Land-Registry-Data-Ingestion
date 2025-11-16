
postgres_connection_string = 'user=postgres password=adminpassword host=192.168.1.232 dbname=postgres port=5432'

from datetime import datetime
from datetime import timezone
def datetime_now():
    return datetime.now(timezone.utc)
print(f'start: {datetime_now()}')

import requests
url = 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.txt'
response = requests.get(url, allow_redirects=True)
pp_complete_data = response.content
pp_complete_data_MB = len(pp_complete_data) / (1024 * 1024)
print(f'download complete: {datetime_now()}')

import io
import pandas
df = pandas.read_csv(io.BytesIO(pp_complete_data), header=None, dtype=str, keep_default_na=False,)
print(f'pandas read complete: {datetime_now()}')
df_pp_complete_columns = [
    'transaction_unique_id',
    'price',
    'transaction_date',
    'postcode',
    'property_type',
    'new_tag',
    'lease',
    'primary_address_object_name',
    'secondary_address_object_name',
    'street',
    'locality',
    'town_city',
    'district',
    'county',
    'ppd_cat',
    'record_op',
]
df.columns = df_pp_complete_columns
df['transaction_date'] = pandas.to_datetime(arg=df['transaction_date'], utc=True, format='%Y-%m-%d %H:%M')
print(f'datetime conversion complete: {datetime_now()}')

import psycopg
#file = io.StringIO(pp_complete_data.decode('utf-8'))
file = io.StringIO()
df.to_csv(file, index=False, header=False)
file.seek(0)
print(f'write to StringIO complete: {datetime_now()}')

columns = '(transaction_unique_id, price, transaction_date, postcode, property_type, new_tag, lease, primary_address_object_name, secondary_address_object_name, street, locality, town_city, district, county, ppd_cat, record_op)'
with psycopg.connect(postgres_connection_string) as conn:
    with conn.cursor() as cur:
        with cur.copy(f'COPY land_registry_simple.pp_complete_data {columns} FROM STDIN WITH (FORMAT csv, NULL \'\\N\')') as copy:
            copy.write(file.read())
    conn.commit()
print(f'upload to database complete: {datetime_now()}')