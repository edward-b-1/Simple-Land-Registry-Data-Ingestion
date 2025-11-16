
postgres_connection_string = 'user=someuser password=somepassword host=somehost dbname=postgres port=5432'

import pandas
df = pandas.read_csv('test_file.csv', header=None)
import psycopg
import io
file = io.StringIO()
df.to_csv(file, index=False, header=False)
file.seek(0)
columns = '(string_column, int_column)'
with psycopg.connect(postgres_connection_string) as conn:
    with conn.cursor() as cur:
        with cur.copy(f'COPY land_registry_simple.test_table {columns} FROM STDIN WITH (FORMAT csv)') as copy:
            copy.write(file.read())
    conn.commit()
