import prestodb
import pandas as pd

connection = prestodb.dbapi.connect(
    host = 'localhost',
    catalog = 'cassandra',
    user = 'GuhCh',
    port = '8080',
    schema = 'data'
)

pin_cursor = connection.cursor()

pin_cursor.execute('SELECT * FROM pinterest_data')
rows = pin_cursor.fetchall()
pin_df = pd.DataFrame(rows, columns = [i[0] for i in pin_cursor.description])
print(pin_df.head(10))

pin_cursor.execute('SELECT site_index,category,title FROM pinterest_data')
rows = pin_cursor.fetchall()
smaller_df = pd.DataFrame(rows, columns = [i[0] for i in pin_cursor.description])
print(smaller_df.head(10))