

import os
import psycopg2
from google.cloud import bigquery
import datetime

# Establish connection to PostgreSQL
conn = psycopg2.connect(database="Marketing",
                        user="postgres",
                        password="Musica321",
                        host="localhost",
                        port="5432")

cur = conn.cursor()

# Fetch data from PostgreSQL
cur.execute('''SELECT ORDER_NUM, ORDER_TYPE, CUST_NAME, PROD_NUMBER, PROD_NAME,
                   QUANTITY, PRICE, DISCOUNT, QUANTITY * PRICE * (1 - DISCOUNT) AS ORDER_TOTAL
               FROM Sales''')
rows = cur.fetchall()

# Set the path to your service account key file
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\Anthony.DESKTOP-ES5HL78\Downloads\inner-orb-349717-213eaa6ac019.json'

# Create a BigQuery client
client = bigquery.Client()

# Specify your project ID and dataset ID
project_id = 'inner-orb-349717'
dataset_id = 'Anthony'  # Remove the project ID from dataset_id

# Construct the BigQuery table schema
schema = [
    bigquery.SchemaField('ORDER_NUM', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('ORDER_TYPE', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('CUST_NAME', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('PROD_NUMBER', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('PROD_NAME', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('QUANTITY', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('PRICE', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('DISCOUNT', 'FLOAT', mode='NULLABLE'),
    bigquery.SchemaField('ORDER_TOTAL', 'FLOAT', mode='NULLABLE'),
]

# Create the BigQuery table with partitioning if it doesn't exist
table_ref = client.dataset(dataset_id, project=project_id).table('Sales4001')  # Specify the project ID in the dataset reference

try:
    table = client.get_table(table_ref)
    print('BigQuery table already exists.')
except:
    table = bigquery.Table(table_ref, schema=schema)
    table.partitioning_type = 'DAY'  # Specify the partitioning type (e.g., by day)
    table.partitioning_field = 'ORDER_NUM'  # Specify the column for partitioning
    table = client.create_table(table)
    print('BigQuery table created successfully.')

# Insert data into the BigQuery table
rows_to_insert = []
for row in rows:
    rows_to_insert.append(list(row))

errors = client.insert_rows(table, rows_to_insert)
# Check for errors in the insert operation
if not errors:
    print('Data inserted into BigQuery table successfully.')
else:
    print('Error occurred while inserting data into BigQuery table:')
    for error in errors:
        print(error)

# Close the PostgreSQL connection
conn.close()
