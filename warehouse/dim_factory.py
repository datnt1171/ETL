import pandas as pd
import numpy as np
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

database_name = os.getenv('STAGING_NAME')
database_user = os.getenv('STAGING_USER')
database_password = os.getenv('STAGING_PASSWORD')
database_host = os.getenv('STAGING_HOST')
database_port = os.getenv('STAGING_PORT')

conn = psycopg2.connect(database=database_name, user=database_user, password=database_password
                        ,host=database_host, port=database_port)
cur = conn.cursor()

cur.execute("""SELECT DISTINCT factory_code, factory_name
            FROM copr13
            ORDER BY factory_code""")
data = cur.fetchall()
column_names = [description[0] for description in cur.description]
df_factory = pd.DataFrame(data = data, columns = column_names)

df_factory.drop_duplicates(subset='factory_code', inplace=True)

# Connect to Warehouse
database_name = os.getenv('WAREHOUSE_NAME')
database_user = os.getenv('WAREHOUSE_USER')
database_password = os.getenv('WAREHOUSE_PASSWORD')
database_host = os.getenv('WAREHOUSE_HOST')
database_port = os.getenv('WAREHOUSE_PORT')

conn = psycopg2.connect(database=database_name, user=database_user, password=database_password
                        ,host=database_host, port=database_port)
cur = conn.cursor()
# Upsert query
upsert_query = """
INSERT INTO dim_factory (factory_code, factory_name)
VALUES (%s, %s)
ON CONFLICT (factory_code)
DO UPDATE SET
    factory_name = EXCLUDED.factory_name;
"""

# Execute the upsert for each row in data
for _, row in df_factory.iterrows():  # Iterate over rows as (index, Series)
    cur.execute(upsert_query, (row["factory_code"], row["factory_name"]))

# Commit changes
conn.commit()