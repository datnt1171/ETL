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
df_factory['factory_code'] = df_factory['factory_code'].str.replace(r'.0','', regex=False)
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
for _, row in df_factory.iterrows():  
    cur.execute(upsert_query, (row["factory_code"], row["factory_name"]))

# Commit changes
conn.commit()

df_revenue = pd.read_excel(r"D:\VL1251\A Dũng\revenue.xlsx")
insert_query = """
INSERT INTO revenue_by_month (factory_code, year,
                month,
                revenue,
                revenue_predict)
VALUES (%s, %s, %s, %s, %s);
"""
df_revenue['factory_code'] = df_revenue['factory_code'].astype(str)
df_revenue['factory_code'] = df_revenue['factory_code'].str.replace(r'.0','', regex=False)
for _, row in df_revenue.iterrows():  
    cur.execute(insert_query, (row["factory_code"], row["year"], row["month"], row["revenue"], row["Predict"]))