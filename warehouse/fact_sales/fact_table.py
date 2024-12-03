import pandas as pd
import numpy as np
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

from utils import insert_data

# Load
database_name = os.getenv('WAREHOUSE_NAME')
database_user = os.getenv('WAREHOUSE_USER')
database_password = os.getenv('WAREHOUSE_PASSWORD')
database_host = os.getenv('WAREHOUSE_HOST')
database_port = os.getenv('WAREHOUSE_PORT')

conn = psycopg2.connect(database=database_name, user=database_user, password=database_password
                        ,host=database_host, port=database_port)
cur = conn.cursor()

cur.execute("""SELECT max(import_timestamp) from fact_sales""")
latest_import = cur.fetchone()[0]
latest_import

# Connect
database_name = os.getenv('STAGING_NAME')
database_user = os.getenv('STAGING_USER')
database_password = os.getenv('STAGING_PASSWORD')
database_host = os.getenv('STAGING_HOST')
database_port = os.getenv('STAGING_PORT')

conn = psycopg2.connect(database=database_name, user=database_user, password=database_password
                            , host=database_host, port=database_port)
cur = conn.cursor()


# Extract
cur.execute("""SELECT product_code,product_name,qc,factory_code,
            sales_date,sales_code,order_code,sales_quantity,
            unit,package_sales_quantity,
            package_unit,
            department,salesman,
            warehouse_code,warehouse_type, import_code, factory_order_code,
            import_timestamp
            FROM copr23""")
            # WHERE import_timestamp > %(latest_import)s
            #         """,{'latest_import':latest_import})

data = cur.fetchall()
column_names = [description[0] for description in cur.description]
df_copr23 = pd.DataFrame(data = data, columns = column_names)
df_copr23.shape

# Transform
df_copr23_date_cols = ['sales_date']
for col in df_copr23_date_cols:
    df_copr23[col] = pd.to_datetime(df_copr23[col], dayfirst=True)
df_copr23.shape
df_copr23['first_4_sales_code'] = df_copr23['sales_code'].str.split("-").str[0]
df_copr23 = df_copr23[df_copr23['first_4_sales_code']=='2301']

df_copr23.dropna(subset='qc', inplace=True)


df_copr23 = df_copr23.replace(np.nan, None)
for col in df_copr23_date_cols:
    df_copr23[col] = df_copr23[col].astype(object).where(df_copr23[col].notnull(), None)

df_copr23.drop(columns=['first_4_sales_code'], inplace=True)

df_copr23['import_wh_timestamp'] = datetime.now()
df_copr23.shape

# Load
database_name = os.getenv('WAREHOUSE_NAME')
database_user = os.getenv('WAREHOUSE_USER')
database_password = os.getenv('WAREHOUSE_PASSWORD')
database_host = os.getenv('WAREHOUSE_HOST')
database_port = os.getenv('WAREHOUSE_PORT')

conn = psycopg2.connect(database=database_name, user=database_user, password=database_password
                        ,host=database_host, port=database_port)
cur = conn.cursor()

insert_data(df_copr23, "fact_sales", conn)