import pandas as pd
import numpy as np
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

from utils import insert_data


# latest import
# Load
database_name = os.getenv('WAREHOUSE_NAME')
database_user = os.getenv('WAREHOUSE_USER')
database_password = os.getenv('WAREHOUSE_PASSWORD')
database_host = os.getenv('WAREHOUSE_HOST')
database_port = os.getenv('WAREHOUSE_PORT')

conn = psycopg2.connect(database=database_name, user=database_user, password=database_password
                        ,host=database_host, port=database_port)
cur = conn.cursor()

cur.execute("""SELECT max(import_timestamp) from fact_order""")
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
cur.execute("""SELECT order_date, order_code, ct_date, factory_code, factory_order_code,
                    tax_type, department, salesman,
                    deposit_rate, payment_registration_code, payment_registration_name,
                    delivery_address,
                    product_code, product_name, qc, 
                    warehouse_type, order_quantity, delivered_quantity,
                    package_order_quantity, delivered_package_order_quantity,
                    unit, package_unit,
                    estimated_delivery_date, original_estimated_delivery_date,
                    pre_ct, finish_code, import_timestamp
                FROM copr13""")
                # WHERE import_timestamp > %(latest_import)s
                #     """,{'latest_import':latest_import})

data = cur.fetchall()
column_names = [description[0] for description in cur.description]
df_copr13 = pd.DataFrame(data = data, columns = column_names)
df_copr13.shape
conn.close()

# Transform
df_copr13_date_cols = ['order_date','ct_date','estimated_delivery_date','original_estimated_delivery_date']
for col in df_copr13_date_cols:
    df_copr13[col] = pd.to_datetime(df_copr13[col], dayfirst=True)

df_copr13['first_4_order_code'] = df_copr13['order_code'].str.split("-").str[0]
df_copr13 = df_copr13[df_copr13['first_4_order_code']=='2201']

tax_type_mapping = {'Thu? c?ng rieng':'Thuế cộng riêng',
                    'Thu? su?t b?ng 0': 'Thuế suất bằng 0',
                    'Bao g?m thu?': 'Bao gồm thuế'}
df_copr13['tax_type'] = df_copr13['tax_type'].map(tax_type_mapping)

df_copr13.dropna(subset='qc', inplace=True)
df_copr13['factory_code'] = df_copr13['factory_code'].str.replace(r'.0','', regex=False)


df_copr13 = df_copr13.replace(np.nan, None)
for col in df_copr13_date_cols:
    df_copr13[col] = df_copr13[col].astype(object).where(df_copr13[col].notnull(), None)

df_copr13.drop(columns=['first_4_order_code'], inplace=True)



df_KDT = df_copr13[df_copr13['factory_code']=='30895.2'][['order_code','factory_code','factory_order_code']]
df_KDT.fillna("temp", inplace=True)

df_KDT.loc[df_KDT['factory_order_code'].str.contains('ST', case=False, na=False), 'factory_code'] = "30895.1"
df_KDT.loc[df_KDT['factory_order_code'].str.contains('TN', case=False, na=False), 'factory_code'] = "30895"
df_KDT.loc[df_KDT['factory_order_code'].str.contains('BP', case=False, na=False), 'factory_code'] = "30895.5"
df_KDT.loc[df_KDT['factory_order_code'].str.contains('QT', case=False, na=False), 'factory_code'] = "30895.4"

df_KDT.columns = ['order_code','factory_code_fixed','factory_order_code']

df_copr13 = df_copr13.merge(df_KDT[['order_code','factory_code_fixed']], on='order_code', how='left')
# Replace values in df1's column with df2's column
df_copr13['factory_code'] = df_copr13['factory_code_fixed'].combine_first(df_copr13['factory_code'])
df_copr13.drop(columns=['factory_code_fixed'], inplace=True)

# Get the import time
df_copr13['import_wh_timestamp'] = datetime.now()
# Load
database_name = os.getenv('WAREHOUSE_NAME')
database_user = os.getenv('WAREHOUSE_USER')
database_password = os.getenv('WAREHOUSE_PASSWORD')
database_host = os.getenv('WAREHOUSE_HOST')
database_port = os.getenv('WAREHOUSE_PORT')

conn = psycopg2.connect(database=database_name, user=database_user, password=database_password
                        ,host=database_host, port=database_port)
cur = conn.cursor()

insert_data(df_copr13, "fact_order", conn)