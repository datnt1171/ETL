import pandas as pd
import numpy as np
import psycopg2
import os
from utils import insert_data

from dotenv import load_dotenv
load_dotenv()

database_name = os.getenv('WAREHOUSE_NAME')
database_user = os.getenv('WAREHOUSE_USER')
database_password = os.getenv('WAREHOUSE_PASSWORD')
database_host = os.getenv('WAREHOUSE_HOST')
database_port = os.getenv('WAREHOUSE_PORT')

conn = psycopg2.connect(database=database_name, user=database_user, password=database_password
                            , host=database_host, port=database_port)
cur = conn.cursor()



# User ######################################################################
# User ######################################################################
cur.execute("""DROP TABLE users""")
cur.execute("""CREATE TABLE users(
            username TEXT PRIMARY KEY,
            password TEXT,
            role TEXT)""")
# User ######################################################################
# User ######################################################################




# Dim ######################################################################
# Dim ######################################################################
# Dim ######################################################################
# Dim ######################################################################

# dim_date ######################################################################
# dim_date ######################################################################
start_date = '2000-01-01'
end_date = '2050-12-31'
date_range = pd.date_range(start=start_date, end=end_date)


df_date = pd.DataFrame({
    'date': date_range,
    'year': date_range.year,
    'month': date_range.month,
    'month_name': date_range.strftime('%B'),
    'day': date_range.day,
    'day_of_week': date_range.dayofweek,
    'day_name': date_range.strftime('%A'),
    'week_of_year': date_range.isocalendar().week,
    'quarter': date_range.quarter,
    'is_weekend': date_range.dayofweek >= 5,  # 5 and 6 are Saturday and Sunday
})

df_date['week_of_year'] = df_date['week_of_year'].astype(int) # Convert Uint to int
cur.execute("""DROP TABLE dim_date""")
cur.execute("""CREATE TABLE dim_date(
            date date PRIMARY KEY,
            year int,
            month int,
            month_name text,
            day int,
            day_of_week int,
            day_name text,
            week_of_year int,
            quarter int,
            is_weekend boolean)""")
conn.commit()

insert_data(df_date, "dim_date", conn)
# dim_date ######################################################################
# dim_date ######################################################################

# dim_factory ######################################################################
# dim_factory ######################################################################
cur.execute("""DROP TABLE dim_factory""")
cur.execute("""CREATE TABLE dim_factory(
            factory_code TEXT PRIMARY KEY,
            factory_name TEXT)""")
conn.commit()

# dim_factory ######################################################################
# dim_factory ######################################################################


# dim_product ######################################################################
# dim_product ######################################################################
cur.execute("""DROP TABLE dim_product""")
cur.execute("""CREATE TABLE dim_product(
            product_code TEXT PRIMARY KEY,
            product_name TEXT)""")
conn.commit()

# dim_product ######################################################################
# dim_product ######################################################################


# Dim ######################################################################
# Dim ######################################################################
# Dim ######################################################################
# Dim ######################################################################


# Fact ######################################################################
# Fact ######################################################################
# Fact ######################################################################
# Fact ######################################################################
conn.commit()
# Order ######################################################################
# Order ######################################################################
cur.execute("""DROP TABLE fact_order""")
cur.execute("""CREATE TABLE fact_order(
            order_date DATE,
            order_code TEXT PRIMARY KEY,
            ct_date DATE,
            factory_code TEXT,
            factory_order_code TEXT,
            tax_type TEXT,
            department TEXT,
            salesman TEXT,
            deposit_rate TEXT,
            payment_registration_code TEXT,
            payment_registration_name TEXT,
            delivery_address TEXT,
            product_code TEXT,
            product_name TEXT,
            qc TEXT,
            warehouse_type TEXT,
            order_quantity REAL,
            delivered_quantity REAL,
            package_order_quantity REAL,
            delivered_package_order_quantity REAL,
            unit TEXT,
            package_unit TEXT,
            estimated_delivery_date DATE,
            original_estimated_delivery_date DATE,
            pre_ct TEXT,
            finish_code TEXT,

            import_timestamp TIMESTAMP,
            import_wh_timestamp TIMESTAMP

            )""")
conn.commit()
# Order ######################################################################
# Order ######################################################################

# Fact ######################################################################
# Fact ######################################################################
cur.execute("""DROP TABLE fact_sales""")
cur.execute("""CREATE TABLE fact_sales(
            product_code TEXT,
            product_name TEXT,
            qc TEXT, 
            factory_code TEXT, 
            sales_date DATE,
            sales_code TEXT, 
            order_code TEXT, 
            sales_quantity REAL, 
            unit TEXT,
            package_sales_quantity REAL, 
            package_unit TEXT, 
            department TEXT, 
            salesman TEXT,
            warehouse_code TEXT, 
            warehouse_type TEXT, 
            import_code TEXT,
            factory_order_code TEXT,
            import_timestamp TIMESTAMP,
            import_wh_timestamp TIMESTAMP
            )""")
conn.commit()
# Fact ######################################################################
# Fact ######################################################################
conn.close()


# Aggregate table
cur.execute("""DROP TABLE revenue_by_month""")
cur.execute("""CREATE TABLE revenue_by_month(
                factory_code TEXT,
                year INT,
                month INT,
                revenue float,
                revenue_predict float
                )""")

df_revenue = pd.read_excel(r'')