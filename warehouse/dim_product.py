import pandas as pd
import numpy as np
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

# Connect to Warehouse
database_name = os.getenv('WAREHOUSE_NAME')
database_user = os.getenv('WAREHOUSE_USER')
database_password = os.getenv('WAREHOUSE_PASSWORD')
database_host = os.getenv('WAREHOUSE_HOST')
database_port = os.getenv('WAREHOUSE_PORT')

conn = psycopg2.connect(database=database_name, user=database_user, password=database_password
                        ,host=database_host, port=database_port)
cur = conn.cursor()

cur.execute("""SELECT DISTINCT product_code, product_name
            FROM fact_sales""")
data = cur.fetchall()
column_names = [description[0] for description in cur.description]
df_product = pd.DataFrame(data = data, columns = column_names)


# Upsert query
upsert_query = """
INSERT INTO dim_product (product_code, product_name)
VALUES (%s, %s)
ON CONFLICT (product_code) 
DO UPDATE SET 
    product_name = EXCLUDED.product_name;
"""

# Execute the upsert for each row in data
for _, row in df_product.iterrows():  # Iterate over rows as (index, Series)
    cur.execute(upsert_query, (row["product_code"], row["product_name"]))
# Commit changes
conn.commit()