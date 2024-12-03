import pandas as pd
import numpy as np
import psycopg2
import os
from psycopg2 import OperationalError
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

database_name = os.getenv('STAGING_NAME')
database_user = os.getenv('STAGING_USER')
database_password = os.getenv('STAGING_PASSWORD')
database_host = os.getenv('STAGING_HOST')
database_port = os.getenv('STAGING_PORT')

conn = psycopg2.connect(database=database_name, user=database_user, password=database_password
                            , host=database_host, port=database_port)
cur = conn.cursor()
###################################################################################
###################################################################################
df_copr13 = pd.read_excel(r"order_daily\COPR1320241121000192202411210001.xlsx")

df_copr13.columns = ['order_date','order_code','ct_date','factory_code','factory_name',
                    'factory_order_code','currency','exchange_rate','tax_type',
                    'channel','type','area','nation','path','path_2','department',
                    'salesman','export_factory', 'register_price', 'note', 'deposit',
                    'deposit_rate', 'payment_registration_code', 'payment_registration_name',
                    'register_transaction','delivery_address', 'delivery_address_2',
                    'volumn_unit', 'money_order','tax', 'total_quantity', 'gw',
                    'total_volumn', 'total_package','numerical_order',
                    'product_code', 'product_name', 'qc', 'factory_product_code', 
                    'warehouse_type', 'predict_code','factory_product_name', 
                    'factory_qc', 'order_quantity', 'delivered_quantity',
                    'package_order_quantity', 'delivered_package_order_quantity', 
                    'gift_quantity','delivered_gift_quantity', 'package_gift_quantity',
                    'delivered_package_gift_quantity', 'reserve_quantity',
                    'delivered_reserve_quantity', 'package_reserve_quantity',
                    'delivered_package_reserve_quantity', 'temporary_export_quantity',
                    'package_temporary_export_quantity', 'unit', 'small_unit',
                    'package_unit', 'price', 'money', 'priced_quantity',
                    'estimated_delivery_date', 'original_estimated_delivery_date',
                    'priced_unit', 'pre_ct', 'note_1', 'finish_code', 'package_pt',
                    'package_name', 'weight_with_package', 'volumn_with_package',
                    'project_code','project_name']

df_copr13.dropna(subset=['order_code','numerical_order'], inplace=True) # Drop unaccepted null col

# Convert date cols to yyyy-mm-dd (ISO 8601 standard)
df_copr13_date_cols = ['order_date','ct_date','estimated_delivery_date','original_estimated_delivery_date']
for col in df_copr13_date_cols:
    df_copr13[col] = pd.to_datetime(df_copr13[col], dayfirst=True)

# Convert numerical_order (STT) to 4 digits with 4 zero-leadings
df_copr13['numerical_order'] = df_copr13['numerical_order'].astype(int)
df_copr13['numerical_order'] = df_copr13['numerical_order'].apply(lambda x: f"{int(float(x)):04}")

# Concat numerical order with order code to create new UNIQUE order code
df_copr13['order_code'] = df_copr13['order_code'] + "-" + df_copr13['numerical_order']

df_copr13 = df_copr13.replace(np.nan, None)
for col in df_copr13_date_cols:
    df_copr13[col] = df_copr13[col].astype(object).where(df_copr13[col].notnull(), None)
df_copr13['import_timestamp'] = datetime.now()

df_copr13.shape


successful_inserts = []
conflict_rows = []

# Define insert with upsert statement
insert_query = """
    INSERT INTO copr13 (
    order_date, order_code, ct_date, factory_code, factory_name,
    factory_order_code, currency, exchange_rate, tax_type,
    channel, type, area, nation, path, path_2, department,
    salesman, export_factory, register_price, note, deposit,
    deposit_rate, payment_registration_code,
    payment_registration_name, register_transaction, delivery_address,
    delivery_address_2, volumn_unit, money_order, tax,
    total_quantity, gw, total_volumn, total_package,
    numerical_order, product_code, product_name, qc,
    factory_product_code, warehouse_type, predict_code,
    factory_product_name, factory_qc, order_quantity,
    delivered_quantity, package_order_quantity,
    delivered_package_order_quantity, gift_quantity,
    delivered_gift_quantity, package_gift_quantity,
    delivered_package_gift_quantity, reserve_quantity,
    delivered_reserve_quantity, package_reserve_quantity,
    delivered_package_reserve_quantity, temporary_export_quantity,
    package_temporary_export_quantity, unit, small_unit,
    package_unit, price, money, priced_quantity,
    estimated_delivery_date, original_estimated_delivery_date,
    priced_unit, pre_ct, note_1, finish_code, package_pt,
    package_name, weight_with_package, volumn_with_package,
    project_code, project_name, import_timestamp
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (order_code) DO UPDATE SET
        order_quantity = EXCLUDED.order_quantity,
        import_timestamp = EXCLUDED.import_timestamp;
"""

# Insert rows one by one
try:
    for _, row in df_copr13.iterrows():
        values = tuple(row)
        try:
            cur.execute(insert_query, values)
            successful_inserts.append(row)  # Append successfully inserted rows
        except OperationalError as e:
            print(f"Conflict or error for row with order_code {row['order_code']}: {e}")
            conflict_rows.append(row)  # Store conflict rows

except Exception as e:
    print(f"Database operation failed: {e}")
conn.commit()
# Convert conflict rows to DataFrame
conflict_df = pd.DataFrame(conflict_rows)
df_successful_inserts = pd.DataFrame(successful_inserts)
# Output results
print("Successfully inserted rows:", df_successful_inserts.shape)
print("Rows with conflicts:", conflict_df.shape)



###########################################################################
###########################################################################
# Copr23
df_copr23 = pd.read_excel(r"D:\VL1251\ETL\sales_daily\20241118~19 GIAO HANG.xlsx")
df_copr23.columns = ['sales_date', 'ct_date', 'sales_code', 'factory_code',
       'factory_name', 'salesman', 'product_code', 'product_name', 'qc', 'warehouse_code', 'sales_quantity',
       'package_sales_quantity', 'order_code', 'import_code', 'note', 'factory_order_code']

df_copr23.dropna(subset=['sales_code'], inplace=True) # Drop unaccepted null col


# Convert date cols to yyyy-mm-dd (ISO 8601 standard)
df_copr23_date_cols = ['sales_date']
for col in df_copr23_date_cols:
    df_copr23[col] = pd.to_datetime(df_copr23[col], dayfirst=True)



df_copr23['factory_code'] = df_copr23['factory_code'].astype('string')
df_copr23['factory_code'] = df_copr23['factory_code'].str.replace(r'.0','',regex=False)


df_copr23["numerical_order"] = (
    df_copr23.groupby("sales_code").cumcount() + 1
).astype(str).str.zfill(4)  # Generate 0001, 0002, etc.

df_copr23["sales_code"] = df_copr23["sales_code"] + "  -" + df_copr23["numerical_order"]


df_copr23 = df_copr23.replace(np.nan, None)
for col in df_copr23_date_cols:
    df_copr23[col] = df_copr23[col].astype(object).where(df_copr23[col].notnull(), None)

df_copr23['import_timestamp'] = datetime.now()

df_copr23.shape
df_copr23.columns

copr23_columns = ['product_code',
            'product_name',
            'qc',
            'factory_code' ,
            'factory_name' ,
            'sales_date' ,
            'sales_code' ,
            'order_code' ,
            'sales_quantity' ,
            'gift_quantity' ,
            'unit' ,
            'small_unit' ,
            'package_sales_quantity' ,
            'package_gift_quantity' ,
            'package_unit' ,
            'priced_quantity' ,
            'priced_unit',
            'currency' ,
            'exchange_rate',
            'price' ,
            'unpaid_tw' ,
            'tax_tw' ,
            'unpaid_vn' ,
            'tax_vn',
            'capital' ,
            'gross_profit' ,
            'gross_profit_rate',
            'lot_code' ,
            'tax_type' ,
            'department' ,
            'salesman' ,
            'export_factory_code' ,
            'export_factory' ,
            'warehouse_code' ,
            'warehouse_type' ,
            'warehouse_loc' ,
            'import_code' ,
            'note' ,
            'factory_order_code',

            'import_timestamp']

df_copr23.shape
for col in copr23_columns:
    if col not in df_copr23.columns:
        df_copr23[col] = None

df_copr23 = df_copr23[copr23_columns]

df_copr23.shape
df_copr23.columns


successful_inserts = []
conflict_rows = []

# Define insert with upsert statement
insert_query = """
    INSERT INTO copr23 (
    product_code, product_name,qc,factory_code ,factory_name ,sales_date ,sales_code ,
    order_code ,sales_quantity ,gift_quantity ,unit ,small_unit,package_sales_quantity ,
    package_gift_quantity ,package_unit ,priced_quantity ,priced_unit,currency ,exchange_rate,
    price ,unpaid_tw ,tax_tw ,unpaid_vn ,tax_vn,capital ,gross_profit ,gross_profit_rate,
    lot_code ,tax_type ,department ,salesman ,export_factory_code ,export_factory ,warehouse_code ,
    warehouse_type ,warehouse_loc ,import_code ,note ,factory_order_code,

    import_timestamp
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (sales_code) DO NOTHING;
"""

# Insert rows one by one
try:
    for _, row in df_copr23.iterrows():
        values = tuple(row)
        try:
            cur.execute(insert_query, values)
            successful_inserts.append(row)  # Append successfully inserted rows
        except OperationalError as e:
            print(f"Conflict or error for row with order_code {row['order_code']}: {e}")
            conflict_rows.append(row)  # Store conflict rows

except Exception as e:
    print(f"Database operation failed: {e}")
conn.commit()
# Convert conflict rows to DataFrame
conflict_df = pd.DataFrame(conflict_rows)
df_successful_inserts = pd.DataFrame(successful_inserts)
# Output results
print("Successfully inserted rows:", df_successful_inserts.shape)
print("Rows with conflicts:", conflict_df.shape)