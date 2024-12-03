import pandas as pd
import numpy as np
import psycopg2
import os
import warnings
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

from utils import insert_data


database_name = os.getenv('STAGING_NAME')
database_user = os.getenv('STAGING_USER')
database_password = os.getenv('STAGING_PASSWORD')
database_host = os.getenv('STAGING_HOST')
database_port = os.getenv('STAGING_PORT')

conn = psycopg2.connect(database=database_name, user=database_user, password=database_password
                            , host=database_host, port=database_port)
cur = conn.cursor()



def read_excel_files(folder_path):
    # List to store DataFrames
    dataframes = []

    # Iterate through files in the specified folder
    for file in os.listdir(folder_path):
        # Check if the file is an Excel file (.xlsx)
        if file.endswith(".xlsx"):
            file_path = os.path.join(folder_path, file)
            
            # Suppress the specific warning and read the Excel file
            with warnings.catch_warnings(record=True):
                warnings.simplefilter("ignore")
                df = pd.read_excel(file_path, engine="openpyxl")
                dataframes.append(df)
    
    # Concatenate all DataFrames
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    return combined_df

# COPR13 ######################################################################
# COPR13 ######################################################################
cur.execute("""DROP TABLE COPR13""")
cur.execute("""CREATE TABLE COPR13(
            order_date DATE,
            order_code TEXT PRIMARY KEY,
            ct_date DATE,
            factory_code TEXT,
            factory_name TEXT,
            factory_order_code TEXT,
            currency TEXT,
            exchange_rate REAL,
            tax_type TEXT,
            channel TEXT,
            type TEXT,
            area TEXT,
            nation TEXT,
            path TEXT,
            path_2 TEXT,
            department TEXT,
            salesman TEXT,
            export_factory TEXT,
            register_price TEXT,
            note TEXT,
            deposit TEXT,
            deposit_rate TEXT,
            payment_registration_code TEXT,
            payment_registration_name TEXT,
            register_transaction TEXT,
            delivery_address TEXT,
            delivery_address_2 TEXT,
            volumn_unit TEXT,
            money_order REAL,
            tax REAL,
            total_quantity REAL,
            gw REAL,
            total_volumn REAL,
            total_package REAL,
            numerical_order TEXT,
            product_code TEXT,
            product_name TEXT,
            qc TEXT,
            factory_product_code TEXT,
            warehouse_type TEXT,
            predict_code TEXT,
            factory_product_name TEXT,
            factory_qc TEXT,
            order_quantity REAL,
            delivered_quantity REAL,
            package_order_quantity REAL,
            delivered_package_order_quantity REAL,
            gift_quantity REAL,
            delivered_gift_quantity REAL,
            package_gift_quantity REAL,
            delivered_package_gift_quantity REAL,
            reserve_quantity REAL,
            delivered_reserve_quantity REAL,
            package_reserve_quantity REAL,
            delivered_package_reserve_quantity REAL,
            temporary_export_quantity REAL,
            package_temporary_export_quantity REAL,
            unit TEXT,
            small_unit TEXT,
            package_unit TEXT,
            price REAL,
            money REAL,
            priced_quantity REAL,
            estimated_delivery_date DATE,
            original_estimated_delivery_date DATE,
            priced_unit TEXT,
            pre_ct TEXT,
            note_1 TEXT,
            finish_code TEXT,
            package_pt TEXT,
            package_name TEXT,
            weight_with_package REAL,
            volumn_with_package REAL,
            project_code TEXT,
            project_name TEXT,

            import_timestamp TIMESTAMP
            )""")
conn.commit()


# copr13_path = r"D:\VL1251\data\order\backup"
# df_copr13_1 = read_excel_files(copr13_path)
# df_copr13 = df_copr13_1.copy()
df_copr13 = pd.read_excel(r"D:\VL1251\data\copr13_backup_26112024.xlsx")
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
                    'project_code','project_name','import_timestamp']

df_copr13.dropna(subset=['order_code','numerical_order'], inplace=True) # Drop unaccepted null col

# Convert date cols to yyyy-mm-dd (ISO 8601 standard)
df_copr13_date_cols = ['order_date','ct_date','estimated_delivery_date','original_estimated_delivery_date']
for col in df_copr13_date_cols:
    df_copr13[col] = pd.to_datetime(df_copr13[col], dayfirst=True)

# Convert numerical_order (STT) to 4 digits with 4 zero-leadings
# df_copr13['numerical_order'] = df_copr13['numerical_order'].astype(int)
# df_copr13['numerical_order'] = df_copr13['numerical_order'].apply(lambda x: f"{int(float(x)):04}")

# Concat numerical order with order code to create new UNIQUE order code
# df_copr13['order_code'] = df_copr13['order_code'] + "-" + df_copr13['numerical_order']

df_copr13 = df_copr13.replace(np.nan, None)
for col in df_copr13_date_cols:
    df_copr13[col] = df_copr13[col].astype(object).where(df_copr13[col].notnull(), None)
df_copr13['import_timestamp'] = datetime.now()

# Insert data into database
insert_data(df_copr13, 'copr13', conn)
# COPR13 ######################################################################
# COPR13 ######################################################################

# COPR23 ######################################################################
# COPR23 ######################################################################
cur.execute("""DROP TABLE copr23""")
cur.execute("""CREATE TABLE copr23(
            product_code TEXT,
            product_name TEXT,
            qc TEXT,
            factory_code TEXT,
            factory_name TEXT,
            sales_date DATE,
            sales_code TEXT PRIMARY KEY,
            order_code TEXT,
            sales_quantity REAL,
            gift_quantity REAL,
            unit TEXT,
            small_unit TEXT,
            package_sales_quantity REAL,
            package_gift_quantity REAL,
            package_unit TEXT,
            priced_quantity REAL,
            priced_unit TEXT,
            currency TEXT,
            exchange_rate REAL,
            price REAL,
            unpaid_tw REAL,
            tax_tw REAL,
            unpaid_vn REAL,
            tax_vn REAL,
            capital REAL,
            gross_profit REAL,
            gross_profit_rate REAL,
            lot_code TEXT,
            tax_type TEXT,
            department TEXT,
            salesman TEXT,
            export_factory_code TEXT,
            export_factory TEXT,
            warehouse_code TEXT,
            warehouse_type TEXT,
            warehouse_loc TEXT,
            import_code TEXT,
            note TEXT,
            factory_order_code TEXT,

            import_timestamp TIMESTAMP
            )""")
conn.commit()
# copr23_path = r"D:\VL1251\data\sales\backup"
# df_copr23_1 = read_excel_files(copr23_path)
# df_copr23 = df_copr23_1.copy()
df_copr23 = pd.read_excel(r"D:\VL1251\data\copr23_backup_26112024.xlsx")
df_copr23.columns = ['product_code','product_name','qc','factory_code','factory_name',
                     'sales_date','sales_code','order_code','sales_quantity',
                     'gift_quantity','unit','small_unit','package_sales_quantity',
                     'package_gift_quantity','package_unit','priced_quantity',
                     'priced_unit','currency','exchange_rate',
                     'price','unpaid_tw','tax_tw','unpaid_vn','tax_vn',
                     'capital','gross_profit','gross_profit_rate','lot_code',
                     'tax_type','department','salesman','export_factory_code','export_factory',
                     'warehouse_code','warehouse_type','warehouse_loc','import_code','note',
                     'factory_order_code', 'import_timestamp']

df_copr23.dropna(subset=['sales_code'], inplace=True) # Drop unaccepted null col

# Convert date cols to yyyy-mm-dd (ISO 8601 standard)
df_copr23_date_cols = ['sales_date']
for col in df_copr23_date_cols:
    df_copr23[col] = pd.to_datetime(df_copr23[col], dayfirst=True)


df_copr23 = df_copr23.replace(np.nan, None)
for col in df_copr23_date_cols:
    df_copr23[col] = df_copr23[col].astype(object).where(df_copr23[col].notnull(), None)

df_copr23['import_timestamp'] = datetime.now()
insert_data(df_copr23, 'copr23', conn)
# COPR23 ######################################################################
# COPR23 ######################################################################

# MOCAR03 ######################################################################
# MOCAR03 ######################################################################


# MOCAR03 ######################################################################
# MOCAR03 ######################################################################
