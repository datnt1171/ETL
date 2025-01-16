import os
import time
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import shutil
import pandas as pd
import numpy as np
import psycopg2
import os
from psycopg2 import OperationalError
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
from utils import insert_data

class ExcelFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        # Check if the added file is an Excel file
        if not event.is_directory and event.src_path.endswith(('.xls', '.xlsx')):
            
            print(f"New Sales file detected: {event.src_path}")
            print(datetime.today())
            file_path = event.src_path
            print(file_path)
            file_name = os.path.basename(file_path)
            print(type(file_path))
            
            database_name = os.getenv('STAGING_NAME')
            database_user = os.getenv('STAGING_USER')
            database_password = os.getenv('STAGING_PASSWORD')
            database_host = os.getenv('STAGING_HOST')
            database_port = os.getenv('STAGING_PORT')

            conn = psycopg2.connect(database=database_name, user=database_user, password=database_password
                                        , host=database_host, port=database_port)
            cur = conn.cursor()
            
            
            df_copr23 = pd.read_excel(file_path)
            try:
                df_copr23.columns = ['sales_date', 'ct_date', 'sales_code', 'factory_code',
                    'factory_name', 'salesman', 'product_code', 'product_name', 'qc', 'warehouse_code', 'sales_quantity',
                    'order_code', 'import_code', 'note', 'factory_order_code']
            except:
                print("Number of columns does not match!")
            else:
                df_copr23.dropna(subset=['sales_code'], inplace=True) # Drop unaccepted null col


                # Convert date cols to yyyy-mm-dd (ISO 8601 standard)
                df_copr23_date_cols = ['sales_date','ct_date']
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
                time.sleep(5)
                des_folder = r"D:\VL1251\ETL\old_data\wh\sales"
                shutil.move(file_path, os.path.join(des_folder, file_name))
                print("File moved to old folder")
                time.sleep(5)
                
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


                conn.commit()
                # Extract
                cur.execute("""SELECT product_code,product_name,qc,factory_code,
                            sales_date,sales_code,order_code,sales_quantity,
                            unit,package_sales_quantity,
                            package_unit,
                            department,salesman,
                            warehouse_code,warehouse_type, import_code, factory_order_code,
                            import_timestamp
                            FROM copr23
                            WHERE import_timestamp > %(latest_import)s
                                    """,{'latest_import':latest_import})

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
                
                df_KDT = df_copr23[df_copr23['factory_code']=='30895.2'][['sales_code','factory_code','factory_order_code']]
                df_KDT.fillna("temp", inplace=True)

                df_KDT.loc[df_KDT['factory_order_code'].str.contains('ST', case=False, na=False), 'factory_code'] = "30895.1"
                df_KDT.loc[df_KDT['factory_order_code'].str.contains('TN', case=False, na=False), 'factory_code'] = "30895"
                df_KDT.loc[df_KDT['factory_order_code'].str.contains('BP', case=False, na=False), 'factory_code'] = "30895.5"
                df_KDT.loc[df_KDT['factory_order_code'].str.contains('QT', case=False, na=False), 'factory_code'] = "30895.4"

                df_KDT.columns = ['sales_code','factory_code_fixed','factory_order_code']

                df_copr23 = df_copr23.merge(df_KDT[['sales_code','factory_code_fixed']], on='sales_code', how='left')
                # Replace values in df1's column with df2's column
                df_copr23['factory_code'] = df_copr23['factory_code_fixed'].combine_first(df_copr23['factory_code'])
                df_copr23.drop(columns=['factory_code_fixed'], inplace=True)
                
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
            

if __name__ == "__main__":
    folder_to_monitor = r"D:\VL1251\dashboard\data\wh\sales" # Replace with your folder path

    event_handler = ExcelFileHandler()
    observer = Observer()
    observer.schedule(event_handler, folder_to_monitor, recursive=False)
    
    print(f"Monitoring folder: {folder_to_monitor}")
    try:
        observer.start()
        while True:
            time.sleep(1)  # Keep the script running
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
