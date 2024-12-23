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
            print(f"New Excel file detected: {event.src_path}")
            print(datetime.today())
            file_path = event.src_path
            print(file_path)
            print(type(file_path))
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
            
            df_copr13 = pd.read_excel(file_path)
            try:
                df_copr13.columns = ['order_date','ct_date','original_estimated_delivery_date','estimated_delivery_date',
                                     'order_code','factory_code','factory_name','product_code',
                                     'product_name','qc','order_quantity','delivered_quantity',
                                     'factory_order_code','note','numerical_order','path','warehouse_type']
            except:
                print("Number of columns does not match!")
            else:
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

                copr13_columns = ['order_date','order_code','ct_date','factory_code','factory_name',
                                'factory_order_code','currency','exchange_rate','tax_type',
                                'channel','type','area','nation','path','path_2','department',
                                'salesman','export_factory','register_price','note','deposit',
                                'deposit_rate','payment_registration_code',
                                'payment_registration_name','register_transaction','delivery_address',
                                'delivery_address_2','volumn_unit','money_order','tax',
                                'total_quantity','gw','total_volumn','total_package',
                                'numerical_order','product_code','product_name','qc',
                                'factory_product_code','warehouse_type','predict_code',
                                'factory_product_name','factory_qc','order_quantity',
                                'delivered_quantity','package_order_quantity',
                                'delivered_package_order_quantity','gift_quantity',
                                'delivered_gift_quantity','package_gift_quantity',
                                'delivered_package_gift_quantity','reserve_quantity',
                                'delivered_reserve_quantity','package_reserve_quantity',
                                'delivered_package_reserve_quantity','temporary_export_quantity',
                                'package_temporary_export_quantity','unit','small_unit',
                                'package_unit','price','money','priced_quantity',
                                'estimated_delivery_date','original_estimated_delivery_date',
                                'priced_unit','pre_ct','note_1','finish_code','package_pt',
                                'package_name','weight_with_package','volumn_with_package',
                                'project_code','project_name','import_timestamp']

                df_copr13.shape
                for col in copr13_columns:
                    if col not in df_copr13.columns:
                        df_copr13[col] = None

                df_copr13 = df_copr13[copr13_columns]
                

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
                time.sleep(5)
                shutil.move(file_path, r"D:\VL1251\ETL\old_data\wh\order")
                print("File moved to old folder")
                time.sleep(5)
                
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
                            tax_type, department, salesman, deposit_rate, payment_registration_code, payment_registration_name,
                            delivery_address, product_code, product_name, qc, warehouse_type, order_quantity, delivered_quantity,
                            package_order_quantity, delivered_package_order_quantity, unit, package_unit, estimated_delivery_date,
                            original_estimated_delivery_date, pre_ct, finish_code, import_timestamp
                            FROM copr13
                            WHERE import_timestamp > %(latest_import)s
                                """,{'latest_import':latest_import})

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
            

if __name__ == "__main__":
    folder_to_monitor = r"D:\VL1251\dashboard\data\wh\order" # Replace with your folder path

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
