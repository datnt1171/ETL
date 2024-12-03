import psycopg2
import os

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

cur.execute("""INSERT INTO users(username, password, role) VALUES ('dat', '123', 'admin')""")
cur.execute("""INSERT INTO users(username, password, role) VALUES ('stanley', '123', 'admin')""")
cur.execute("""INSERT INTO users(username, password, role) VALUES ('dungtq', '123', 'admin')""")
cur.execute("""INSERT INTO users(username, password, role) VALUES ('KHOBA', '123', 'warehouse')""")
cur.execute("""INSERT INTO users(username, password, role) VALUES ('color_mixing', '123', 'color_mixing')""")
cur.execute("""INSERT INTO users(username, password, role) VALUES ('sales', '123', 'sales')""")
cur.execute("""INSERT INTO users(username, password, role) VALUES ('production', '123', 'production')""")
cur.execute("""INSERT INTO users(username, password, role) VALUES ('rd', '123', 'rd')""")

conn.commit()