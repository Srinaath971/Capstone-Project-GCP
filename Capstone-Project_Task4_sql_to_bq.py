import psycopg2
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import db_dtypes
from pandas.io import gbq

conn=psycopg2.connect(database='postgres',user='postgres',password='onepiece987',host='35.229.79.72',port='5432')
cursor=conn.cursor()
conn.autocommit=True

credentials = service_account.Credentials.from_service_account_file('/home/airflow/gcs/data/googlekey.json')
client = bigquery.Client(credentials=credentials)
df = pd.read_sql('''select customer_id,name,row_number() over(order by customer_id asc) as address_id,updated_timestamp as start_date,cast('2100-01-01 23:59:59' as timestamp) as end_date from customer_master;''',conn)
#df.head(5)
df.to_gbq(destination_table='capstone_bq.dim_customer',project_id='capstone-6-352803',if_exists='append')