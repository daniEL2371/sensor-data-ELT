from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from airflow.operators.mysql_operator import MySqlOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator

import mysql.connector as mysql
from sqlalchemy import create_engine, types, text
import json

mysql_user = 'root'
mysql_password = 'pssd'
mysql_host = 'mysql-dbt'
mysql_db_name = 'analytics'
mysql_port = 3306

selec_batch_size = 100000

def create_mysql_connection():

    connection = f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db_name}'
    engine = create_engine(connection)
    return engine

def get_record_count(table_name):
   engine = create_mysql_connection()
   conn = engine.connect()
   query = f'SELECT COUNT(*) FROM {table_name}'
   result = conn.execute(query)
   return result.fetchone()[0]





def select_src_data(**kwargs):
    table_name = kwargs['table_name']

    engine = create_mysql_connection()
    conn = engine.connect()
    row_count = get_record_count(f'{table_name}')

    cur = 0
    while cur < row_count :
        query = text(f'select * from {table_name} Limit {cur}, {selec_batch_size}')
        result = conn.execute(query)
        json_res  = json.dumps([dict(r) for r in result])
        cur += selec_batch_size

        # todo move the data to postgres
    print("select statment finished")

default_args = {
    'owner': '10Academy',
    'depends_on_past': False,
    'start_date': datetime(2021, 9, 23),
    'email': ['danielzelalemheru@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'data_migration_to_postgres',
    default_args=default_args,
    description='A data migration dag from mysql to python',
    schedule_interval='@once',
    # schedule_interval='*/ * * * *'
)


migrate_station_summary = PythonOperator(
    task_id='migrate_station_summary',
    python_callable=select_src_data,
    op_kwargs={'table_name': 'analytics.Station_Summary' },
    dag=dag
)

migrate_I80Stations= PythonOperator(
    task_id='migrate_I80Stations',
    python_callable=select_src_data,
    op_kwargs={'table_name': 'analytics.I80Stations' },
    dag=dag
)

migrate_merged_station_summary = PythonOperator(
    task_id='migrate_merged_station_summary',
    python_callable=select_src_data,
    op_kwargs={'table_name': 'analytics.merged_station_summary	' },
    dag=dag
)

migrate__raw_observations = PythonOperator(
    task_id='migrate__raw_observations',
    python_callable=select_src_data,
    op_kwargs={'table_name': 'analytics.raw_observations	' },
    dag=dag
)


