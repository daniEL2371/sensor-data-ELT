from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from airflow.operators.mysql_operator import MySqlOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator

import mysql.connector as mysql
from sqlalchemy import create_engine, types

mysql_user = 'root'
mysql_password = 'pssd'
mysql_host = 'mysql-dbt'
mysql_db_name = 'analytics'
mysql_port = 3306

def connect_to_mysql():

    print("yee")
    connection = f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db_name}'
    engine = create_engine(connection)
    print("hhe")
  

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


connect_to_mysql = PythonOperator(
    task_id='connect_to_mysql',
    python_callable=connect_to_mysql,
    dag=dag
)


