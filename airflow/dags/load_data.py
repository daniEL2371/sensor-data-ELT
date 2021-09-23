from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from airflow.operators.mysql_operator import MySqlOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta

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
    'data_load_dag',
    default_args=default_args,
    description='An Airflow DAG to invoke simple dbt commands',
    schedule_interval=timedelta(days=1),
    # schedule_interval='*/ * * * *'
)


create_table_mysql_task = MySqlOperator(
    task_id='create_table_mysql',
    mysql_conn_id='mysql_conn_id',
    sql=r"""CREATE TABLE  test_table (fname VARCHAR(20), lname VARCHAR(20));""",
    dag=dag
)

dbt_run = BashOperator(
    task_id='data_load',
    bash_command='echo helloworld',
    dag=dag
)

