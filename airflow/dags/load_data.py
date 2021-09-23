from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta

default_args = {
    'owner': 'astronomer',
    'depends_on_past': False,
    'start_date': datetime(2021, 9, 23),
    'email': ['noreply@astronomer.io'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(
    'data_load_dag',
    default_args=default_args,
    description='An Airflow DAG to invoke simple dbt commands',
    # schedule_interval=timedelta(days=1),
    schedule_interval='*/1 * * * *'
)

dbt_run = BashOperator(
    task_id='data_load',
    bash_command='ls',
    dag=dag
)

