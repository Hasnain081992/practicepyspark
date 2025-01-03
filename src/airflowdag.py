from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow_practice import process_data

#crating arguments

default_args = {
    'owner':'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025,1,3),
    'email':['cshasnain23@gmail.com'],
    'email_on_failure': False,
    'email_on_retry' :False,
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}

#creating dag

dag = DAG(
    'airflowdag',
    default_args=default_args,
    description = 'My first airflow code'
)

run_etl = PythonOperator(
    task_id='complete_basic_airflow',
    python_collable=process_data,
    dag =dag,
)

run_etl

