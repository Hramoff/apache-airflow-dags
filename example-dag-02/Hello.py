from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('hello_airflow', default_args=default_args, schedule_interval=timedelta(minutes=5))

def print_hello():
    return 'Hello, Airflow!'

run_this = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag
)

run_this