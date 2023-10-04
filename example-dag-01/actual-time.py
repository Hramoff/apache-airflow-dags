from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from time_writer import write_time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'write_time_dag',
    default_args=default_args,
    description='Writes the current time to a file every 2 minutes',
    schedule_interval=timedelta(minutes=2),
)

write_time_task = PythonOperator(
    task_id='write_time_task',
    python_callable=write_time,
    dag=dag,
)

write_time_task