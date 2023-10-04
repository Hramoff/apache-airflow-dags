from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'write_time_to_file',
    default_args=default_args,
    description='Writes current time to file every 2 minutes',
    schedule_interval=timedelta(minutes=2),
)

def write_time():
    with open('/tmp/time.txt', 'a') as f:
        f.write(str(datetime.now()) + '\n')

write_time_task = PythonOperator(
    task_id='write_time',
    python_callable=write_time,
    dag=dag,
)

write_time_task