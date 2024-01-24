# main_dag.py
from brompton.tasks import *
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define default_args dictionary to specify the DAG's default parameters
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG instance
dag = DAG(
    'my_dag',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval=timedelta(days=1),
)

# Define tasks in the DAG using PythonOperator
task_1 = PythonOperator(
    task_id='task_one',
    python_callable=task_one,
    dag=dag,
)

task_2 = PythonOperator(
    task_id='task_two',
    python_callable=task_two,
    dag=dag,
)

task_3 = PythonOperator(
    task_id='task_three',
    python_callable=task_three,
    dag=dag,
)

# Define the task execution sequence
task_1 >> task_2
task_1 >> task_3
