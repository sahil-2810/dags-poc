# your_dag_file.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from be.brompton import print_hello
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
}

dag = DAG(
    'simple_hello_world_dag',
    default_args=default_args,
    description='A simple DAG printing Hello, World!',
    schedule_interval='@daily',
)

def hello_world_task(**kwargs):
    print_hello()

with dag:
    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=hello_world_task,
    )

if __name__ == "__main__":
    dag.cli()
