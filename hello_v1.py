# your_dag_file.py

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
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

hello_task = BashOperator(
    task_id='hello_task',
    bash_command='bash /opt/bitnami/airflow/dags/brompton/hello.sh',  # Replace with the correct path
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()
