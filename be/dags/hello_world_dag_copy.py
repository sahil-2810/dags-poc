from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
dag = DAG(
    'hello_world_dag_COPY',
    default_args=default_args,
    # schedule_interval="*/2 * * * *",
    description='A simple DAG with a Hello World task',
    schedule_interval=timedelta(minutes=10)  # Set to run every 10 minutes
)

# Define a Python function to run as a task
def hello_world():
    print("Hello, World!")

# Instantiate a PythonOperator
hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=hello_world,
    dag=dag,
)

# Set up the task dependencies
hello_task  # This DAG only has one task, so no dependencies need to be set
