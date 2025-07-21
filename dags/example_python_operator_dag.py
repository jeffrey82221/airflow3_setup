from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator

# Define the function that the PythonOperator will execute
def greet(name):
    print(f"Hello, {name}! This is a PythonOperator task.")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Instantiate the DAG
with DAG(
    'example_python_operator_dag',
    default_args=default_args,
    description='A simple DAG that demonstrates PythonOperator',
    schedule='@daily',
    catchup=False
) as dag:

    # Define the task using PythonOperator
    hello_task = PythonOperator(
        task_id='greet_user',
        python_callable=greet,
        op_kwargs={'name': 'World'}  # Pass arguments to the function
    )

    # You can chain multiple tasks here, for example:
    # another_task = PythonOperator(...)
    # hello_task >> another_task

    # For a single task, no need to specify dependencies
