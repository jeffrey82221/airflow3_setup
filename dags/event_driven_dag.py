import json

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.providers.apache.kafka.triggers.msg_queue import KafkaMessageQueueTrigger
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, AssetWatcher, DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


# Define the Kafka queue URL
# Replace <your_kafka_host>, <port>, and <your_topic> with your Kafka


# Define a function to apply when a message is received
# This function will be called with the message as an argument
def apply_function(*args, **kwargs):
    message = args[-1]
    val = json.loads(message.value())
    print(f"Value in message is {val}")
    return val

trigger = MessageQueueTrigger(
    queue='kafka://localhost:9092/my_topic',
    apply_function="event_driven_dag.apply_function",
    apply_function_args=None,
    apply_function_kwargs=None,
    poll_timeout=1,
    poll_interval=5,
)

asset = Asset("kafka_topic_asset", watchers=[AssetWatcher(name="kafka_watcher_1", trigger=trigger)])

def greet(name):
    print(f"Hello, {name}! This is a PythonOperator task.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
# Instantiate the DAG
with DAG(
    'event_driven_dag',
    default_args=default_args,
    description='A simple DAG that demonstrates PythonOperator',
    schedule=[asset],
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
