from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
def my_producer_function():
    # producer: confluent_kafka.Producer instance
    # context: Airflow context, optional (for dynamic message generation)
    
    return (('key-hello', 'value-hello'),)
# Instantiate the DAG
with DAG(
    'producer_dag',
    default_args=default_args,
    description='A simple DAG that demonstrates PythonOperator',
    schedule='@daily',
    catchup=False
) as dag:
    # Define the task using PythonOperator
    produce_to_kafka = ProduceToTopicOperator(
        task_id="produce_message",
        topic="my_topic",            # Replace with your target topic
        # message="{'key': 'value'}",       # Replace with your message (str, JSON, etc.)
        producer_function=my_producer_function,
        kafka_config_id="kafka_default",  # Uses your Kafka connection
        dag=dag,
    )