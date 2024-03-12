from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import time

# Define the Python function to be executed as a task
def wait_for_five_minutes():
    print("Waiting for 5 minutes...")
    time.sleep(300)  # Sleep for 5 minutes (300 seconds)
    print("Finished waiting!")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Instantiate the DAG object
dag = DAG(
    'sample_dag3',
    default_args=default_args,
    description='A DAG to demonstrate waiting for 5 minutes',
    schedule_interval=None,
)

# Define the PythonOperator to execute the function
wait_task = PythonOperator(
    task_id='wait_for_five_minutes',
    python_callable=wait_for_five_minutes,
    dag=dag,
)

# Set task dependencies
wait_task
