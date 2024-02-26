from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import time

# Define the Python function to be executed as a task
def wait_for_one_minutes():
    print("Waiting for 1 minutes...")
    time.sleep(60)  # Sleep for 1 minutes (300 seconds)
    print("Finished waiting!")
	
def wait_for_two_minutes():
    print("Waiting for 2 minutes...")
    time.sleep(120)  
    print("Finished waiting!")

def wait_for_three_minutes():
    print("Waiting for 3 minutes...")
    time.sleep(180)  
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
    'Multitask_dag',
    default_args=default_args,
    description='Multitask',
    schedule_interval=None,
)

# Define the PythonOperator to execute the function
wait_task_one = PythonOperator(
    task_id='wait_for_one_minutes',
    python_callable=wait_for_one_minutes,
    dag=dag,
)

wait_task_two = PythonOperator(
    task_id='wait_for_two_minutes',
    python_callable=wait_for_two_minutes,
    dag=dag,
)

wait_task_three = PythonOperator(
    task_id='wait_for_three_minutes',
    python_callable=wait_for_three_minutes,
    dag=dag,
)

# Set task dependencies
wait_task_one >> wait_task_two >> wait_task_three
