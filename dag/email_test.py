from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.task_group import TaskGroup
import requests
import json
import os
from datetime import datetime, timedelta
from time import gmtime, strftime
from pendulum import duration


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2021, 8, 23),
    "email": ["arjun.sivakumar@6dtech.co.in","joice.jacob@6dtech.co.in"],
    "email_on_failure": True,
}

with DAG(
        'email_alert',
        schedule_interval=None,
        concurrency=1, max_active_runs=1,
        default_args=default_args
) as dag:


    def initial(**context):

        fileappend = datetime.now().strftime('%Y%m%d%H%M%S')
        context['task_instance'].xcom_push(key="fileappend", value=fileappend)

    def finalnameconvension(filename,fileappender):

        filename = '/log/btc_autopilot/etl_files/' + filename + '_' + fileappender + '.csv'
        print(filename)
        return filename


    def check_file_existence():
        raise ValueError("ERROR")

    Start = PythonOperator(
        task_id='Start',
        python_callable=initial,
        provide_context=True
    )

    email = EmailOperator(
        task_id='send_email',
        to=['arjun.sivakumar@6dtech.co.in','joice.jacob@6dtech.co.in'],
        subject='Airflow Alert',
        html_content=""" <h3>Email Test</h3> """,
        dag=dag
    )

    end = DummyOperator(
        task_id="End",
        dag=dag
    )

    Start >> email >> end