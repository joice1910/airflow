from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
import requests
import json
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from time import gmtime, strftime
from airflow.models import Variable
import time
from airflow.utils.trigger_rule import TriggerRule




default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 2, 27),
}

with DAG(
        'multi_trigger_dag',
        schedule_interval="*/10 * * * *",
        concurrency=1, max_active_runs=1,
        default_args=default_args,
        catchup=False
) as dag:


    Start = DummyOperator(
        task_id="Start",
        dag=dag
    )

    trigger_dag1 = TriggerDagRunOperator(
        task_id='execute_etl1',
        trigger_dag_id='sample_dag1',
        wait_for_completion=True,
        trigger_rule=TriggerRule.ALL_DONE
    )

    trigger_dag2 = TriggerDagRunOperator(
        task_id='execute_etl2',
        trigger_dag_id='sample_dag2',
        wait_for_completion=True,
        trigger_rule=TriggerRule.ALL_DONE
    )

    trigger_dag3 = TriggerDagRunOperator(
        task_id='execute_etl3',
        trigger_dag_id='sample_dag3',
        wait_for_completion=True,
        trigger_rule=TriggerRule.ALL_DONE
    )


    End = DummyOperator(
        task_id="End",
        dag=dag
    )

    Start >> trigger_dag1 >> trigger_dag2 >> trigger_dag3 >> End