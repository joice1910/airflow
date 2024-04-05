from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import requests
import os
import json
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from time import gmtime, strftime
from airflow.models import Variable
#from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.sensors.sql import SqlSensor


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2021, 8, 23),
}

with DAG(
        'sql_sensor',
        schedule_interval=None,
        concurrency=1, max_active_runs=1,
        default_args=default_args
) as dag:

    def success_criteria(record):
        print("record success : "+record)
        return record


    def failure_criteria(record):
        print("record failed: " + record)
        return True if not record else False


    profile_sensor = SqlSensor(
        task_id='check_for_data_in_profile',
        conn_id="82_mysql",
        success=success_criteria,
        failure=failure_criteria,
        poke_interval=20,
        #mode="reschedule",
        timeout=60 * 5,
        sql="""
          SELECT completion_status FROM `Audit_Log_Fact` WHERE table_name='Profile_Cdr' and completion_status='Y';
        """,
        dag=dag)

    usage_sensor = SqlSensor(
        task_id='check_for_data_in_customer360_usage',
        conn_id="82_mysql",
        success=success_criteria,
        failure=failure_criteria,
        poke_interval=20,
        #mode="reschedule",
        timeout=60 * 5,
        sql="""
          SELECT completion_status FROM `Audit_Log_Fact` WHERE table_name='Customer_360_Usage' and completion_status='Y';
        """,
        dag=dag)


    recharge_sensor = SqlSensor(
        task_id='check_for_data_in_customer360_recharge',
        conn_id="82_mysql",
        success=success_criteria,
        failure=failure_criteria,
        poke_interval=20,
        #mode="reschedule",
        timeout=60 * 5,
        sql="""
          SELECT completion_status FROM `Audit_Log_Fact` WHERE table_name='Customer_360_Recharge' and completion_status='Y';
        """,
        dag=dag)


    Start = DummyOperator(
        task_id="Start",
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    fetch_profile = DummyOperator(
        task_id="fetch_profile",
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE
    )

    fetch_usage = DummyOperator(
        task_id="fetch_usage",
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE
    )

    fetch_recharge = DummyOperator(
        task_id="fetch_recharge",
        dag=dag,
    )


    End = DummyOperator(
        task_id="End",
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE
    )

    Start >> [profile_sensor,usage_sensor,recharge_sensor]

    profile_sensor >> fetch_profile >> End

    usage_sensor >> fetch_usage >> End

    recharge_sensor >> fetch_recharge >> End
