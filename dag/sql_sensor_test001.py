from airflow import DAG, settings
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
from airflow.sensors.sql import SqlSensor
from airflow.models.connection import Connection
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator



default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2021, 8, 23),
}

with DAG(
        'sql_custom_sensor_test',
        schedule_interval=None,
        concurrency=1, max_active_runs=1,
        default_args=default_args
) as dag:
    def create_or_update_mysql_connection(conn_id, conn_dict):
        session = settings.Session()
        conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()

        if conn is None:
            new_conn = Connection(conn_id=conn_id, **conn_dict)
            session.add(new_conn)
            session.commit()
            print(f"Created new connection with ID: {conn_id}")
        else:
            for attr, value in conn_dict.items():
                setattr(conn, attr, value)
            session.commit()
            print(f"Updated connection with ID: {conn_id}")

        session.close()




    def success_criteria(record):
        print("record success : "+record)
        return record


    def failure_criteria(record):
        print("record failed: " + record)
        return True if not record else False


    create_mysql_conn = PythonOperator(
        task_id='create_mysql_conn',
        python_callable=create_or_update_mysql_connection,
        op_kwargs={
            'conn_id': 'dynamic_mysql_conn',
            'conn_dict': {
                'conn_type': 'mysql',
                'host': '10.128.0.5',
                'schema': 'airflow_rnd',
                'login': 'hadoopuser',
                'password': 'Ambariuser.123',
                'port': 3306,
            },
        },
        dag=dag,
    )


    profile_sensor = SqlSensor(
        task_id='check_for_data_in_profile',
        conn_id='dynamic_mysql_conn',
        success=success_criteria,
        failure=failure_criteria,
        poke_interval=20,
        #mode="reschedule",
        timeout=60 * 5,
        sql="""
          SELECT completion_status FROM `Audit_Log_Fact` WHERE table_name='Profile_Cdr' and completion_status='Y';
        """,
        dag=dag)

    create_mysql_conn >> profile_sensor
