from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'paul_yoo',
    'start_date': datetime(2026, 7, 6)
}

with DAG(
    'prototype_connecting_pieces_test',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:

    # Programmatically execute your test file using the pre-cached container binaries
    trigger_spark_job = BashOperator(
        task_id='trigger_spark_job',
        bash_command='spark-submit /opt/airflow/jobs/smoke_test.py'
    )

    trigger_spark_job