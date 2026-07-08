import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'taranjit_singh', 
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def validate_curated_data():
    # UPDATED: Added /opt/streamflow/ to match the Docker volume mount
    paths_to_check = [
        "/opt/streamflow/data/curated/daily_summary/top_items",
        "/opt/streamflow/data/curated/daily_summary/low_stock_alerts"
    ]
    
    for path in paths_to_check:
        if not os.path.exists(path):
            raise ValueError(f"Validation Failed: Directory '{path}' does not exist.")
        
        files = os.listdir(path)
        if not files:
            raise ValueError(f"Validation Failed: Directory '{path}' is empty.")
        
        print(f"Validation Passed: Found {len(files)} items in {path}")

with DAG(
    dag_id='streamflow_daily_summary',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
) as dag:

    run_daily_summary = SparkSubmitOperator(
        task_id='submit_spark_daily_summary',
        application='/opt/airflow/jobs/daily_summary.py',
        conn_id='spark_default', 
        name='StreamflowDailySummaryBatch',
        conf={
            "spark.master": "local[*]"
        },
        verbose=True
    )

    validate_outputs = PythonOperator(
        task_id='validate_output_files_exist',
        python_callable=validate_curated_data
    )

    run_daily_summary >> validate_outputs