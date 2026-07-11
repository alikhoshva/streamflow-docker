import os
import shutil
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    'owner': 'streamflow_ops',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def clear_processed_rejects():
    """Housekeeping step: wipes evaluated records to avoid infinite re-processing loops."""
    rejects_path = "/opt/streamflow/data/rejects"
    if os.path.exists(rejects_path):
        for item in os.listdir(rejects_path):
            if item == ".gitkeep":
                continue
            item_path = os.path.join(rejects_path, item)
            try:
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                else:
                    os.unlink(item_path)
            except Exception as e:
                print(f"Failed to delete {item_path}: {e}")
        print("Cleared processed rejects directory.")

with DAG(
    dag_id='streamflow_automated_reprocessing',
    default_args=default_args,
    description='Automated standalone batch recovery loop for data quality rejects',
    schedule='@hourly',
    catchup=False,
    tags=['streamflow', 'recovery', 'standalone']
) as dag:

    # Task 1: Submit the standalone script to heal and append records
    execute_recovery_job = SparkSubmitOperator(
        task_id='submit_spark_reprocessing_job',
        application='/opt/airflow/jobs/reprocess_rejects.py',
        conn_id='spark_default',
        name='StreamflowAutomatedReprocessorBatchStandalone',
        conf={
            "spark.master": "local[*]"
        },
        verbose=True
    )

    # Task 2: Delete items from the rejects folder only after successful append execution
    purge_rejects_directory = PythonOperator(
        task_id='purge_processed_rejects_directory',
        python_callable=clear_processed_rejects
    )

    execute_recovery_job >> purge_rejects_directory