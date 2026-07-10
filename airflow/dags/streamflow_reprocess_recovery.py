import os
import shutil
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    'owner': 'streamflow_ops',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 6), # Aligning with project inception date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def clear_processed_rejects():
    """
    Optional Post-recovery step: Wipes the rejects folder after a run 
    so the same dead records aren't re-evaluated in the next batch schedule.
    """
    rejects_path = "/opt/streamflow/data/rejects"
    if os.path.exists(rejects_path):
        for item in os.listdir(rejects_path):
            if item == ".gitkeep":
                continue
            item_path = os.path.join(rejects_path, item)
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)
            else:
                os.unlink(item_path)
        print("Cleared processed rejects directory to prevent processing loops.")

with DAG(
    dag_id='streamflow_automated_reprocessing',
    default_args=default_args,
    description='Automated batch recovery loop for quarantined data quality rejects',
    schedule='@hourly', # Runs frequently to keep data freshness high
    catchup=False,
) as dag:

    # Task 1: Submit the Spark App to triage and re-inject healed records
    execute_recovery_job = SparkSubmitOperator(
        task_id='submit_spark_reprocessing_job',
        application='/opt/airflow/jobs/reprocess_rejects_job.py',
        conn_id='spark_default',
        name='StreamflowAutomatedReprocessorBatch',
        conf={
            "spark.master": "local[*]"
        },
        verbose=True
    )

    # Task 2: Housekeeping step to clean up directory state
    purge_rejects_directory = PythonOperator(
        task_id='purge_processed_rejects_directory',
        python_callable=clear_processed_rejects
    )

    execute_recovery_job >> purge_rejects_directory