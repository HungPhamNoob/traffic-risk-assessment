from airflow import DAG
from airflow.operators.python import PythonOperator

# from airflow.providers.google.cloud.operators.gcs import GCSToGCSOperator
from datetime import datetime, timedelta
import logging

default_args = {
    "owner": "dang",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}


def trigger_spark_job(ti, **context):
    """Trigger Spark job on Node 3 via SSH or REST API"""
    # Option 1: SSHOperator (nếu config SSH access)
    # Option 2: REST API call to Spark submit endpoint
    logging.info(f"Triggering Spark ETL for date: {context['ds']}")
    # Implement spark-submit logic here
    return {"status": "success", "date": context["ds"]}


def validate_output(ti, **context):
    """Validate GCS output after Spark job"""
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket("capstone-team4-silver")
    # Check if files exist for today's date
    blobs = list(bucket.list_blobs(prefix=f"processed/{context['ds']}/"))
    if len(blobs) == 0:
        raise ValueError(f"No output files found for {context['ds']}")
    logging.info(f"Validated {len(blobs)} output files")
    return {"validated": True, "count": len(blobs)}


with DAG(
    "dag_batch_etl",
    default_args=default_args,
    schedule_interval="0 2 * * *",  # Daily at 2 AM
    catchup=False,
    tags=["batch", "etl", "spark"],
) as dag:

    trigger_spark = PythonOperator(
        task_id="trigger_spark_etl",
        python_callable=trigger_spark_job,
        provide_context=True,
    )

    validate = PythonOperator(
        task_id="validate_gcs_output",
        python_callable=validate_output,
        provide_context=True,
    )

    # Optional: Trigger downstream DAGs
    # trigger_model_retrain = TriggerDagRunOperator(...)

    trigger_spark >> validate
