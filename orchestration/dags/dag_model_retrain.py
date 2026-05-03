from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "hung",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def fetch_latest_features(ti, **context):
    """Fetch latest feature set from GCS Silver for training"""
    # Connect to GCS, list latest feature files
    # Return path to ML training script
    return {"feature_path": f"gs://capstone-team4-silver/features/{context['ds']}"}


def train_and_register(ti, **context):
    """Run H2O training and register to MLflow"""
    import subprocess

    feature_path = ti.xcom_pull(task_ids="fetch_features")["feature_path"]

    result = subprocess.run(
        [
            "python",
            "ml/train_h2o.py",
            "--input",
            feature_path,
            "--mlflow-uri",
            "http://mlflow:5000",
            "--experiment",
            "capstone-risk-model",
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Training failed: {result.stderr}")

    return {"model_version": "v1.0", "mlflow_run_id": "xxx"}


with DAG(
    "dag_model_retrain",
    default_args=default_args,
    schedule_interval="0 3 * * 0",  # Weekly on Sunday at 3 AM
    catchup=False,
    tags=["ml", "retrain", "h2o"],
) as dag:

    fetch = PythonOperator(
        task_id="fetch_features",
        python_callable=fetch_latest_features,
        provide_context=True,
    )

    train = PythonOperator(
        task_id="train_and_register",
        python_callable=train_and_register,
        provide_context=True,
    )

    fetch >> train
