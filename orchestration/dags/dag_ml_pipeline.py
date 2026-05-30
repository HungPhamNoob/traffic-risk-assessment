#!/usr/bin/env python3
"""
orchestration/dags/dag_ml_pipeline.py
Airflow DAG: model_retrain_hourly

Purpose:
    Runs every hour to retrain the US H2O model with fresh US replay data.
    Spark reads US silver data -> cleans -> writes gold Parquet -> H2O retrains -> MLflow registers new version.

    TomTom live incidents are rule-based and do not participate in Spark,
    H2O, MLflow model training, or model serving.

The schedule is configurable through AIRFLOW_MODEL_RETRAIN_SCHEDULE. The
default remains fast for demonstration; set it to '0 * * * *' for hourly
production retraining.

Steps:
    1. Spark Silver -> Gold: Reads Flink-generated feature JSONL files from
       GCS Silver, validates schema, deduplicates, and writes ML-ready Parquet
       to GCS Gold on Node 3.
    2. H2O retrain: Trains a new H2O AutoML model on the Gold Parquet dataset
       and registers the result in MLflow on Node 1.
    3. Notify success: Logs the completion timestamp.

Recovery strategy:
    If the Spark or H2O step fails (e.g. Node 3 is temporarily unavailable),
    Airflow retries up to 5 times with a 5-minute delay. The Node 2/3 lifecycle
    script can be used as a manual recovery hook.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# ---------------------------------------------------------------------------
# Default task arguments
# ---------------------------------------------------------------------------

default_args = {
    "owner": "traffic-risk-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="model_retrain_hourly",
    default_args=default_args,
    description="Periodic H2O AutoML retraining from the latest Flink-generated Silver features",
    schedule_interval=os.getenv("AIRFLOW_MODEL_RETRAIN_SCHEDULE", "*/15 * * * *"),
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["ml", "retrain", "batch", "spark", "h2o"],
) as dag:

    # ------------------------------------------------------------------
    # Task 1 – Spark: Silver → Gold Parquet (executes on Node 3)
    # ------------------------------------------------------------------
    spark_silver_to_gold = BashOperator(
        task_id="spark_silver_to_gold",
        bash_command=r"""
            echo "=== [Airflow] Spark Silver -> Gold ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node3-batch "
                cd /opt/traffic &&
                COMPOSE_FILE=deployment/node3-batch/docker-compose.yaml &&
                docker compose --env-file .env.cloud -f \$COMPOSE_FILE up -d &&
                docker compose --env-file .env.cloud -f \$COMPOSE_FILE exec -T spark-master \
                    /opt/spark/bin/spark-submit \
                    --master spark://spark-master:7077 \
                    /opt/traffic/processing/spark_batch.py
            " || {
                echo "ERROR: Spark Silver -> Gold failed. Attempting Node 2/3 recovery before retry."
                bash /opt/traffic/scripts/gcp/node23-lifecycle.sh restart || true
                exit 1
            }
        """,
    )

    # ------------------------------------------------------------------
    # Task 2 – H2O AutoML: retrain on Gold Parquet (executes on Node 3)
    # ------------------------------------------------------------------
    h2o_retrain = BashOperator(
        task_id="h2o_retrain",
        bash_command=r"""
            echo "=== [Airflow] H2O AutoML Retrain ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node3-batch "
                cd /opt/traffic &&
                export MLFLOW_TRACKING_URI=http://10.128.0.4:5000 &&
                python ml/training/h2o_after_2020.py
            " || {
                echo "ERROR: H2O retrain on Node 3 failed. Airflow will retry."
                exit 1
            }
        """,
    )

    # ------------------------------------------------------------------
    # Task 3 – Notify success
    # ------------------------------------------------------------------
    notify_success = BashOperator(
        task_id="notify_success",
        bash_command="""
            echo "=== [Airflow] Model retrain pipeline completed successfully ==="
            echo "Completed at: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        """,
    )

    # DAG structure: Spark -> H2O -> Notify
    spark_silver_to_gold >> h2o_retrain >> notify_success
