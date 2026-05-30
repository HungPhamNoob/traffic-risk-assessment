#!/usr/bin/env python3
"""
Airflow DAG 1 - Hourly Model Retraining

Purpose:
    Runs every hour to retrain the US H2O model with fresh US replay data.
    Spark reads US silver data -> cleans -> writes gold Parquet -> H2O retrains -> MLflow registers new version.

    TomTom live incidents are rule-based and do not participate in Spark,
    H2O, MLflow model training, or model serving.

    If the batch branch fails, Airflow recovers Node 2 and Node 3 together so
    the replay timeline stays synchronized across both branches.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# ============================================================
# Default args - applied to all tasks
# ============================================================
default_args = {
    "owner": "hung",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,  # Retry 5 times if Node 3 is down
    "retry_delay": timedelta(minutes=5),  # Wait 5 minutes between retries
}

# ============================================================
# DAG definition
# ============================================================
with DAG(
    "model_retrain_hourly",
    default_args=default_args,
    description="Hourly H2O retrain from latest silver data",
    schedule_interval=os.getenv("AIRFLOW_MODEL_RETRAIN_SCHEDULE", "*/15 * * * *"),
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["ml", "retrain", "batch"],
) as dag:

    # ----------------------------------------------------------
    # Task 1: Spark - Silver -> Gold Parquet (runs on Node 3)
    # ----------------------------------------------------------
    spark_silver_to_gold = BashOperator(
        task_id="spark_silver_to_gold",
        bash_command=r"""
            echo "=== Spark: Silver -> Gold ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node3-batch "
                cd /opt/traffic &&
                COMPOSE_FILE=deployment/node3-batch/docker-compose.yaml &&
                docker compose --env-file .env.cloud -f \$COMPOSE_FILE up -d &&
                docker compose --env-file .env.cloud -f \$COMPOSE_FILE exec -T spark-master \
                    /opt/spark/bin/spark-submit \
                    --master spark://spark-master:7077 \
                    /opt/traffic/processing/spark_batch.py
            " || {
                echo "WARNING: Batch flow failed. Restarting the synchronized Node 2/Node 3 pair before retry."
                bash /opt/traffic/scripts/gcp/node23-lifecycle.sh restart || true
                exit 1
            }
        """,
    )

    # ----------------------------------------------------------
    # Task 2: H2O - Retrain model from gold Parquet (runs on Node 3)
    # ----------------------------------------------------------
    h2o_retrain = BashOperator(
        task_id="h2o_retrain",
        bash_command=r"""
            echo "=== H2O: Retrain ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node3-batch "
                cd /opt/traffic &&
                export MLFLOW_TRACKING_URI=http://10.128.0.4:5000 &&
                python ml/training/h2o_after_2020.py
            " || {
                echo "WARNING: Node 3 retrain failed. Airflow will retry the synchronized pair later."
                exit 1
            }
        """,
    )

    # ----------------------------------------------------------
    # Task 3: Notify success
    # ----------------------------------------------------------
    notify_success = BashOperator(
        task_id="notify_success",
        bash_command="""
            echo "Model retrain completed successfully at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        """,
    )

    # ----------------------------------------------------------
    # DAG structure: Spark -> H2O -> Notify
    # ----------------------------------------------------------
    spark_silver_to_gold >> h2o_retrain >> notify_success
