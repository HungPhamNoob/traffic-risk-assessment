#!/usr/bin/env python3
"""
Airflow DAG 1 – Hourly Model Retraining

Purpose:
    Runs every hour to retrain the H2O model with fresh data.
    Spark reads silver data → cleans → writes gold Parquet → H2O retrains → MLflow registers new version.

    If Node 3 (Spot VM) is down, the DAG retries automatically.
    Once the VM comes back, the next retry succeeds.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ============================================================
# Default args – applied to all tasks
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
    schedule_interval="@hourly",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["ml", "retrain", "batch"],
) as dag:

    # ----------------------------------------------------------
    # Task 1: Spark – Silver → Gold Parquet (runs on Node 3)
    # ----------------------------------------------------------
    spark_silver_to_gold = BashOperator(
        task_id="spark_silver_to_gold",
        bash_command="""
            echo "=== Spark: Silver → Gold ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node3-batch "
                cd /opt/traffic &&
                COMPOSE_FILE=deployment/node3-batch/docker-compose.yaml &&
                docker compose --env-file .env.cloud -f \$COMPOSE_FILE up -d &&
                docker compose --env-file .env.cloud -f \$COMPOSE_FILE exec -T spark-master \
                    /opt/spark/bin/spark-submit \
                    --master spark://spark-master:7077 \
                    /opt/traffic/processing/spark_batch.py
            " || {
                echo "WARNING: Node 3 batch flow failed. Restart Node 2 and Node 3 together before retry."
                bash /opt/traffic/scripts/gcp/start-node23-synced.sh || true
                exit 1
            }
        """,
    )

    # ----------------------------------------------------------
    # Task 2: H2O – Retrain model from gold Parquet (runs on Node 3)
    # ----------------------------------------------------------
    h2o_retrain = BashOperator(
        task_id="h2o_retrain",
        bash_command="""
            echo "=== H2O: Retrain ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node3-batch "
                cd /opt/traffic &&
                export MLFLOW_TRACKING_URI=http://10.128.0.4:5000 &&
                python ml/training/train_h2o_online.py
            " || {
                echo "WARNING: Node 3 retrain failed. Airflow will retry later."
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
    # DAG structure: Spark → H2O → Notify
    # ----------------------------------------------------------
    spark_silver_to_gold >> h2o_retrain >> notify_success
