#!/usr/bin/env python3
"""
Airflow DAG 2 - Realtime Pair Health Check

Purpose:
    Runs frequently to verify the synchronized realtime pair is healthy.
    Checks Kafka, Flink, the active Flink job, and Spark.

    If either branch is unhealthy, Airflow restarts Node 2 and Node 3 together
    so replay offsets, checkpoints, and retraining inputs stay aligned.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# ============================================================
# Default args
# ============================================================
default_args = {
    "owner": "hung",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

# ============================================================
# DAG definition
# ============================================================
with DAG(
    "streaming_health_check",
    default_args=default_args,
    description="Monitor Kafka + Flink + Producer health",
    schedule_interval=os.getenv("AIRFLOW_STREAM_HEALTH_SCHEDULE", "*/5 * * * *"),
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["streaming", "monitoring"],
) as dag:

    # ----------------------------------------------------------
    # Task 1: Check Kafka broker is alive on Node 2
    # ----------------------------------------------------------
    check_kafka = BashOperator(
        task_id="check_kafka",
        bash_command="""
            echo "=== Checking Kafka broker ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node2-streaming "
                docker exec node2-kafka-1 kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1
            " && echo "Kafka: OK" || {
                echo "WARNING: Kafka is DOWN or Node 2 is unreachable"
                exit 1
            }
        """,
    )

    # ----------------------------------------------------------
    # Task 2: Check Flink JobManager is running on Node 2
    # ----------------------------------------------------------
    check_flink = BashOperator(
        task_id="check_flink",
        bash_command="""
            echo "=== Checking Flink JobManager ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node2-streaming "
                curl -sf http://localhost:8081/overview > /dev/null 2>&1
            " && echo "Flink: OK" || {
                echo "WARNING: Flink JobManager is DOWN or Node 2 is unreachable"
                exit 1
            }
        """,
    )

    # ----------------------------------------------------------
    # Task 3: Check Flink job is actively running
    # ----------------------------------------------------------
    check_flink_job = BashOperator(
        task_id="check_flink_job",
        bash_command=r"""
            echo "=== Checking Flink job status ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node2-streaming "
                RUNNING=\$(curl -s http://localhost:8081/jobs | python3 -c \\" 
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
running = sum(1 for j in jobs if j.get('status') == 'RUNNING')
sys.stdout.write(str(running))
\\")
                if [ \"\$RUNNING\" -gt 0 ]; then
                    echo \"Flink job: RUNNING (\$RUNNING active)\"
                else
                    echo \"WARNING: No Flink job is running\"
                    exit 1
                fi
            " || {
                echo "WARNING: Could not verify Flink job status"
                exit 1
            }
        """,
        trigger_rule="all_done",  # Run even if check_flink fails
    )

    check_batch_node = BashOperator(
        task_id="check_batch_node",
        bash_command="""
            echo "=== Checking Spark on Node 3 ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node3-batch "
                curl -sf http://localhost:8080 > /dev/null 2>&1
            " && echo "Spark: OK" || {
                echo "WARNING: Spark is DOWN or Node 3 is unreachable"
                exit 1
            }
        """,
    )

    recover_realtime_pair = BashOperator(
        task_id="recover_realtime_pair",
        bash_command="""
            echo "=== Restarting the synchronized realtime pair ==="
            cd /opt/traffic
            bash scripts/gcp/node23-lifecycle.sh restart
        """,
        trigger_rule="one_failed",
    )

    # ----------------------------------------------------------
    # Task 4: Summary report
    # ----------------------------------------------------------
    summary = BashOperator(
        task_id="health_summary",
        bash_command="""
            echo "============================================="
            echo "Streaming health check completed at $(date)"
            echo "============================================="
        """,
        trigger_rule="all_done",
    )

    # ----------------------------------------------------------
    # DAG structure: all checks run in parallel, then summary
    # ----------------------------------------------------------
    (
        [check_kafka, check_flink, check_flink_job, check_batch_node]
        >> recover_realtime_pair
        >> summary
    )
    [check_kafka, check_flink, check_flink_job, check_batch_node] >> summary
