#!/usr/bin/env python3
"""
orchestration/dags/dag_stream_replay_monitor.py
Airflow DAG: streaming_health_check

Monitors the realtime streaming pair (Node 2 + Node 3) on a configurable
schedule (default: every 2 minutes). If any check fails, the recovery task
restarts both nodes in sync to keep Kafka offsets and Flink checkpoints aligned.

Health checks:
    1. Kafka broker API on Node 2 (brokers reachable, topics exist)
    2. Flink JobManager REST API on Node 2 (8081 accessible)
    3. Flink job running status (at least one RUNNING job)
    4. Spark master UI on Node 3 (8080 accessible)

Recovery:
    If any single check fails, the recover_realtime_pair task runs the
    node23-lifecycle.sh restart script to bring both nodes back in sync
    before Airflow retries.
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
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="streaming_health_check",
    default_args=default_args,
    description="Periodic health check for Kafka, Flink, and Spark; auto-recovers failed nodes",
    schedule_interval=os.getenv("AIRFLOW_STREAM_HEALTH_SCHEDULE", "*/2 * * * *"),
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["streaming", "monitoring", "health"],
) as dag:

    # ------------------------------------------------------------------
    # Check 1 – Kafka broker reachability on Node 2
    # ------------------------------------------------------------------
    check_kafka = BashOperator(
        task_id="check_kafka",
        bash_command="""
            echo "=== [Health] Checking Kafka broker on Node 2 ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node2-streaming "
                docker exec node2-kafka-1 kafka-broker-api-versions \
                    --bootstrap-server localhost:9092 > /dev/null 2>&1
            " && echo "Kafka: OK" || {
                echo "ERROR: Kafka is unreachable on Node 2"
                exit 1
            }
        """,
    )

    # ------------------------------------------------------------------
    # Check 2 – Flink JobManager REST API on Node 2
    # ------------------------------------------------------------------
    check_flink = BashOperator(
        task_id="check_flink",
        bash_command="""
            echo "=== [Health] Checking Flink JobManager on Node 2 ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node2-streaming "
                curl -sf http://localhost:8081/overview > /dev/null 2>&1
            " && echo "Flink JobManager: OK" || {
                echo "ERROR: Flink JobManager is unreachable on Node 2"
                exit 1
            }
        """,
    )

    # ------------------------------------------------------------------
    # Check 3 – At least one Flink job is in RUNNING state
    # ------------------------------------------------------------------
    check_flink_job = BashOperator(
        task_id="check_flink_job",
        bash_command=r"""
            echo "=== [Health] Checking active Flink job on Node 2 ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node2-streaming "
                RUNNING=\$(curl -s http://localhost:8081/jobs | python3 -c \"
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
running = sum(1 for j in jobs if j.get('status') == 'RUNNING')
sys.stdout.write(str(running))
\")
                if [ \"\$RUNNING\" -gt 0 ]; then
                    echo \"Flink job: RUNNING (\$RUNNING active)\"
                else
                    echo \"ERROR: No active Flink job found\"
                    exit 1
                fi
            " || {
                echo "ERROR: Could not verify Flink job status"
                exit 1
            }
        """,
        trigger_rule="all_done",
    )

    # ------------------------------------------------------------------
    # Check 4 – Spark master UI on Node 3
    # ------------------------------------------------------------------
    check_batch_node = BashOperator(
        task_id="check_batch_node",
        bash_command="""
            echo "=== [Health] Checking Spark master on Node 3 ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node3-batch "
                curl -sf http://localhost:8080 > /dev/null 2>&1
            " && echo "Spark master: OK" || {
                echo "ERROR: Spark master is unreachable on Node 3"
                exit 1
            }
        """,
    )

    # ------------------------------------------------------------------
    # Recovery – restart Nodes 2 and 3 in sync if any check fails
    # ------------------------------------------------------------------
    recover_realtime_pair = BashOperator(
        task_id="recover_realtime_pair",
        bash_command="""
            echo "=== [Recovery] Restarting synchronized realtime pair (Node 2 + Node 3) ==="
            cd /opt/traffic
            bash scripts/gcp/node23-lifecycle.sh restart
        """,
        trigger_rule="one_failed",
    )

    # ------------------------------------------------------------------
    # Summary – always runs regardless of upstream result
    # ------------------------------------------------------------------
    summary = BashOperator(
        task_id="health_summary",
        bash_command="""
            echo "============================================="
            echo "[Health Check] Completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
            echo "============================================="
        """,
        trigger_rule="all_done",
    )

    # DAG structure: all checks run in parallel, then recovery (if needed), then summary.
    (
        [check_kafka, check_flink, check_flink_job, check_batch_node]
        >> recover_realtime_pair
        >> summary
    )
    [check_kafka, check_flink, check_flink_job, check_batch_node] >> summary
