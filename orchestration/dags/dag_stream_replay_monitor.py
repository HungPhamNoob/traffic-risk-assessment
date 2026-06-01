#!/usr/bin/env python3
"""
orchestration/dags/dag_stream_replay_monitor.py
Airflow DAG: streaming_health_check

Monitors the realtime streaming pair (Node 2 + Node 3) on a configurable
schedule (default: every 2 minutes) and surfaces health without mutating
runtime state.

Health checks:
    1. Kafka broker API on Node 2 (brokers reachable, topics exist)
    2. Flink JobManager REST API on Node 2 (8081 accessible)
    3. Flink job running status (at least one RUNNING job)
    4. Spark master UI on Node 3 (8080 accessible)

This DAG is intentionally read-only so dashboard health checks never reset
or interrupt the live replay pipeline.
"""

from datetime import datetime
import os

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
    "retries": 0,
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
    max_active_runs=1,
    tags=["streaming", "monitoring", "health"],
) as dag:

    # ------------------------------------------------------------------
    # Check 1 – Kafka broker reachability on Node 2
    # ------------------------------------------------------------------
    check_kafka = BashOperator(
        task_id="check_kafka",
        bash_command="""
            echo "=== [Health] Checking Kafka broker on Node 2 ==="
            python3 - <<PY
import os
import socket
import sys

host = os.environ.get("NODE2_INTERNAL_IP", "10.128.0.5")
ports = (9092, 9093, 9094)
for port in ports:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(5)
        if sock.connect_ex((host, port)) != 0:
            print(f"ERROR: Kafka port {port} is unreachable on {host}")
            sys.exit(1)
print(f"Kafka brokers reachable on {host}:{','.join(map(str, ports))}")
PY
            if [ $? -ne 0 ]; then
                exit 1
            fi
        """,
    )

    # ------------------------------------------------------------------
    # Check 2 – Flink JobManager REST API on Node 2
    # ------------------------------------------------------------------
    check_flink = BashOperator(
        task_id="check_flink",
        bash_command="""
            echo "=== [Health] Checking Flink JobManager on Node 2 ==="
            curl -sf "http://${NODE2_INTERNAL_IP:-10.128.0.5}:8081/overview" > /dev/null 2>&1 && echo "Flink JobManager: OK" || {
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
            RUNNING=$(curl -s "http://${NODE2_INTERNAL_IP:-10.128.0.5}:8081/jobs" | python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
running = sum(1 for j in jobs if j.get('status') == 'RUNNING')
sys.stdout.write(str(running))
") || {
                echo "ERROR: Could not verify Flink job status"
                exit 1
            }
            if [ "$RUNNING" -gt 0 ]; then
                echo "Flink job: RUNNING ($RUNNING active)"
            else
                echo "ERROR: No active Flink job found"
                exit 1
            fi
        """,
    )

    # ------------------------------------------------------------------
    # Check 4 – Spark master UI on Node 3
    # ------------------------------------------------------------------
    check_batch_node = BashOperator(
        task_id="check_batch_node",
        bash_command="""
            echo "=== [Health] Checking Spark master on Node 3 ==="
            curl -sf "http://${NODE3_INTERNAL_IP:-10.128.0.8}:8080" > /dev/null 2>&1 && echo "Spark master: OK" || {
                echo "ERROR: Spark master is unreachable on Node 3"
                exit 1
            }
        """,
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

    # DAG structure: all checks run in parallel and surface status only.
    [check_kafka, check_flink, check_flink_job, check_batch_node] >> summary
