#!/usr/bin/env python3
"""
Airflow DAG 2 - Realtime Pair Health Check

Purpose:
    Runs every hour to verify the synchronized realtime pair is healthy.
    Checks Kafka, both raw topics, both Flink jobs, and the TomTom producer.

    TomTom live ingestion does not depend on Spark/Node 3, so this DAG only
    recovers the Node 2 streaming stack.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "traffic.us.raw")
KAFKA_TOPIC_TOMTOM_RAW = os.getenv("KAFKA_TOPIC_TOMTOM_RAW", "traffic.tomtom.raw")

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
    description="Monitor Node2 Kafka + Flink + TomTom producer health",
    schedule_interval="@hourly",
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
    # Task 2: Check both raw Kafka topics exist
    # ----------------------------------------------------------
    check_kafka_topics = BashOperator(
        task_id="check_kafka_topics",
        bash_command=f"""
            echo "=== Checking Kafka topics ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node2-streaming "
                for TOPIC in {KAFKA_TOPIC_RAW} {KAFKA_TOPIC_TOMTOM_RAW}; do
                    docker exec node2-kafka-1 kafka-topics \
                        --bootstrap-server localhost:9092 \
                        --describe \
                        --topic \\$TOPIC > /dev/null 2>&1 || exit 1
                done
            " && echo "Kafka topics: OK" || {{
                echo "WARNING: Required Kafka topic is missing or unreachable"
                exit 1
            }}
        """,
    )

    # ----------------------------------------------------------
    # Task 3: Check Flink JobManager is running on Node 2
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
    # Task 4: Check both Flink jobs are actively running
    # ----------------------------------------------------------
    check_flink_jobs = BashOperator(
        task_id="check_flink_jobs",
        bash_command=r"""
            echo "=== Checking Flink job status ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node2-streaming "
                curl -s http://localhost:8081/jobs/overview | python3 -c \\"
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
required = {
    'Flink Traffic Risk Prediction - GCS + PostGIS',
    'Flink TomTom Live Incidents - PostGIS',
}
running = {j.get('name') for j in jobs if j.get('state') == 'RUNNING'}
missing = sorted(required - running)
if missing:
    print('WARNING: Missing running Flink jobs: ' + ', '.join(missing))
    sys.exit(1)
print('Flink jobs: OK')
\\")
            " || {
                echo "WARNING: Could not verify Flink job status"
                exit 1
            }
        """,
        trigger_rule="all_done",  # Run even if check_flink fails
    )

    # ----------------------------------------------------------
    # Task 5: Check TomTom producer container
    # ----------------------------------------------------------
    check_tomtom_producer = BashOperator(
        task_id="check_tomtom_producer",
        bash_command="""
            echo "=== Checking TomTom producer ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node2-streaming "
                test \"\$(docker inspect -f '{{.State.Running}}' node2-tomtom-producer 2>/dev/null)\" = \"true\"
            " && echo "TomTom producer: OK" || {
                echo "WARNING: TomTom producer is not running"
                exit 1
            }
        """,
    )

    recover_streaming_stack = BashOperator(
        task_id="recover_streaming_stack",
        bash_command="""
            echo "=== Restarting Node 2 streaming stack ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node2-streaming "
                cd /opt/traffic &&
                docker compose --env-file .env.cloud -f deployment/node2-streaming/docker-compose.yaml up -d
            "
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
        [
            check_kafka,
            check_kafka_topics,
            check_flink,
            check_flink_jobs,
            check_tomtom_producer,
        ]
        >> recover_streaming_stack
        >> summary
    )
    [
        check_kafka,
        check_kafka_topics,
        check_flink,
        check_flink_jobs,
        check_tomtom_producer,
    ] >> summary
