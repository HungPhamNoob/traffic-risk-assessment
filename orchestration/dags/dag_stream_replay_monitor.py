#!/usr/bin/env python3
"""
Airflow DAG: streaming_health_check

Checks the Node 2 streaming stack for the split US + TomTom pipeline:

1. Kafka broker is reachable.
2. Both raw Kafka topics exist.
3. Flink JobManager REST API is reachable.
4. Both Flink jobs are RUNNING:
   - Flink Traffic Risk Prediction - GCS + PostGIS
   - Flink TomTom Live Incidents - PostGIS
5. The TomTom live producer container is running.

TomTom live ingestion does not depend on Spark/Node 3, so this DAG only
recovers the Node 2 streaming stack.
"""

import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "traffic.us.raw")
KAFKA_TOPIC_TOMTOM_RAW = os.getenv("KAFKA_TOPIC_TOMTOM_RAW", "traffic.tomtom.raw")

default_args = {
    "owner": "traffic-risk-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

REQUIRED_FLINK_JOBS = {
    "Flink Traffic Risk Prediction - GCS + PostGIS",
    "Flink TomTom Live Incidents - PostGIS",
}

with DAG(
    dag_id="streaming_health_check",
    default_args=default_args,
    description="Monitor Node2 Kafka, Flink US/TomTom jobs, and TomTom producer",
    schedule_interval=os.getenv("AIRFLOW_STREAM_HEALTH_SCHEDULE", "*/5 * * * *"),
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["streaming", "monitoring", "health"],
) as dag:
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

    check_kafka_topics = BashOperator(
        task_id="check_kafka_topics",
        bash_command=f"""
            echo "=== [Health] Checking Kafka topics on Node 2 ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node2-streaming "
                for TOPIC in {KAFKA_TOPIC_RAW} {KAFKA_TOPIC_TOMTOM_RAW}; do
                    docker exec node2-kafka-1 kafka-topics \
                        --bootstrap-server localhost:9092 \
                        --describe \
                        --topic \\$TOPIC > /dev/null 2>&1 || exit 1
                done
            " && echo "Kafka topics: OK" || {{
                echo "ERROR: Required Kafka topic is missing or unreachable"
                exit 1
            }}
        """,
    )

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

    check_flink_jobs = BashOperator(
        task_id="check_flink_jobs",
        bash_command=f"""
            echo "=== [Health] Checking split Flink jobs on Node 2 ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node2-streaming "
                curl -s http://localhost:8081/jobs/overview | python3 -c '
import json
import sys

jobs = json.load(sys.stdin).get(\"jobs\", [])
required = set({json.dumps(sorted(REQUIRED_FLINK_JOBS))})
running = {{job.get(\"name\") for job in jobs if job.get(\"state\") == \"RUNNING\"}}
missing = sorted(required - running)
if missing:
    print(\"ERROR: Missing running Flink jobs: \" + \", \".join(missing))
    sys.exit(1)
print(\"Flink jobs: OK\")
'
            " || {{
                echo "ERROR: Could not verify Flink job status"
                exit 1
            }}
        """,
        trigger_rule="all_done",
    )

    check_tomtom_producer = BashOperator(
        task_id="check_tomtom_producer",
        bash_command="""
            echo "=== [Health] Checking TomTom producer on Node 2 ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node2-streaming "
                test \"$(docker inspect -f '{{.State.Running}}' node2-tomtom-producer 2>/dev/null)\" = \"true\"
            " && echo "TomTom producer: OK" || {
                echo "ERROR: TomTom producer is not running"
                exit 1
            }
        """,
    )

    recover_streaming_stack = BashOperator(
        task_id="recover_streaming_stack",
        bash_command="""
            echo "=== [Recovery] Restarting Node 2 streaming stack ==="
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 node2-streaming "
                cd /opt/traffic &&
                docker compose --env-file .env.cloud \
                    -f deployment/node2-streaming/docker-compose.yaml up -d
            "
        """,
        trigger_rule="one_failed",
    )

    summary = BashOperator(
        task_id="health_summary",
        bash_command="""
            echo "============================================="
            echo "Streaming health check completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
            echo "============================================="
        """,
        trigger_rule="all_done",
    )

    checks = [
        check_kafka,
        check_kafka_topics,
        check_flink,
        check_flink_jobs,
        check_tomtom_producer,
    ]

    checks >> recover_streaming_stack >> summary
    checks >> summary
