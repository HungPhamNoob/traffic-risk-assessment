#!/bin/bash
# Start Node 2 streaming services.
#
# Node 2 responsibilities:
#   - Three Kafka brokers.
#   - One raw topic with one partition and three replicas.
#   - Three replay producers split by row_index modulo producer index.
#   - Flink streaming inference job using existing checkpoints when present.

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/opt/traffic}"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env.cloud}"

echo "Node 2 run script started at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Project root: ${PROJECT_ROOT}"
echo "Environment file: ${ENV_FILE}"

cd "${PROJECT_ROOT}"

if [ -f "${ENV_FILE}" ]; then
  set -a
  . "${ENV_FILE}"
  set +a
else
  echo "ERROR: ${ENV_FILE} does not exist."
  exit 1
fi

echo "Starting Kafka, producers, Redis, and Flink streaming job..."
docker compose --env-file "${ENV_FILE}" -f deployment/node2-streaming/docker-compose.yaml up -d

echo "Node 2 services:"
docker compose --env-file "${ENV_FILE}" -f deployment/node2-streaming/docker-compose.yaml ps

echo "Node 2 run script completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
