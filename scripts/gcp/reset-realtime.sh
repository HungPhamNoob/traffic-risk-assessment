#!/bin/bash
# Reset realtime replay state for both streaming and batch branches.
#
# Use this only when you intentionally want to replay from the beginning of
# the 2020+ dataset. The streaming and batch checkpoints are reset together so
# Node 2 and Node 3 do not drift out of sync.

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/opt/traffic}"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env.cloud}"
RESET_LOCAL_COMPOSE="${RESET_LOCAL_COMPOSE:-true}"
RESET_POSTGRES="${RESET_POSTGRES:-true}"
RESET_GCS="${RESET_GCS:-true}"
NODE2_COMPOSE_FILE="${PROJECT_ROOT}/deployment/node2-streaming/docker-compose.yaml"
NODE3_COMPOSE_FILE="${PROJECT_ROOT}/deployment/node3-batch/docker-compose.yaml"

cd "${PROJECT_ROOT}"

if [ -f "${ENV_FILE}" ]; then
  set -a
  . "${ENV_FILE}"
  set +a
else
  echo "ERROR: ${ENV_FILE} does not exist."
  exit 1
fi

export CLOUDSDK_CONFIG="${CLOUDSDK_CONFIG:-/tmp/gcloud-config-$(id -u)}"
mkdir -p "${CLOUDSDK_CONFIG}"
chmod 700 "${CLOUDSDK_CONFIG}"
echo "Cloud SDK runtime config: ${CLOUDSDK_CONFIG}"

if [ "${RESET_LOCAL_COMPOSE}" = "true" ]; then
  echo "Stopping Node 2 streaming services and clearing local Kafka/Flink volumes if this script is running on Node 2..."
  docker compose \
    --project-directory "${PROJECT_ROOT}" \
    --env-file "${ENV_FILE}" \
    -f "${NODE2_COMPOSE_FILE}" \
    down --volumes --remove-orphans 2>/dev/null || true

  echo "Stopping Node 3 batch services and clearing local Spark volumes if this script is running on Node 3..."
  docker compose \
    --project-directory "${PROJECT_ROOT}" \
    --env-file "${ENV_FILE}" \
    -f "${NODE3_COMPOSE_FILE}" \
    down --volumes --remove-orphans 2>/dev/null || true
else
  echo "Local Docker Compose reset is disabled for this host."
fi

if [ "${RESET_POSTGRES}" = "true" ]; then
  echo "Dropping realtime PostgreSQL serving tables if this script is running on Node 1..."
  if docker ps --format '{{.Names}}' | grep -q '^node1-postgres$'; then
    docker exec -i node1-postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" <<SQL
DROP TABLE IF EXISTS ${POSTGRES_US_PREDICTION_TABLE:-${POSTGRES_PREDICTION_TABLE:-traffic_risk_predictions}} CASCADE;
DROP TABLE IF EXISTS ${POSTGRES_TOMTOM_TABLE:-traffic_tomtom_incidents} CASCADE;
SQL
  else
    echo "Node 1 PostgreSQL container is not present on this host. Skipping table reset."
  fi
else
  echo "PostgreSQL table reset is disabled for this host."
fi

if [ "${RESET_GCS}" = "true" ]; then
  echo "Deleting Flink checkpoints: ${FLINK_CHECKPOINT_DIR}"
  gsutil -m rm -r "${FLINK_CHECKPOINT_DIR%/}/**" 2>/dev/null || true

  echo "Deleting Spark checkpoints: ${SPARK_CHECKPOINT_DIR}"
  gsutil -m rm -r "${SPARK_CHECKPOINT_DIR%/}/**" 2>/dev/null || true

  echo "Deleting generated silver replay features: ${SILVER_FEATURES_PATH}"
  gsutil -m rm -r "${SILVER_FEATURES_PATH%/}/**" 2>/dev/null || true

  echo "Deleting generated gold retrain data: ${GOLD_RETRAIN_PATH}"
  gsutil -m rm -r "${GOLD_RETRAIN_PATH%/}/**" 2>/dev/null || true
else
  echo "GCS checkpoint and dataset reset is disabled for this host."
fi

echo "Realtime reset completed. Start Node 2 and Node 3 together before replaying again."
