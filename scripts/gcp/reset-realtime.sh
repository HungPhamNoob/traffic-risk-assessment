#!/bin/bash
# Reset realtime replay state for both streaming and batch branches.
#
# Use this only when you intentionally want to replay from the beginning of
# the 2020+ dataset. The streaming and batch checkpoints are reset together so
# Node 2 and Node 3 do not drift out of sync.

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/opt/traffic}"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env.cloud}"

cd "${PROJECT_ROOT}"

if [ -f "${ENV_FILE}" ]; then
  set -a
  . "${ENV_FILE}"
  set +a
else
  echo "ERROR: ${ENV_FILE} does not exist."
  exit 1
fi

echo "Stopping Node 2 streaming services if this script is running on Node 2..."
docker compose --env-file "${ENV_FILE}" -f deployment/node2-streaming/docker-compose.yaml down --remove-orphans 2>/dev/null || true

echo "Stopping Node 3 batch services if this script is running on Node 3..."
docker compose --env-file "${ENV_FILE}" -f deployment/node3-batch/docker-compose.yaml down --remove-orphans 2>/dev/null || true

echo "Deleting Flink checkpoints: ${FLINK_CHECKPOINT_DIR}"
gsutil -m rm -r "${FLINK_CHECKPOINT_DIR%/}/**" 2>/dev/null || true

echo "Deleting Spark checkpoints: ${SPARK_CHECKPOINT_DIR}"
gsutil -m rm -r "${SPARK_CHECKPOINT_DIR%/}/**" 2>/dev/null || true

echo "Deleting generated silver replay features: ${SILVER_FEATURES_PATH}"
gsutil -m rm -r "${SILVER_FEATURES_PATH%/}/**" 2>/dev/null || true

echo "Deleting generated gold retrain data: ${GOLD_RETRAIN_PATH}"
gsutil -m rm -r "${GOLD_RETRAIN_PATH%/}/**" 2>/dev/null || true

echo "Realtime reset completed. Start Node 2 and Node 3 together before replaying again."
