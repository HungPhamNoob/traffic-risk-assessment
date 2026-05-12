#!/bin/bash
# Start Node 1 control-plane services and bootstrap the offline model.
#
# Node 1 responsibilities:
#   - PostgreSQL/PostGIS for prediction storage.
#   - MLflow tracking and model serving.
#   - FastAPI dashboard backend.
#   - Airflow scheduler/webserver.
#   - Offline H2O training on pre-2020 data before stream/batch nodes start.

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/opt/traffic}"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env.cloud}"

echo "Node 1 run script started at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
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

echo "Starting Node 1 Docker services..."
echo "Removing stale Node 1 containers from previous Compose project names..."
docker rm -f \
  node1-postgres \
  node1-airflow-db \
  node1-airflow \
  node1-blackbox-exporter \
  node1-prometheus \
  node1-grafana \
  node1-mlflow \
  node1-mlflow-serving \
  node1-fastapi \
  2>/dev/null || true

echo "Ensuring the shared Docker network exists before Compose starts."
docker network inspect capstone-net >/dev/null 2>&1 || docker network create capstone-net >/dev/null

docker compose --env-file "${ENV_FILE}" -f deployment/node1-control/docker-compose.yaml up -d --remove-orphans

echo "Checking whether a registered MLflow model already exists..."
MODEL_EXISTS="false"
if command -v python3 >/dev/null 2>&1; then
  MODEL_EXISTS="$(python3 - <<'PY'
import os
import sys

try:
    from mlflow.tracking import MlflowClient
except Exception:
    print("false")
    sys.exit(0)

tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://10.128.0.4:5000")
model_name = os.getenv("ML_MODEL_NAME", "traffic-risk-model")

try:
    client = MlflowClient(tracking_uri=tracking_uri)
    versions = client.search_model_versions(f"name = '{model_name}'")
    print("true" if versions else "false")
except Exception:
    print("false")
PY
)"
fi

if [ "${MODEL_EXISTS}" = "true" ]; then
  echo "MLflow already has a registered model. Offline training bootstrap is skipped."
else
  echo "No registered model found. Running offline feature engineering and H2O training."
  python3 -m pip install --user --upgrade pip
  python3 -m pip install --user pandas numpy h2o mlflow python-dotenv gcsfs google-auth scikit-learn
  if [[ "${US_TRAIN_OFFLINE_PATH:-}" != gs://* ]]; then
    python3 ml/dataset/dataset_offline.py
  else
    echo "US_TRAIN_OFFLINE_PATH is a GCS feature CSV. Skipping local feature generation."
  fi
  python3 ml/training/h2o_before_2020.py
fi

echo "Restarting MLflow model serving after model bootstrap..."
docker compose --env-file "${ENV_FILE}" -f deployment/node1-control/docker-compose.yaml up -d mlflow-serving fastapi

echo "Node 1 services:"
docker compose --env-file "${ENV_FILE}" -f deployment/node1-control/docker-compose.yaml ps

echo "Node 1 run script completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
