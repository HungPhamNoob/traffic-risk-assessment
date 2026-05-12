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

export MLFLOW_TRACKING_URI="${NODE1_MLFLOW_TRACKING_URI:-http://localhost:5000}"
echo "Node 1 MLflow tracking URI: ${MLFLOW_TRACKING_URI}"

echo "Starting Node 1 Docker services..."
echo "Preparing writable runtime directories for containers."
mkdir -p orchestration/logs ml/mlruns
chown -R 50000:0 orchestration/logs 2>/dev/null || true
chmod -R g+rwX orchestration/logs 2>/dev/null || true

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

echo "Waiting for MLflow tracking server before checking model registry..."
for attempt in $(seq 1 30); do
  if curl --max-time 5 -fsS "${MLFLOW_TRACKING_URI}/health" >/dev/null 2>&1; then
    echo "MLflow tracking server is reachable."
    break
  fi
  echo "MLflow is not ready yet (${attempt}/30)."
  sleep 5
done

echo "Checking whether a registered MLflow model already exists..."
MODEL_EXISTS="false"
MODEL_NAME="${ML_MODEL_NAME:-traffic-risk-model}"
MODEL_REGISTRY_URL="${MLFLOW_TRACKING_URI}/api/2.0/mlflow/registered-models/get?name=${MODEL_NAME}"
if curl --max-time 10 -fsS "${MODEL_REGISTRY_URL}" >/dev/null 2>&1; then
  MODEL_EXISTS="true"
fi

if [ "${MODEL_EXISTS}" = "true" ]; then
  echo "MLflow already has a registered model. Offline training bootstrap is skipped."
elif [ "${RUN_OFFLINE_TRAINING_ON_DEPLOY:-true}" != "true" ]; then
  echo "No registered model found, but RUN_OFFLINE_TRAINING_ON_DEPLOY is not true."
  echo "Offline training bootstrap is skipped for this deployment run."
else
  echo "No registered model found. Running offline feature engineering and H2O training."
  if ! python3 -m venv "${PROJECT_ROOT}/.venv-node1" >/dev/null 2>&1; then
    echo "python3-venv is missing. Installing it before creating the training virtual environment."
    apt-get update
    apt-get install -y python3-venv
    python3 -m venv "${PROJECT_ROOT}/.venv-node1"
  fi

  TRAINING_PYTHON="${PROJECT_ROOT}/.venv-node1/bin/python"
  "${TRAINING_PYTHON}" -m pip install --upgrade pip
  "${TRAINING_PYTHON}" -m pip install pandas numpy h2o mlflow python-dotenv gcsfs google-auth scikit-learn
  if [[ "${US_TRAIN_OFFLINE_PATH:-}" != gs://* ]]; then
    "${TRAINING_PYTHON}" ml/dataset/dataset_offline.py
  else
    echo "US_TRAIN_OFFLINE_PATH is a GCS feature CSV. Skipping local feature generation."
  fi
  "${TRAINING_PYTHON}" ml/training/h2o_before_2020.py
fi

echo "Restarting MLflow model serving after model bootstrap..."
docker compose --env-file "${ENV_FILE}" -f deployment/node1-control/docker-compose.yaml up -d mlflow-serving fastapi

echo "Node 1 services:"
docker compose --env-file "${ENV_FILE}" -f deployment/node1-control/docker-compose.yaml ps

echo "Node 1 run script completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
