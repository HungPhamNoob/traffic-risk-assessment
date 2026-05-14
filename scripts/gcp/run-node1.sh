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
IS_TRAIN_OFFLINE="${IS_TRAIN_OFFLINE:-false}"
echo "Node 1 MLflow tracking URI: ${MLFLOW_TRACKING_URI}"
echo "IS_TRAIN_OFFLINE: ${IS_TRAIN_OFFLINE}"

NODE1_CANONICAL_NAME_PATTERN='^node1-(postgres|airflow-db|airflow|blackbox-exporter|prometheus|grafana|mlflow|mlflow-serving|fastapi)$'
NODE1_TRANSIENT_NAME_PATTERN='^.+_node1-(postgres|airflow-db|airflow|blackbox-exporter|prometheus|grafana|mlflow|mlflow-serving|fastapi)$'

remove_matching_node1_containers() {
  # Remove containers that match the supplied pattern. The caller decides whether
  # canonical service names or Compose-generated transient names should be cleaned up.
  local container_name_pattern="$1"
  local stale_containers=()

  while IFS= read -r container_name; do
    if [ -n "${container_name}" ]; then
      stale_containers+=("${container_name}")
    fi
  done < <(docker ps -a --format '{{.Names}}' | grep -E "${container_name_pattern}" || true)

  if [ "${#stale_containers[@]}" -eq 0 ]; then
    echo "No stale Node 1 containers were found."
    return 0
  fi

  echo "Removing stale Node 1 containers detected from previous Compose reconciliations:"
  printf '  - %s\n' "${stale_containers[@]}"
  docker rm -f "${stale_containers[@]}" >/dev/null
}

echo "Checking host dependencies required for offline H2O training."
if ! command -v java >/dev/null 2>&1; then
  echo "Java is not installed. Installing OpenJDK 17 because H2O cannot start without a JVM."
  sudo apt-get update
  sudo apt-get install -y openjdk-17-jre-headless
fi

if ! python3 -m venv --help >/dev/null 2>&1; then
  echo "python3-venv is not installed. Installing it before creating training environments."
  sudo apt-get update
  sudo apt-get install -y python3-venv
fi

echo "Starting Node 1 Docker services..."
echo "Preparing writable runtime directories for containers."
mkdir -p orchestration/logs ml/mlruns
chown -R 50000:0 orchestration/logs 2>/dev/null || true
chmod -R g+rwX orchestration/logs 2>/dev/null || true

echo "Removing stale Node 1 containers from previous Compose project names..."
remove_matching_node1_containers "${NODE1_CANONICAL_NAME_PATTERN}"
remove_matching_node1_containers "${NODE1_TRANSIENT_NAME_PATTERN}"

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

run_offline_training() {
  echo "Running offline feature engineering and H2O training."
  python3 -m venv "${PROJECT_ROOT}/.venv-node1"

  TRAINING_PYTHON="${PROJECT_ROOT}/.venv-node1/bin/python"
  "${TRAINING_PYTHON}" -m pip install --upgrade pip
  "${TRAINING_PYTHON}" -m pip install \
    h2o==3.46.0.6 \
    mlflow==2.12.1 \
    pandas==2.2.2 \
    numpy==1.26.4 \
    scikit-learn==1.4.2 \
    python-dotenv==1.0.1 \
    gcsfs==2024.3.1 \
    "google-auth>=2.23.0" \
    "google-cloud-storage>=2.14.0"
  if [[ "${US_TRAIN_OFFLINE_PATH:-}" != gs://* ]]; then
    "${TRAINING_PYTHON}" ml/dataset/dataset_offline.py
  else
    echo "US_TRAIN_OFFLINE_PATH is a GCS feature CSV. Skipping local feature generation."
  fi
  "${TRAINING_PYTHON}" ml/training/h2o_before_2020.py
}

if [ "${IS_TRAIN_OFFLINE}" = "true" ]; then
  echo "IS_TRAIN_OFFLINE=true. Forcing offline feature engineering and H2O training."
  run_offline_training
elif [ "${MODEL_EXISTS}" = "true" ]; then
  echo "MLflow already has a registered model. Offline training bootstrap is skipped."
else
  echo "No registered model found in MLflow. Offline training is required."
  run_offline_training
fi

echo "Restarting MLflow model serving after model bootstrap..."
echo "Removing any transient Node 1 containers that could block the serving restart."
remove_matching_node1_containers "${NODE1_TRANSIENT_NAME_PATTERN}"
docker compose --env-file "${ENV_FILE}" -f deployment/node1-control/docker-compose.yaml up -d mlflow-serving fastapi

echo "Node 1 services:"
docker compose --env-file "${ENV_FILE}" -f deployment/node1-control/docker-compose.yaml ps

echo "Node 1 run script completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
