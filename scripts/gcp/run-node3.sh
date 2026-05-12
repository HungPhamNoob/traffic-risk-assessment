#!/bin/bash
# Start Node 3 batch services and run the hourly retraining input job once.
#
# Node 3 responsibilities:
#   - Spark master and worker.
#   - Silver-to-gold batch processing for replay data from 2020 onward.
#   - H2O online retraining against the latest gold retrain dataset.

set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/opt/traffic}"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env.cloud}"

echo "Node 3 run script started at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
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

echo "Starting Spark services..."
docker compose --env-file "${ENV_FILE}" -f deployment/node3-batch/docker-compose.yaml up -d

echo "Waiting for Spark master to accept jobs..."
sleep 20

echo "Running Spark silver-to-gold job once. Existing checkpoints/data are preserved."
docker compose --env-file "${ENV_FILE}" -f deployment/node3-batch/docker-compose.yaml exec -T spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/traffic/processing/spark_batch.py

echo "Running online H2O retraining once from the latest gold data."
if ! command -v java >/dev/null 2>&1; then
  echo "Java is not installed. Installing OpenJDK 17 because H2O cannot start without a JVM."
  sudo apt-get update
  sudo apt-get install -y openjdk-17-jre-headless
fi

if ! python3 -m venv --help >/dev/null 2>&1; then
  echo "python3-venv is not installed. Installing it before creating retraining environments."
  sudo apt-get update
  sudo apt-get install -y python3-venv
fi

python3 -m venv "${PROJECT_ROOT}/.venv-node3"
RETRAINING_PYTHON="${PROJECT_ROOT}/.venv-node3/bin/python"
"${RETRAINING_PYTHON}" -m pip install --upgrade pip
"${RETRAINING_PYTHON}" -m pip install \
  h2o==3.46.0.6 \
  mlflow==2.12.1 \
  pandas==2.2.2 \
  numpy==1.26.4 \
  scikit-learn==1.4.2 \
  pyarrow==11.0.0 \
  python-dotenv==1.0.1 \
  gcsfs==2024.3.1 \
  "google-auth>=2.23.0" \
  "google-cloud-storage>=2.14.0"
"${RETRAINING_PYTHON}" ml/training/h2o_after_2020.py

echo "Node 3 services:"
docker compose --env-file "${ENV_FILE}" -f deployment/node3-batch/docker-compose.yaml ps

echo "Node 3 run script completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
