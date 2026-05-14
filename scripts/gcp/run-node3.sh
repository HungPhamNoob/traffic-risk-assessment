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
NODE3_WAIT_FOR_SILVER_SECONDS="${NODE3_WAIT_FOR_SILVER_SECONDS:-600}"
NODE3_WAIT_FOR_SILVER_INTERVAL_SECONDS="${NODE3_WAIT_FOR_SILVER_INTERVAL_SECONDS:-15}"

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

configure_cloud_sdk_runtime() {
  # Some VM images have /home/<user>/.config/gcloud owned by root after startup
  # scripts run with sudo. Use a writable runtime config directory so gcloud
  # and gsutil commands can use the VM service account without touching HOME.
  export CLOUDSDK_CONFIG="${CLOUDSDK_CONFIG:-/tmp/gcloud-config-$(id -u)}"
  mkdir -p "${CLOUDSDK_CONFIG}"
  chmod 700 "${CLOUDSDK_CONFIG}"
  echo "Cloud SDK runtime config: ${CLOUDSDK_CONFIG}"
}

wait_for_silver_data() {
  # Node 2 writes Silver objects asynchronously. Node 3 must wait until at
  # least one feature object is visible before taking a local snapshot for
  # Spark, otherwise the batch job succeeds with an empty dataset.
  local silver_glob="${SILVER_FEATURES_PATH%/}/**"
  local waited_seconds=0

  echo "Waiting for Silver feature objects before running Spark."
  echo "Silver object glob: ${silver_glob}"

  while [ "${waited_seconds}" -le "${NODE3_WAIT_FOR_SILVER_SECONDS}" ]; do
    if gcloud storage ls "${silver_glob}" >/tmp/node3-silver-ls.txt 2>/tmp/node3-silver-ls.err; then
      if [ -s /tmp/node3-silver-ls.txt ]; then
        echo "Silver data is available. Sample objects:"
        head -20 /tmp/node3-silver-ls.txt
        return 0
      fi
    fi

    echo "No Silver data visible yet after ${waited_seconds}s. Waiting ${NODE3_WAIT_FOR_SILVER_INTERVAL_SECONDS}s."
    sleep "${NODE3_WAIT_FOR_SILVER_INTERVAL_SECONDS}"
    waited_seconds=$((waited_seconds + NODE3_WAIT_FOR_SILVER_INTERVAL_SECONDS))
  done

  echo "ERROR: No Silver data found after ${NODE3_WAIT_FOR_SILVER_SECONDS}s."
  echo "ERROR: Start Node 2 first and verify flink-python-job writes to ${SILVER_FEATURES_PATH}."
  if [ -s /tmp/node3-silver-ls.err ]; then
    echo "Last gcloud storage ls error:"
    cat /tmp/node3-silver-ls.err
  fi
  exit 1
}

configure_cloud_sdk_runtime

echo "Starting Spark services..."
echo "Removing stale Node 3 containers from previous Compose project names..."
docker rm -f \
  node3-spark-master \
  node3-spark-worker-1 \
  node3-spark-worker-2 \
  node3-spark-worker-3 \
  2>/dev/null || true

echo "Ensuring the shared Docker network exists before Compose starts."
docker network inspect capstone-net >/dev/null 2>&1 || docker network create capstone-net >/dev/null

docker compose --env-file "${ENV_FILE}" -f deployment/node3-batch/docker-compose.yaml up -d

echo "Waiting for Spark master to accept jobs..."
sleep 20

LOCAL_CLOUD_DATA_DIR="${LOCAL_CLOUD_DATA_DIR:-${PROJECT_ROOT}/data/cloud}"
LOCAL_SILVER_FEATURES_PATH="${LOCAL_SILVER_FEATURES_PATH:-${LOCAL_CLOUD_DATA_DIR}/silver/flink_features}"
LOCAL_GOLD_RETRAIN_PATH="${LOCAL_GOLD_RETRAIN_PATH:-${LOCAL_CLOUD_DATA_DIR}/gold/features/retrain}"
LOCAL_GOLD_RETRAIN_PARQUET_PATH="${LOCAL_GOLD_RETRAIN_PARQUET_PATH:-${LOCAL_GOLD_RETRAIN_PATH}/parquet}"
LOCAL_GOLD_RETRAIN_CSV_PATH="${LOCAL_GOLD_RETRAIN_CSV_PATH:-${LOCAL_GOLD_RETRAIN_PATH}/csv}"

wait_for_silver_data

echo "Syncing Silver data from GCS to local disk for Spark processing."
echo "GCS Silver:   ${SILVER_FEATURES_PATH}"
echo "Local Silver: ${LOCAL_SILVER_FEATURES_PATH}"
echo "Removing the previous local Silver snapshot so Spark never reads stale files."
sudo rm -rf "${LOCAL_SILVER_FEATURES_PATH}"
mkdir -p "${LOCAL_SILVER_FEATURES_PATH}" "${LOCAL_GOLD_RETRAIN_PATH}"
if ! gcloud storage rsync -r "${SILVER_FEATURES_PATH}" "${LOCAL_SILVER_FEATURES_PATH}"; then
  echo "WARNING: Silver rsync reported transient copy errors while streaming was active."
  echo "WARNING: Continuing with the files that were copied into the local snapshot."
fi

if ! find "${LOCAL_SILVER_FEATURES_PATH}" -type f | head -1 | grep -q .; then
  echo "ERROR: Local Silver snapshot is empty after rsync."
  echo "ERROR: Check Node 2 Flink logs and GCS permissions before running Node 3."
  exit 1
fi

echo "Preparing local Spark output directories with container-writable permissions."
sudo rm -rf "${LOCAL_GOLD_RETRAIN_PARQUET_PATH}" "${LOCAL_GOLD_RETRAIN_CSV_PATH}"
mkdir -p "${LOCAL_GOLD_RETRAIN_PARQUET_PATH}" "${LOCAL_GOLD_RETRAIN_CSV_PATH}"
sudo chown -R "$(id -u):$(id -g)" "${LOCAL_CLOUD_DATA_DIR}"
sudo chmod -R a+rwX "${LOCAL_CLOUD_DATA_DIR}"

echo "Running Spark silver-to-gold job once. Existing checkpoints/data are preserved."
docker compose --env-file "${ENV_FILE}" -f deployment/node3-batch/docker-compose.yaml exec -T --user root spark-master \
  sh -c 'mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars && chown -R spark:spark /home/spark/.ivy2'

docker compose --env-file "${ENV_FILE}" -f deployment/node3-batch/docker-compose.yaml exec -T spark-master \
  env \
  SILVER_FEATURES_PATH=/data/cloud/silver/flink_features \
  GOLD_RETRAIN_PATH=/data/cloud/gold/features/retrain \
  GOLD_RETRAIN_PARQUET_PATH=/data/cloud/gold/features/retrain/parquet \
  GOLD_RETRAIN_CSV_PATH=/data/cloud/gold/features/retrain/csv \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/traffic/processing/spark_batch.py

echo "Syncing Gold Parquet and CSV outputs back to GCS."
gcloud storage rsync -r "${LOCAL_GOLD_RETRAIN_PARQUET_PATH}" "${GOLD_RETRAIN_PARQUET_PATH}"
gcloud storage rsync -r "${LOCAL_GOLD_RETRAIN_CSV_PATH}" "${GOLD_RETRAIN_CSV_PATH}"

echo "Running online H2O retraining once from the latest gold data."
if ! command -v java >/dev/null 2>&1; then
  echo "Java is not installed. Installing OpenJDK 17 because H2O cannot start without a JVM."
  sudo apt-get update
  sudo apt-get install -y openjdk-17-jre-headless
fi

echo "Ensuring python3-venv is installed before creating the retraining environment."
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y python3-venv

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
H2O_MAX_RUNTIME="${NODE3_H2O_MAX_RUNTIME:-${H2O_MAX_RUNTIME:-600}}" \
  RETRAIN_DATA_PATH="${LOCAL_GOLD_RETRAIN_CSV_PATH}" \
  "${RETRAINING_PYTHON}" ml/training/h2o_after_2020.py

echo "Node 3 services:"
docker compose --env-file "${ENV_FILE}" -f deployment/node3-batch/docker-compose.yaml ps

echo "Node 3 run script completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
