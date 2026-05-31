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
NODE3_COMPOSE_FILE="${PROJECT_ROOT}/deployment/node3-batch/docker-compose.yaml"
NODE3_COMPOSE_DIR="$(dirname "${NODE3_COMPOSE_FILE}")"
NODE3_WAIT_FOR_SILVER_SECONDS="${NODE3_WAIT_FOR_SILVER_SECONDS:-600}"
NODE3_WAIT_FOR_SILVER_INTERVAL_SECONDS="${NODE3_WAIT_FOR_SILVER_INTERVAL_SECONDS:-15}"
NODE3_MIN_SILVER_OBJECTS="${NODE3_MIN_SILVER_OBJECTS:-100}"
APT_CACHE_UPDATED=0
NODE3_TEMP_DIR="$(mktemp -d /tmp/node3-run-XXXXXX)"
NODE3_SILVER_LS_STDOUT="${NODE3_TEMP_DIR}/silver-ls.txt"
NODE3_SILVER_LS_STDERR="${NODE3_TEMP_DIR}/silver-ls.err"

cleanup_node3_temp() {
  rm -rf "${NODE3_TEMP_DIR}" 2>/dev/null || true
}
trap cleanup_node3_temp EXIT

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

apt_install_if_missing() {
  if [ "${APT_CACHE_UPDATED}" -eq 0 ]; then
    sudo apt-get update
    APT_CACHE_UPDATED=1
  fi
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y "$@"
}

ensure_command() {
  local command_name="$1"
  local package_name="$2"
  if command -v "${command_name}" >/dev/null 2>&1; then
    return 0
  fi
  echo "Installing missing dependency for '${command_name}': ${package_name}"
  apt_install_if_missing "${package_name}"
}

ensure_docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    return 0
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    return 0
  fi
  echo "Installing missing dependency for 'docker compose': docker-compose-plugin"
  apt_install_if_missing docker-compose-plugin
}

ensure_gcloud_cli() {
  if command -v gcloud >/dev/null 2>&1; then
    return 0
  fi
  echo "Installing missing dependency for 'gcloud': google-cloud-cli tarball"
  local installer_tgz="/tmp/google-cloud-cli-460.0.0-linux-x86_64.tar.gz"
  curl -fsSL "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-460.0.0-linux-x86_64.tar.gz" -o "${installer_tgz}"
  rm -rf "${HOME}/google-cloud-sdk"
  tar -xf "${installer_tgz}" -C "${HOME}"
  "${HOME}/google-cloud-sdk/install.sh" --quiet
  export PATH="${PATH}:${HOME}/google-cloud-sdk/bin"
}

compose_cmd() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
    return
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    docker-compose "$@"
    return
  fi
  ensure_docker_compose
  docker compose "$@"
}

echo "Checking host dependencies required for Node 3 services."
ensure_command docker docker.io
ensure_docker_compose
ensure_command curl curl
ensure_gcloud_cli
ensure_command java openjdk-17-jre-headless
ensure_command python3 python3
if ! python3 -m venv --help >/dev/null 2>&1; then
  echo "Installing missing dependency for Python virtual environments: python3-venv"
  apt_install_if_missing python3-venv
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
    if gcloud storage ls "${silver_glob}" >"${NODE3_SILVER_LS_STDOUT}" 2>"${NODE3_SILVER_LS_STDERR}"; then
      if [ -s "${NODE3_SILVER_LS_STDOUT}" ]; then
        local object_count
        object_count="$(wc -l < "${NODE3_SILVER_LS_STDOUT}" | tr -d ' ')"
        if [ "${object_count}" -ge "${NODE3_MIN_SILVER_OBJECTS}" ]; then
          echo "Silver data is available with ${object_count} objects. Sample objects:"
          head -20 "${NODE3_SILVER_LS_STDOUT}"
          return 0
        fi
        echo "Silver data exists but only ${object_count} objects are visible. Waiting for at least ${NODE3_MIN_SILVER_OBJECTS} objects."
      fi
    fi

    echo "No Silver data visible yet after ${waited_seconds}s. Waiting ${NODE3_WAIT_FOR_SILVER_INTERVAL_SECONDS}s."
    sleep "${NODE3_WAIT_FOR_SILVER_INTERVAL_SECONDS}"
    waited_seconds=$((waited_seconds + NODE3_WAIT_FOR_SILVER_INTERVAL_SECONDS))
  done

  echo "ERROR: No Silver data found after ${NODE3_WAIT_FOR_SILVER_SECONDS}s."
  echo "ERROR: Start Node 2 first and verify flink-python-job writes to ${SILVER_FEATURES_PATH}."
  if [ -s "${NODE3_SILVER_LS_STDERR}" ]; then
    echo "Last gcloud storage ls error:"
    cat "${NODE3_SILVER_LS_STDERR}"
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

compose_cmd \
  --project-directory "${NODE3_COMPOSE_DIR}" \
  --env-file "${ENV_FILE}" \
  -f "${NODE3_COMPOSE_FILE}" \
  up -d

echo "Verifying that the Spark master container is mounted from ${PROJECT_ROOT}."
SPARK_MOUNT_SOURCE="$(docker inspect node3-spark-master --format '{{range .Mounts}}{{if eq .Destination "/opt/traffic"}}{{.Source}}{{end}}{{end}}' 2>/dev/null || true)"
if [ "${SPARK_MOUNT_SOURCE}" != "${PROJECT_ROOT}" ]; then
  echo "ERROR: node3-spark-master is mounted from '${SPARK_MOUNT_SOURCE}', expected '${PROJECT_ROOT}'."
  echo "ERROR: Aborting so the operator does not accidentally run an outdated checkout."
  exit 1
fi

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

LOCAL_SILVER_SAMPLE_FILE="$(find "${LOCAL_SILVER_FEATURES_PATH}" -type f -print -quit)"
if [ -z "${LOCAL_SILVER_SAMPLE_FILE}" ]; then
  echo "ERROR: Local Silver snapshot is empty after rsync."
  echo "ERROR: Check Node 2 Flink logs and GCS permissions before running Node 3."
  exit 1
fi
echo "Local Silver snapshot is ready. Sample file: ${LOCAL_SILVER_SAMPLE_FILE}"

echo "Preparing local Spark output directories with container-writable permissions."
sudo rm -rf "${LOCAL_GOLD_RETRAIN_PARQUET_PATH}" "${LOCAL_GOLD_RETRAIN_CSV_PATH}"
mkdir -p "${LOCAL_GOLD_RETRAIN_PARQUET_PATH}" "${LOCAL_GOLD_RETRAIN_CSV_PATH}"
sudo chown -R "$(id -u):$(id -g)" "${LOCAL_CLOUD_DATA_DIR}"
sudo chmod -R a+rwX "${LOCAL_CLOUD_DATA_DIR}"

echo "Running Spark silver-to-gold job once. Existing checkpoints/data are preserved."
compose_cmd \
  --project-directory "${NODE3_COMPOSE_DIR}" \
  --env-file "${ENV_FILE}" \
  -f "${NODE3_COMPOSE_FILE}" \
  exec -T --user root spark-master \
  sh -c 'mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars && chown -R spark:spark /home/spark/.ivy2'

compose_cmd \
  --project-directory "${NODE3_COMPOSE_DIR}" \
  --env-file "${ENV_FILE}" \
  -f "${NODE3_COMPOSE_FILE}" \
  exec -T spark-master \
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
RETRAINING_VENV="${PROJECT_ROOT}/.venv-node3"
RETRAINING_PYTHON="${RETRAINING_VENV}/bin/python"
if [ ! -x "${RETRAINING_PYTHON}" ]; then
  python3 -m venv "${RETRAINING_VENV}"
fi

MISSING_RETRAIN_SPECS=()
if ! "${RETRAINING_PYTHON}" -c "import h2o" >/dev/null 2>&1; then
  MISSING_RETRAIN_SPECS+=("h2o==3.46.0.6")
fi
if ! "${RETRAINING_PYTHON}" -c "import mlflow" >/dev/null 2>&1; then
  MISSING_RETRAIN_SPECS+=("mlflow==2.12.1")
fi
if ! "${RETRAINING_PYTHON}" -c "import pandas" >/dev/null 2>&1; then
  MISSING_RETRAIN_SPECS+=("pandas==2.2.2")
fi
if ! "${RETRAINING_PYTHON}" -c "import numpy" >/dev/null 2>&1; then
  MISSING_RETRAIN_SPECS+=("numpy==1.26.4")
fi
if ! "${RETRAINING_PYTHON}" -c "import sklearn" >/dev/null 2>&1; then
  MISSING_RETRAIN_SPECS+=("scikit-learn==1.4.2")
fi
if ! "${RETRAINING_PYTHON}" -c "import pyarrow" >/dev/null 2>&1; then
  MISSING_RETRAIN_SPECS+=("pyarrow==11.0.0")
fi
if ! "${RETRAINING_PYTHON}" -c "import dotenv" >/dev/null 2>&1; then
  MISSING_RETRAIN_SPECS+=("python-dotenv==1.0.1")
fi
if ! "${RETRAINING_PYTHON}" -c "import gcsfs" >/dev/null 2>&1; then
  MISSING_RETRAIN_SPECS+=("gcsfs==2024.3.1")
fi
if ! "${RETRAINING_PYTHON}" -c "import google.auth" >/dev/null 2>&1; then
  MISSING_RETRAIN_SPECS+=("google-auth>=2.23.0")
fi
if ! "${RETRAINING_PYTHON}" -c "import google.cloud.storage" >/dev/null 2>&1; then
  MISSING_RETRAIN_SPECS+=("google-cloud-storage>=2.14.0")
fi

if [ "${#MISSING_RETRAIN_SPECS[@]}" -gt 0 ]; then
  echo "Installing missing Python retraining dependencies into ${RETRAINING_VENV}."
  "${RETRAINING_PYTHON}" -m pip install --upgrade pip
  "${RETRAINING_PYTHON}" -m pip install "${MISSING_RETRAIN_SPECS[@]}"
else
  echo "Python retraining dependencies already exist in ${RETRAINING_VENV}."
fi

H2O_MAX_RUNTIME="${NODE3_H2O_MAX_RUNTIME:-${H2O_MAX_RUNTIME:-600}}" \
  RETRAIN_DATA_PATH="${LOCAL_GOLD_RETRAIN_CSV_PATH}" \
  "${RETRAINING_PYTHON}" ml/training/h2o_after_2020.py

echo "Node 3 services:"
compose_cmd \
  --project-directory "${NODE3_COMPOSE_DIR}" \
  --env-file "${ENV_FILE}" \
  -f "${NODE3_COMPOSE_FILE}" \
  ps

echo "Node 3 run script completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
