#!/bin/bash
# Reset and run the complete cloud pipeline from the beginning.
#
# This script is intentionally cloud-first. It does not start the full stack on
# a local laptop. It resets realtime state, trains or verifies the pre-2020
# baseline model, starts US replay plus TomTom live streaming, runs Spark/H2O
# retraining, and then collects measured evidence.

set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-big-data-group-4}"
ZONE="${GCP_ZONE:-us-central1-a}"
NODE1="${NODE1:-node1-control}"
NODE2="${NODE2:-node2-streaming}"
NODE3="${NODE3:-node3-batch}"
BRANCH="${BRANCH:-main}"
PROJECT_ROOT="${PROJECT_ROOT:-/opt/traffic}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
LOG_DIR="${LOG_DIR:-logs/cloud_runs/${RUN_ID}}"
RUN_OFFLINE_TRAINING="${RUN_OFFLINE_TRAINING:-true}"
STREAM_MAX_RECORDS="${STREAM_MAX_RECORDS:-0}"
STREAM_THROTTLE_SECONDS="${STREAM_THROTTLE_SECONDS:-0.0}"
NODE3_WAIT_FOR_SILVER_SECONDS="${NODE3_WAIT_FOR_SILVER_SECONDS:-900}"

mkdir -p "${LOG_DIR}"

ssh_cmd() {
  local node="$1"
  shift
  gcloud compute ssh "${node}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --quiet \
    --command="$*"
}

sync_repo_on_node() {
  local node="$1"
  echo "Syncing ${node} to origin/${BRANCH}."
  ssh_cmd "${node}" "
    set -euo pipefail
    sudo chown -R \$(whoami):\$(whoami) ${PROJECT_ROOT} 2>/dev/null || true
    git config --global --add safe.directory ${PROJECT_ROOT} 2>/dev/null || true
    if [ ! -d ${PROJECT_ROOT}/.git ]; then
      sudo rm -rf ${PROJECT_ROOT}
      sudo mkdir -p ${PROJECT_ROOT}
      sudo chown -R \$(whoami):\$(whoami) ${PROJECT_ROOT}
      git clone https://github.com/HungPhamNoob/traffic-risk-assessment.git ${PROJECT_ROOT}
    fi
    cd ${PROJECT_ROOT}
    git reset --hard || true
    git clean -fd -e data/ -e logs/ -e .venv-node1/ -e .venv-node3/ -e dashboard/frontend/node_modules/ -e dashboard/frontend/.next/ || true
    git fetch --prune origin
    git checkout -B ${BRANCH} origin/${BRANCH}
    git reset --hard origin/${BRANCH}
    git clean -fd -e data/ -e logs/ -e .venv-node1/ -e .venv-node3/ -e dashboard/frontend/node_modules/ -e dashboard/frontend/.next/
  "
}

echo "Run ID: ${RUN_ID}"
echo "Project: ${PROJECT_ID}, zone: ${ZONE}, branch: ${BRANCH}"
echo "Logs: ${LOG_DIR}"

echo "Uploading .env.cloud to GCS."
gcloud storage cp .env.cloud gs://big-data-group-4-bronze/env/.env.cloud \
  | tee "${LOG_DIR}/00-sync-env.log"

sync_repo_on_node "${NODE1}" | tee "${LOG_DIR}/01-sync-node1.log"
sync_repo_on_node "${NODE2}" | tee "${LOG_DIR}/02-sync-node2.log"
sync_repo_on_node "${NODE3}" | tee "${LOG_DIR}/03-sync-node3.log"

echo "Copying .env.cloud from GCS to each VM."
for node in "${NODE1}" "${NODE2}" "${NODE3}"; do
  ssh_cmd "${node}" "
    cd ${PROJECT_ROOT}
    export CLOUDSDK_CONFIG=/tmp/gcloud-config-\$(id -u)
    mkdir -p \"\${CLOUDSDK_CONFIG}\"
    chmod 700 \"\${CLOUDSDK_CONFIG}\"
    gcloud storage cp gs://big-data-group-4-bronze/env/.env.cloud .env.cloud
    cp .env.cloud .env
  " | tee "${LOG_DIR}/04-env-${node}.log"
done

echo "Resetting Node 1 PostgreSQL serving tables."
ssh_cmd "${NODE1}" "cd ${PROJECT_ROOT} && RESET_LOCAL_COMPOSE=false RESET_POSTGRES=true RESET_GCS=true bash scripts/gcp/reset-realtime.sh" \
  | tee "${LOG_DIR}/05-reset-node1.log"

echo "Resetting Node 2 Kafka/Flink streaming state."
ssh_cmd "${NODE2}" "cd ${PROJECT_ROOT} && RESET_LOCAL_COMPOSE=true RESET_POSTGRES=false RESET_GCS=false bash scripts/gcp/reset-realtime.sh" \
  | tee "${LOG_DIR}/06-reset-node2.log"

echo "Resetting Node 3 Spark batch state."
ssh_cmd "${NODE3}" "cd ${PROJECT_ROOT} && RESET_LOCAL_COMPOSE=true RESET_POSTGRES=false RESET_GCS=false bash scripts/gcp/reset-realtime.sh" \
  | tee "${LOG_DIR}/07-reset-node3.log"

echo "Starting Node 1 and bootstrapping the pre-2020 H2O model."
ssh_cmd "${NODE1}" "
  cd ${PROJECT_ROOT}
  IS_TRAIN_OFFLINE=${RUN_OFFLINE_TRAINING} bash scripts/gcp/run-node1.sh
" | tee "${LOG_DIR}/08-run-node1.log"

echo "Starting Node 2 realtime streams."
ssh_cmd "${NODE2}" "
  cd ${PROJECT_ROOT}
  STREAM_MAX_RECORDS=${STREAM_MAX_RECORDS} STREAM_THROTTLE_SECONDS=${STREAM_THROTTLE_SECONDS} bash scripts/gcp/run-node2.sh
" | tee "${LOG_DIR}/09-run-node2.log"

echo "Starting Node 3 Spark and H2O retraining from generated Silver data."
ssh_cmd "${NODE3}" "
  cd ${PROJECT_ROOT}
  NODE3_WAIT_FOR_SILVER_SECONDS=${NODE3_WAIT_FOR_SILVER_SECONDS} bash scripts/gcp/run-node3.sh
" | tee "${LOG_DIR}/10-run-node3.log"

echo "Collecting measured cloud evidence."
RUN_ID="${RUN_ID}" LOG_DIR="logs/cloud_runs" \
  bash scripts/gcp/collect-cloud-metrics.sh | tee "${LOG_DIR}/11-metrics.log"

echo "Full cloud run completed. Evidence is under ${LOG_DIR}."
