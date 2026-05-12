#!/bin/bash
# Manage Node 2 and Node 3 as one synchronized realtime pair.
#
# Why this script exists:
#   - The streaming branch on Node 2 and the batch/retraining branch on Node 3
#     consume the same replay timeline.
#   - Restarting only one side can leave checkpoints, replay offsets, or Gold
#     outputs out of sync.
#   - Operators and Airflow therefore need one entrypoint that can start,
#     stop, restart, and reset the pair as a single unit.

set -euo pipefail

ACTION="${1:-}"
PROJECT_ID="${GCP_PROJECT_ID:-big-data-group-4}"
ZONE="${GCP_ZONE:-us-central1-a}"
PROJECT_ROOT="${PROJECT_ROOT:-/opt/traffic}"
NODE2_NAME="${NODE2_NAME:-node2-streaming}"
NODE3_NAME="${NODE3_NAME:-node3-batch}"

usage() {
  echo "Usage: $0 {start|stop|restart|reset}"
  exit 1
}

require_action() {
  if [[ -z "${ACTION}" ]]; then
    usage
  fi
}

wait_for_ssh() {
  local node_name="$1"

  echo "Waiting for SSH on ${node_name}."
  for attempt in $(seq 1 24); do
    if gcloud compute ssh "${node_name}" \
      --zone="${ZONE}" \
      --project="${PROJECT_ID}" \
      --quiet \
      --command="echo SSH_READY_${node_name}" >/dev/null 2>&1; then
      echo "SSH is ready on ${node_name}."
      return 0
    fi
    echo "SSH not ready on ${node_name} (${attempt}/24)."
    sleep 10
  done

  echo "ERROR: SSH did not become ready on ${node_name}."
  return 1
}

start_instance() {
  local node_name="$1"

  echo "Starting VM ${node_name}."
  gcloud compute instances start "${node_name}" \
    --zone="${ZONE}" \
    --project="${PROJECT_ID}" \
    --quiet >/dev/null
}

stop_instance() {
  local node_name="$1"

  echo "Stopping VM ${node_name}."
  gcloud compute instances stop "${node_name}" \
    --zone="${ZONE}" \
    --project="${PROJECT_ID}" \
    --quiet >/dev/null || true
}

run_remote_script() {
  local node_name="$1"
  local remote_script="$2"

  echo "Running ${remote_script} on ${node_name}."
  gcloud compute ssh "${node_name}" \
    --zone="${ZONE}" \
    --project="${PROJECT_ID}" \
    --quiet \
    --command="cd ${PROJECT_ROOT} && bash ${remote_script}"
}

stop_remote_services() {
  local node_name="$1"
  local compose_file="$2"

  echo "Stopping Docker Compose services on ${node_name}."
  gcloud compute ssh "${node_name}" \
    --zone="${ZONE}" \
    --project="${PROJECT_ID}" \
    --quiet \
    --command="
      set -euo pipefail
      cd ${PROJECT_ROOT}
      if [ -f .env.cloud ]; then
        docker compose --env-file .env.cloud -f ${compose_file} down --remove-orphans || true
      else
        echo 'Skipping docker compose down because .env.cloud is missing.'
      fi
    " >/dev/null 2>&1 || true
}

start_pair() {
  start_instance "${NODE2_NAME}"
  start_instance "${NODE3_NAME}"

  wait_for_ssh "${NODE2_NAME}"
  wait_for_ssh "${NODE3_NAME}"

  run_remote_script "${NODE2_NAME}" "scripts/gcp/run-node2.sh"
  run_remote_script "${NODE3_NAME}" "scripts/gcp/run-node3.sh"
}

stop_pair() {
  stop_remote_services "${NODE3_NAME}" "deployment/node3-batch/docker-compose.yaml"
  stop_remote_services "${NODE2_NAME}" "deployment/node2-streaming/docker-compose.yaml"

  stop_instance "${NODE3_NAME}"
  stop_instance "${NODE2_NAME}"
}

reset_pair() {
  echo "Resetting replay state for the synchronized pair."
  cd "${PROJECT_ROOT}"
  bash scripts/gcp/reset-realtime.sh
}

require_action

case "${ACTION}" in
  start)
    start_pair
    ;;
  stop)
    stop_pair
    ;;
  restart)
    stop_pair
    start_pair
    ;;
  reset)
    reset_pair
    ;;
  *)
    usage
    ;;
esac
