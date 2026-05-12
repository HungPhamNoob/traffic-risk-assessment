#!/bin/bash
# Start, stop, or inspect GCP VMs on demand.
#
# Node 2 and Node 3 are intentionally treated as one synchronized pair. Asking
# for either node starts or stops both so replay state cannot drift.

set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-big-data-group-4}"
ZONE="${GCP_ZONE:-us-central1-a}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NODES=("node1-control" "node2-streaming" "node3-batch")

usage() {
  echo "Usage: $0 {start|stop|status} [node-name]"
  echo "  node-name: node1-control | node2-streaming | node3-batch | pair | all"
  exit 1
}

start_node() {
  local node_name="$1"
  echo "Starting ${node_name}."
  gcloud compute instances start "${node_name}" --zone="${ZONE}" --project="${PROJECT_ID}"

  echo "Waiting for SSH on ${node_name}."
  gcloud compute ssh "${node_name}" --zone="${ZONE}" --project="${PROJECT_ID}" \
    --command="echo '${node_name} is ready'" --quiet || true
}

stop_node() {
  local node_name="$1"
  echo "Stopping ${node_name}."
  gcloud compute instances stop "${node_name}" --zone="${ZONE}" --project="${PROJECT_ID}"
}

status_node() {
  local node_name="$1"
  echo "Status of ${node_name}:"
  gcloud compute instances describe "${node_name}" --zone="${ZONE}" --project="${PROJECT_ID}" \
    --format="table(status, scheduling.preemptible, networkInterfaces[0].networkIP)"
}

start_pair() {
  bash "${SCRIPT_DIR}/node23-lifecycle.sh" start
}

stop_pair() {
  bash "${SCRIPT_DIR}/node23-lifecycle.sh" stop
}

ACTION="${1:-}"
TARGET="${2:-all}"

case "${ACTION}" in
  start)
    case "${TARGET}" in
      node1-control)
        start_node "node1-control"
        ;;
      node2-streaming|node3-batch|pair)
        start_pair
        ;;
      all)
        start_node "node1-control"
        start_pair
        ;;
      *)
        usage
        ;;
    esac
    ;;
  stop)
    case "${TARGET}" in
      node1-control)
        stop_node "node1-control"
        ;;
      node2-streaming|node3-batch|pair|all)
        stop_pair
        ;;
      *)
        usage
        ;;
    esac
    ;;
  status)
    case "${TARGET}" in
      node2-streaming|node3-batch|pair)
        status_node "node2-streaming"
        status_node "node3-batch"
        ;;
      all)
        for node_name in "${NODES[@]}"; do
          status_node "${node_name}"
        done
        ;;
      node1-control)
        status_node "node1-control"
        ;;
      *)
        usage
        ;;
    esac
    ;;
  *)
    usage
    ;;
esac

echo "Done."
