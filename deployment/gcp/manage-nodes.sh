#!/bin/bash
# manage-nodes.sh - Auto start/stop Node2 & Node3 based on Airflow DAG triggers

set -e

PROJECT_ID=${GCP_PROJECT_ID:-capstone-team4}
ZONE=us-central1-a

start_node() {
  local node=$1
  echo "🚀 Starting $node..."
  gcloud compute instances start $node --project=$PROJECT_ID --zone=$ZONE
  # Wait for VM to be ready
  sleep 30
  echo "✅ $node started"
}

stop_node() {
  local node=$1
  echo "🛑 Stopping $node..."
  gcloud compute instances stop $node --project=$PROJECT_ID --zone=$ZONE
  echo "✅ $node stopped"
}

case "$1" in
  start)
    case "$2" in
      node2) start_node "node2-streaming" ;;
      node3) start_node "node3-batch" ;;
      *) echo "❌ Unknown node: $2"; exit 1 ;;
    esac
    ;;
  stop)
    case "$2" in
      node2) stop_node "node2-streaming" ;;
      node3) stop_node "node3-batch" ;;
      all)
        stop_node "node2-streaming"
        stop_node "node3-batch"
        ;;
      *) echo "❌ Unknown node: $2"; exit 1 ;;
    esac
    ;;
  status)
    echo "📊 Node status:"
    gcloud compute instances list --project=$PROJECT_ID --filter="labels.team:capstone4" --format="table(name,status,zone)"
    ;;
  *)
    echo "Usage: $0 {start|stop} {node2|node3|all}"
    echo "       $0 status"
    exit 1
    ;;
esac