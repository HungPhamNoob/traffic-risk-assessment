#!/bin/bash
# scripts/gcp/manage-nodes.sh - start, stop, or inspect GCP VMs on demand.

set -e

PROJECT_ID="${GCP_PROJECT_ID:-big-data-group-4}"
ZONE="${GCP_ZONE:-us-central1-a}"
NODES=("node1-control" "node2-streaming" "node3-batch")

usage() {
    echo "Usage: $0 {start|stop|status} [node-name]"
    echo "  node-name: node1-control | node2-streaming | node3-batch | all"
    exit 1
}

start_node() {
    local node=$1
    echo "Starting $node..."
    gcloud compute instances start $node --zone=$ZONE --project=$PROJECT_ID
    
    # Wait for SSH to be available
    echo "Waiting for SSH..."
    gcloud compute ssh $node --zone=$ZONE --project=$PROJECT_ID \
        --command="echo '$node is ready'" --quiet || true
}

stop_node() {
    local node=$1
    echo "Stopping $node..."
    gcloud compute instances stop $node --zone=$ZONE --project=$PROJECT_ID
}

status_node() {
    local node=$1
    echo "Status of $node:"
    gcloud compute instances describe $node --zone=$ZONE --project=$PROJECT_ID \
        --format="table(status, scheduling.preemptible, networkInterfaces[0].networkIP)"
}

case "$1" in
    start)
        if [[ "$2" == "all" || -z "$2" ]]; then
            for node in "${NODES[@]}"; do start_node $node; done
        else
            start_node "$2"
        fi
        ;;
    stop)
        if [[ "$2" == "all" || -z "$2" ]]; then
            # Never stop node1 (control plane) by default
            for node in node2-streaming node3-batch; do stop_node $node; done
        else
            stop_node "$2"
        fi
        ;;
    status)
        if [[ "$2" == "all" || -z "$2" ]]; then
            for node in "${NODES[@]}"; do status_node $node; done
        else
            status_node "$2"
        fi
        ;;
    *)
        usage
        ;;
esac

echo "Done."
