#!/bin/bash
# Start Node 2 and Node 3 together after a Spot VM failure or checkpoint reset.
#
# Run this from Node 1. It keeps the two realtime branches synchronized by
# restarting the streaming and batch nodes as one operational unit.

set -euo pipefail

ZONE="${GCP_ZONE:-us-central1-a}"
PROJECT_ID="${GCP_PROJECT_ID:-big-data-group-4}"

echo "Starting Node 2 and Node 3 as a synchronized pair..."

gcloud compute ssh node2-streaming \
  --zone="${ZONE}" \
  --project="${PROJECT_ID}" \
  --quiet \
  --command="cd /opt/traffic && bash scripts/gcp/run-node2.sh"

gcloud compute ssh node3-batch \
  --zone="${ZONE}" \
  --project="${PROJECT_ID}" \
  --quiet \
  --command="cd /opt/traffic && bash scripts/gcp/run-node3.sh"

echo "Node 2 and Node 3 synchronized start completed."
