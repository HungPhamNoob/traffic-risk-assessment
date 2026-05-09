#!/bin/bash
set -e

# Config
PROJECT_ID=${GCP_PROJECT_ID:-capstone-team4}
REGION=us-central1
ZONE=us-central1-a
IMAGE_FAMILY=debian-11
IMAGE_PROJECT=debian-cloud

# Node 1: Control (always-on, e2-medium)
echo "🔄 Creating Node1 (Control)..."
gcloud compute instances create node1-control \
  --project=$PROJECT_ID \
  --zone=$ZONE \
  --machine-type=e2-medium \
  --network-interface=network-tier=PREMIUM,subnet=default,no-address \
  --maintenance-policy=MIGRATE \
  --provisioning-model=STANDARD \
  --boot-disk-size=20GB \
  --boot-disk-type=pd-balanced \
  --boot-disk-image-project=$IMAGE_PROJECT \
  --boot-disk-image-family=$IMAGE_FAMILY \
  --labels=role=control,team=capstone4 \
  --metadata-from-file=startup-script=deployment/node1-control/setup.sh

# Node 2: Streaming (preemptible, e2-standard-2)
echo "🔄 Creating Node2 (Streaming - Preemptible)..."
gcloud compute instances create node2-streaming \
  --project=$PROJECT_ID \
  --zone=$ZONE \
  --machine-type=e2-standard-2 \
  --network-interface=network-tier=PREMIUM,subnet=default,no-address \
  --maintenance-policy=TERMINATE \
  --provisioning-model=SPOT \
  --boot-disk-size=30GB \
  --boot-disk-type=pd-balanced \
  --boot-disk-image-project=$IMAGE_PROJECT \
  --boot-disk-image-family=$IMAGE_FAMILY \
  --labels=role=streaming,team=capstone4 \
  --metadata-from-file=startup-script=deployment/node2-streaming/startup.sh

# Node 3: Batch (preemptible, e2-standard-2)
echo "🔄 Creating Node3 (Batch - Preemptible)..."
gcloud compute instances create node3-batch \
  --project=$PROJECT_ID \
  --zone=$ZONE \
  --machine-type=e2-standard-2 \
  --network-interface=network-tier=PREMIUM,subnet=default,no-address \
  --maintenance-policy=TERMINATE \
  --provisioning-model=SPOT \
  --boot-disk-size=30GB \
  --boot-disk-type=pd-balanced \
  --boot-disk-image-project=$IMAGE_PROJECT \
  --boot-disk-image-family=$IMAGE_FAMILY \
  --labels=role=batch,team=capstone4 \
  --metadata-from-file=startup-script=deployment/node3-batch/startup.sh

echo "✅ VMs created successfully!"
echo "💡 Tip: Use 'make start-node2' and 'make start-node3' to start preemptible nodes on-demand"