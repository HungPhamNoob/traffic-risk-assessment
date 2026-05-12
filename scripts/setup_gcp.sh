#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-big-data-group-4}"
REGION="${GCP_REGION:-us-central1}"
ZONE="${GCP_ZONE:-us-central1-a}"
SA_EMAIL="team4-sa@${PROJECT_ID}.iam.gserviceaccount.com"

echo "🔌 Enabling required APIs..."
gcloud services enable compute.googleapis.com storage.googleapis.com artifactregistry.googleapis.com iap.googleapis.com --project $PROJECT_ID

echo "🪣 Creating GCS buckets..."
for bucket in bronze silver gold backups ml-artifacts; do
  BUCKET_NAME="${PROJECT_ID}-${bucket}"
  if ! gsutil ls -b gs://$BUCKET_NAME &>/dev/null; then
    gsutil mb -l $REGION gs://$BUCKET_NAME/
    echo "✅ Created gs://$BUCKET_NAME"
  else
    echo "⚠️ gs://$BUCKET_NAME already exists"
  fi
done

echo "🖥️ Creating Node 1 - Control Plane (e2-medium)..."
if ! gcloud compute instances describe node1-control --zone=$ZONE --project=$PROJECT_ID &>/dev/null; then
  gcloud compute instances create node1-control \
    --zone=$ZONE \
    --project=$PROJECT_ID \
    --machine-type=e2-medium \
    --network-interface=network=default,subnet=default,no-address \
    --metadata-from-file=startup-script=deployment/gcp/startup-node1.sh \
    --service-account=$SA_EMAIL \
    --scopes=cloud-platform,storage-rw \
    --boot-disk-size=20GB \
    --boot-disk-type=pd-balanced \
    --tags=capstone-control \
    --no-address
  echo "✅ Created node1-control"
else
  echo "⚠️ node1-control already exists"
fi

echo "🖥️ Creating Node 2 - Streaming Plane (e2-standard-2)..."
if ! gcloud compute instances describe node2-streaming --zone=$ZONE --project=$PROJECT_ID &>/dev/null; then
  gcloud compute instances create node2-streaming \
    --zone=$ZONE \
    --project=$PROJECT_ID \
    --machine-type=e2-standard-2 \
    --network-interface=network=default,subnet=default,no-address \
    --metadata-from-file=startup-script=deployment/gcp/startup-node2.sh \
    --service-account=$SA_EMAIL \
    --scopes=cloud-platform,storage-rw \
    --boot-disk-size=30GB \
    --boot-disk-type=pd-balanced \
    --tags=capstone-streaming \
    --no-address
  echo "✅ Created node2-streaming"
else
  echo "⚠️ node2-streaming already exists"
fi

echo "🖥️ Creating Node 3 - Batch Plane (e2-medium Standard)..."
if ! gcloud compute instances describe node3-batch --zone=$ZONE --project=$PROJECT_ID &>/dev/null; then
  gcloud compute instances create node3-batch \
    --zone=$ZONE \
    --project=$PROJECT_ID \
    --machine-type=e2-medium \
    --network-interface=network=default,subnet=default,no-address \
    --metadata-from-file=startup-script=deployment/gcp/startup-node3.sh \
    --service-account=$SA_EMAIL \
    --scopes=cloud-platform,storage-rw \
    --boot-disk-size=30GB \
    --boot-disk-type=pd-balanced \
    --tags=capstone-batch \
    --no-address
  echo "✅ Created node3-batch (Standard)"
else
  echo "⚠️ node3-batch already exists"
fi

echo "🔐 Configuring IAP access..."
CURRENT_USER=$(gcloud config get-value account)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="user:$CURRENT_USER" \
  --role="roles/compute.instanceAdmin.v1" \
  --condition=None 2>/dev/null || true

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="user:$CURRENT_USER" \
  --role="roles/iap.tunnelResourceAccessor" \
  --condition=None 2>/dev/null || true

echo ""
echo "✅ Setup complete!"
echo "⏳ Wait 3-5 minutes for startup scripts to finish..."
echo ""
echo "📋 Next steps:"
echo "  1. Check VM status: gcloud compute instances list --project=$PROJECT_ID"
echo "  2. Test SSH:"
echo "     - Node 1: gcloud compute ssh node1-control --zone=$ZONE --tunnel-through-iap"
echo "     - Node 2: gcloud compute ssh node2-streaming --zone=$ZONE --tunnel-through-iap"
echo "     - Node 3: gcloud compute ssh node3-batch --zone=$ZONE --tunnel-through-iap"
echo "  3. Verify Docker: docker --version"