#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-big-data-group-4}"
REGION="${GCP_REGION:-us-central1}"
ZONE="${GCP_ZONE:-us-central1-a}"
SA_EMAIL="team4-sa@${PROJECT_ID}.iam.gserviceaccount.com"

echo "🔌 Enabling required APIs..."
gcloud services enable compute.googleapis.com storage.googleapis.com artifactregistry.googleapis.com --project $PROJECT_ID

echo "🪣 Creating GCS buckets (if not exist)..."
for bucket in bronze silver gold backups ml-artifacts; do
  BUCKET_NAME="${PROJECT_ID}-${bucket}"
  gsutil ls -b gs://$BUCKET_NAME &>/dev/null || gsutil mb -l $REGION gs://$BUCKET_NAME/
done

echo "🖥️ Creating Node 1 - Control Plane VM..."
# Tạo VM node1-control với cấu hình e2-medium
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
    --no-address  # ✅ Private IP only, access via IAP
  
  echo "✅ Created node1-control"
else
  echo "⚠️ node1-control already exists"
fi

echo "🔐 Configuring IAP access for SSH..."
# Grant IAP tunneling permission cho user hiện tại
CURRENT_USER=$(gcloud config get-value account)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="user:$CURRENT_USER" \
  --role="roles/compute.instanceAdmin.v1" \
  --condition=None

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="user:$CURRENT_USER" \
  --role="roles/iap.tunnelResourceAccessor" \
  --condition=None

echo "✅ Setup complete! Next steps:"
echo "  1. Wait 2-3 minutes for VM startup script to finish"
echo "  2. Test SSH via IAP: gcloud compute ssh node1-control --zone=$ZONE --tunnel-through-iap"
echo "  3. Verify Docker: gcloud compute ssh node1-control --zone=$ZONE --tunnel-through-iap --command='docker --version'"