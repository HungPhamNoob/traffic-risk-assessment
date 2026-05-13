#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-big-data-group-4}"
REGION="${GCP_REGION:-us-central1}"
ZONE="${GCP_ZONE:-us-central1-a}"
SA_EMAIL="team4-sa@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Ensuring required Google Cloud APIs are enabled..."
if ! gcloud services enable \
  compute.googleapis.com \
  storage.googleapis.com \
  artifactregistry.googleapis.com \
  iap.googleapis.com \
  --project "$PROJECT_ID"; then
  echo "WARNING: The authenticated account cannot enable one or more Google Cloud APIs."
  echo "WARNING: Continuing because the APIs may already be enabled in the project."
fi

echo "Ensuring the pipeline service account exists..."
if ! gcloud iam service-accounts describe "$SA_EMAIL" --project "$PROJECT_ID" &>/dev/null; then
  gcloud iam service-accounts create team4-sa \
    --display-name="Capstone Team 4 pipeline service account" \
    --project "$PROJECT_ID"
  echo "Created service account $SA_EMAIL"
else
  echo "Service account $SA_EMAIL already exists"
fi

for role in roles/storage.admin roles/artifactregistry.reader roles/logging.logWriter roles/monitoring.metricWriter; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="$role" \
    --condition=None >/dev/null 2>&1 || true
done

echo "Ensuring Artifact Registry Docker repository exists..."
if ! gcloud artifacts repositories describe capstone \
  --location="$REGION" \
  --project="$PROJECT_ID" &>/dev/null; then
  gcloud artifacts repositories create capstone \
    --repository-format=docker \
    --location="$REGION" \
    --description="Capstone Docker images" \
    --project="$PROJECT_ID"
  echo "Created Artifact Registry repository capstone"
else
  echo "Artifact Registry repository capstone already exists"
fi

echo "Creating GCS buckets..."
for bucket in bronze silver gold backups ml-artifacts; do
  BUCKET_NAME="${PROJECT_ID}-${bucket}"
  if ! gsutil ls -b gs://$BUCKET_NAME &>/dev/null; then
    gsutil mb -l $REGION gs://$BUCKET_NAME/
    echo "Created gs://$BUCKET_NAME"
  else
    echo "gs://$BUCKET_NAME already exists"
  fi
done

echo "Ensuring SSH firewall rules exist..."
if ! gcloud compute firewall-rules describe capstone-iap-ssh \
  --project="$PROJECT_ID" &>/dev/null; then
  gcloud compute firewall-rules create capstone-iap-ssh \
    --project="$PROJECT_ID" \
    --network=default \
    --allow=tcp:22 \
    --source-ranges=35.235.240.0/20 \
    --target-tags=capstone-control,capstone-streaming,capstone-batch \
    --description="Allow IAP SSH tunnels to capstone VMs"
  echo "Created capstone-iap-ssh"
else
  echo "capstone-iap-ssh already exists"
fi

echo "Creating node1-control for PostGIS, Airflow, MLflow, Prometheus, and Grafana..."
if ! gcloud compute instances describe node1-control --zone=$ZONE --project=$PROJECT_ID &>/dev/null; then
  gcloud compute instances create node1-control \
    --zone=$ZONE \
    --project=$PROJECT_ID \
    --machine-type=e2-medium \
    --network-interface=network=default,subnet=default \
    --metadata-from-file=startup-script=scripts/gcp/startup-node1.sh \
    --service-account=$SA_EMAIL \
    --scopes=cloud-platform,storage-rw \
    --boot-disk-size=40GB \
    --boot-disk-type=pd-balanced \
    --tags=capstone-control
  echo "Created node1-control"
else
  echo "node1-control already exists"
fi

echo "Creating node2-streaming for Kafka, Flink, and Redis..."
if ! gcloud compute instances describe node2-streaming --zone=$ZONE --project=$PROJECT_ID &>/dev/null; then
  gcloud compute instances create node2-streaming \
    --zone=$ZONE \
    --project=$PROJECT_ID \
    --machine-type=e2-standard-2 \
    --network-interface=network=default,subnet=default \
    --metadata-from-file=startup-script=scripts/gcp/startup-node2.sh \
    --service-account=$SA_EMAIL \
    --scopes=cloud-platform,storage-rw \
    --boot-disk-size=30GB \
    --boot-disk-type=pd-balanced \
    --tags=capstone-streaming
  echo "Created node2-streaming"
else
  echo "node2-streaming already exists"
fi

echo "Creating node3-batch for Spark batch processing..."
if ! gcloud compute instances describe node3-batch --zone=$ZONE --project=$PROJECT_ID &>/dev/null; then
  gcloud compute instances create node3-batch \
    --zone=$ZONE \
    --project=$PROJECT_ID \
    --machine-type=e2-standard-2 \
    --network-interface=network=default,subnet=default \
    --metadata-from-file=startup-script=scripts/gcp/startup-node3.sh \
    --service-account=$SA_EMAIL \
    --scopes=cloud-platform,storage-rw \
    --boot-disk-size=30GB \
    --boot-disk-type=pd-balanced \
    --tags=capstone-batch \
    --preemptible
  echo "Created node3-batch as a preemptible VM"
else
  echo "node3-batch already exists"
fi

echo "Configuring IAP access for the current gcloud user..."
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
echo "Setup complete."
echo "Wait 3-5 minutes for startup scripts to finish installing Docker."
echo ""
echo "Next steps:"
echo "  1. Check VM status: gcloud compute instances list --project=$PROJECT_ID"
echo "  2. Test SSH:"
echo "     - Node 1: gcloud compute ssh node1-control --zone=$ZONE --tunnel-through-iap"
echo "     - Node 2: gcloud compute ssh node2-streaming --zone=$ZONE --tunnel-through-iap"
echo "     - Node 3: gcloud compute ssh node3-batch --zone=$ZONE --tunnel-through-iap"
echo "  3. Verify Docker: docker --version"
