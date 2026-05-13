# GCP Cloud Run Guide A-Z

This guide is the cloud-first runbook for `big-data-group-4`. It is written for a new student who wants to run the complete Big Data project without understanding every internal service first.

The project uses three Google Compute Engine VMs:

- `node1-control`: PostgreSQL/PostGIS, Airflow, MLflow, FastAPI, Prometheus, and Grafana.
- `node2-streaming`: Kafka, Flink streaming inference, Redis, and replay producers.
- `node3-batch`: Spark Silver-to-Gold batch processing and H2O retraining.

The expected data flow is:

```text
Bronze GCS -> Node2 Kafka/Flink -> Silver GCS + Postgres -> Node3 Spark -> Gold GCS -> H2O/MLflow -> FastAPI JSON
```

## 1. Local Machine Rules

Run commands from the repository root.

Use local only for static checks. Do not start the full local Docker Compose stack on a weak laptop unless the teacher explicitly asks for local demo evidence.

```bash
gcloud auth login
gcloud config set project big-data-group-4
gcloud config set compute/zone us-central1-a

gcloud auth list
gcloud config list
```

Safe local checks:

```bash
python3 -m pytest tests/ -v --tb=short
python3 -m compileall dashboard/backend/app processing ingestion ml/dataset ml/training orchestration -q

docker compose --env-file .env.example -f docker-compose.yaml config --quiet
docker compose --env-file .env.cloud -f deployment/node1-control/docker-compose.yaml config --quiet
docker compose --env-file .env.cloud -f deployment/node2-streaming/docker-compose.yaml config --quiet
docker compose --env-file .env.cloud -f deployment/node3-batch/docker-compose.yaml config --quiet
```

If local Docker containers were started accidentally, stop them immediately:

```bash
docker compose --file docker-compose.yaml --env-file .env --project-name traffic-local down --timeout 5
```

If Docker Snap returns `permission denied` while stopping containers, repair AppArmor and retry:

```bash
sudo aa-remove-unknown
sudo systemctl restart snapd.apparmor
sudo systemctl reload apparmor 2>/dev/null || sudo systemctl restart apparmor

docker compose --file docker-compose.yaml --env-file .env --project-name traffic-local down --timeout 5
```

## 2. Bootstrap GCP

Create or repair service account, buckets, Artifact Registry, firewall rules, and all three VMs:

```bash
export GCP_PROJECT_ID=big-data-group-4
export GCP_ZONE=us-central1-a
export GCP_REGION=us-central1

bash scripts/gcp/setup_gcp.sh
```

Upload Bronze input data if the bucket is empty:

```bash
bash scripts/gcp/upload_data_to_gcs.sh
```

Validate cloud resources:

```bash
gcloud compute instances list --project="$GCP_PROJECT_ID"
gcloud storage buckets list --project="$GCP_PROJECT_ID"
gcloud artifacts repositories list --location="$GCP_REGION" --project="$GCP_PROJECT_ID"
```

Required buckets:

- `big-data-group-4-bronze`
- `big-data-group-4-silver`
- `big-data-group-4-gold`
- `big-data-group-4-backups`
- `big-data-group-4-ml-artifacts`

## 3. Cloud Environment File

`.env.cloud` is the runtime configuration source for all VMs. VM internal IPs can change when VMs are recreated, so always verify them before manual deploy:

```bash
gcloud compute instances list \
  --filter='name~node[123]' \
  --format='table(name,status,networkInterfaces[0].networkIP,networkInterfaces[0].accessConfigs[0].natIP)'
```

Update these values when IPs change:

```bash
NODE1_INTERNAL_IP=<node1-internal-ip>
NODE2_INTERNAL_IP=<node2-internal-ip>
NODE3_INTERNAL_IP=<node3-internal-ip>
POSTGRES_HOST=<node1-internal-ip>
MLFLOW_TRACKING_URI=http://<node1-internal-ip>:5000
MLFLOW_SERVING_ENDPOINT=http://<node1-internal-ip>:5001/invocations
FASTAPI_IMAGE=us-central1-docker.pkg.dev/big-data-group-4/capstone/fastapi:latest
POSTGRES_PREDICTION_TABLE=traffic_risk_predictions
```

Upload the environment file for deploy scripts:

```bash
gcloud storage cp .env.cloud gs://big-data-group-4-bronze/env/.env.cloud
```

## 4. Build FastAPI Image

GitHub Actions normally builds and pushes this image. Manual fallback:

```bash
gcloud auth configure-docker us-central1-docker.pkg.dev

docker build -t us-central1-docker.pkg.dev/big-data-group-4/capstone/fastapi:latest \
  -f dashboard/backend/Dockerfile \
  dashboard/backend

docker push us-central1-docker.pkg.dev/big-data-group-4/capstone/fastapi:latest
```

The VM service account must be allowed to pull images:

```bash
gcloud projects add-iam-policy-binding big-data-group-4 \
  --member="serviceAccount:team4-sa@big-data-group-4.iam.gserviceaccount.com" \
  --role="roles/artifactregistry.reader" \
  --condition=None
```

## 5. Manual Cloud Deploy

Use this when GitHub Actions is unavailable or when debugging a VM directly. Deploy Node 1 first, then Node 2, then Node 3.

```bash
gcloud compute ssh node1-control --zone=us-central1-a --project=big-data-group-4 --command='
  cd /opt/traffic &&
  git fetch origin &&
  git checkout hung1 &&
  git pull origin hung1 &&
  bash scripts/gcp/run-node1.sh
'
```

```bash
gcloud compute ssh node2-streaming --zone=us-central1-a --project=big-data-group-4 --command='
  cd /opt/traffic &&
  git fetch origin &&
  git checkout hung1 &&
  git pull origin hung1 &&
  bash scripts/gcp/run-node2.sh
'
```

```bash
gcloud compute ssh node3-batch --zone=us-central1-a --project=big-data-group-4 --command='
  cd /opt/traffic &&
  git fetch origin &&
  git checkout hung1 &&
  git pull origin hung1 &&
  NODE3_H2O_MAX_RUNTIME=120 bash scripts/gcp/run-node3.sh
'
```

Restart only the Flink Python job after changing streaming code:

```bash
gcloud compute ssh node2-streaming --zone=us-central1-a --project=big-data-group-4 --command='
  cd /opt/traffic &&
  docker compose --env-file .env.cloud -f deployment/node2-streaming/docker-compose.yaml up -d --force-recreate flink-python-job
'
```

## 6. GitHub Actions Deploy

Workflow: `.github/workflows/ci-cd.yaml`.

It performs these steps on every push to `hung1`:

- Run Black, Flake8, unit tests, Python compile checks, and Docker Compose validation.
- Ensure Artifact Registry repository `capstone` exists.
- Build and push the FastAPI image.
- Ensure GCP buckets, service account, firewall rules, and the three VMs exist.
- Install the deploy SSH public key into VM metadata.
- Upload `.env.cloud` to GCS with current VM internal IP overrides.
- Deploy Node 1, optionally Node 2 and Node 3, then run smoke tests.

Required GitHub secrets:

- `GCP_PROJECT_ID=big-data-group-4`
- `GCP_SA_KEY`: JSON key for a service account that can manage Compute Engine, Storage, Artifact Registry, and existing VM metadata. API enabling should be done once by a project owner; CI treats API enable as best-effort.
- `ENV_CLOUD`: complete `.env.cloud` content.
- `HUNG_SSH_PRIVATE_KEY`: private key matching the `runner` public key installed on VMs.

Optional GitHub variables:

- `HUNG_SSH_USER=runner`
- `DEPLOY_STREAMING=true`
- `DEPLOY_BATCH=true`

If CI fails with `node3-batch not found`, run:

```bash
GCP_PROJECT_ID=big-data-group-4 GCP_ZONE=us-central1-a GCP_REGION=us-central1 bash scripts/gcp/setup_gcp.sh
```

Then push again to `hung1`.

## 7. Full Cloud Pipeline Validation

### 7.1 Check Node 1 API and databases

```bash
gcloud compute ssh node1-control --zone=us-central1-a --project=big-data-group-4 --command='
  curl -fsS http://localhost:8000/health && echo &&
  curl -fsS http://localhost:8000/api/v1/system/status | python3 -m json.tool &&
  docker exec node1-postgres psql -U capstone -d capstone_db \
    -c "SELECT count(*) AS predictions, max(event_time) AS latest_event_time FROM traffic_risk_predictions;"
'
```

### 7.2 Train the before-2020 model

Use a bounded runtime during debugging. For final evidence, increase `H2O_MAX_RUNTIME` if the VM budget allows it.

```bash
gcloud compute ssh node1-control --zone=us-central1-a --project=big-data-group-4 --command='
  cd /opt/traffic &&
  docker compose --env-file .env.cloud -f deployment/node1-control/docker-compose.yaml ps mlflow &&
  RUN_OFFLINE_TRAINING_ON_DEPLOY=true H2O_MAX_RUNTIME=300 bash scripts/gcp/run-node1.sh
'
```

Expected evidence:

```bash
gcloud compute ssh node1-control --zone=us-central1-a --project=big-data-group-4 --command='
  curl -fsS http://localhost:5000/health && echo MLflow_OK
'
```

### 7.3 Run streaming replay

```bash
gcloud compute ssh node2-streaming --zone=us-central1-a --project=big-data-group-4 --command='
  cd /opt/traffic &&
  STREAM_MAX_RECORDS=900 STREAM_THROTTLE_SECONDS=0.0 bash scripts/gcp/run-node2.sh &&
  docker compose --env-file .env.cloud -f deployment/node2-streaming/docker-compose.yaml logs --tail=120 flink-python-job
'
```

Expected evidence:

```bash
gcloud storage ls "gs://big-data-group-4-silver/process/flink_features/**" | head
```

### 7.4 Run Spark Silver-to-Gold and H2O retraining

```bash
gcloud compute ssh node3-batch --zone=us-central1-a --project=big-data-group-4 --command='
  cd /opt/traffic &&
  SPARK_READ_PARTITIONS=64 NODE3_H2O_MAX_RUNTIME=300 bash scripts/gcp/run-node3.sh
'
```

Expected evidence:

```bash
gcloud compute ssh node3-batch --zone=us-central1-a --project=big-data-group-4 --command='
  cd /opt/traffic &&
  find data/cloud/gold/features/retrain -maxdepth 4 -type f | head
'
```

### 7.5 Collect Backend JSON outputs

```bash
for path in \
  /api/v1/system/status \
  /api/v1/overview/summary \
  /api/v1/predictions/map?limit=20 \
  /api/v1/hotspots?limit=10\&min_events=1 \
  /api/v1/analytics/risk-by-hour \
  /api/v1/analytics/severity-distribution
 do
  gcloud compute ssh node1-control --zone=us-central1-a --project=big-data-group-4 --command="
    curl -fsS -H 'Accept: application/json' http://localhost:8000${path} | python3 -m json.tool >/tmp/api-output.json &&
    echo OK ${path} &&
    head -40 /tmp/api-output.json
  "
done
```

## 8. Reset and Retry

Reset replay/checkpoint state before a clean streaming demo:

```bash
gcloud compute ssh node2-streaming --zone=us-central1-a --project=big-data-group-4 --command='cd /opt/traffic && bash scripts/gcp/reset-realtime.sh'
gcloud compute ssh node3-batch --zone=us-central1-a --project=big-data-group-4 --command='cd /opt/traffic && bash scripts/gcp/reset-realtime.sh'
```

Restart Node 2 and Node 3 as a pair:

```bash
gcloud compute ssh node2-streaming --zone=us-central1-a --project=big-data-group-4 --command='cd /opt/traffic && bash scripts/gcp/run-node2.sh'
gcloud compute ssh node3-batch --zone=us-central1-a --project=big-data-group-4 --command='cd /opt/traffic && bash scripts/gcp/run-node3.sh'
```

## 9. Common Failures

- `node3-batch not found`: run `scripts/gcp/setup_gcp.sh`; the current script creates all three VMs.
- Node 1 disk 100%: resize boot disk, then run `sudo growpart /dev/sda 1 && sudo resize2fs /dev/sda1` on the VM.
- Docker pull from Artifact Registry fails: grant `roles/artifactregistry.reader` to `team4-sa@big-data-group-4.iam.gserviceaccount.com`.
- Airflow unhealthy after restart: the compose command uses `airflow db migrate || airflow db init`; check `docker logs node1-airflow`.
- Flink import fails: verify `PYTHONPATH=/opt/traffic` and that `processing.feature_engineering` exists on the VM.
- Backend returns empty JSON: this is valid if `traffic_risk_predictions` has no rows yet. Run streaming replay first.
- Local Docker cannot stop containers: repair Snap AppArmor as shown in Section 1.

## 10. Git Ignore Notes

`.gitignore` only prevents new untracked files from being added. It does not remove files that are already tracked in Git history.

This repository intentionally tracks a few files that look like they could be ignored:

- `.env.cloud` is explicitly re-included by `!.env.cloud` because it is used as a cloud deployment template.
- `vendor/baseline_*.md` are explicitly re-included as lightweight reference summaries.

To remove an already tracked generated file from Git while keeping the local copy:

```bash
git rm --cached <path>
git commit -m "Remove generated file from tracking"
```

Do not commit private keys, service-account JSON files, raw datasets, local virtual environments, Docker volumes, logs, or model artifacts.

## 11. Operating Principles

- Deploy Node 1 before Node 2 and Node 3.
- Treat Node 2 and Node 3 as a pair after replay/checkpoint failures.
- FastAPI must always return valid JSON, even when the prediction table is empty.
- Keep private keys and service-account JSON only in GitHub Secrets or local `~/.ssh`/secure secret storage.
- Rotate any private key that has been pasted into chat, terminal logs, or documentation.
