# Traffic Risk Assessment Platform — Comprehensive System Report

**Repository:** `github.com/HungPhamNoob/traffic-risk-assessment`  
**Date:** 2026-06-01  
**Environment:** Google Cloud Platform (GCP) — 3 VMs  

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture & Data Flow](#2-architecture--data-flow)
3. [Infrastructure & Node Topology](#3-infrastructure--node-topology)
4. [Google Cloud Storage (GCS) Bucket Strategy](#4-google-cloud-storage-gcs-bucket-strategy)
5. [Component Deep Dive](#5-component-deep-dive)
    - [5.1 Data Ingestion (Kafka Producers)](#51-data-ingestion-kafka-producers)
    - [5.2 Apache Kafka](#52-apache-kafka)
    - [5.3 Apache Flink Streaming](#53-apache-flink-streaming)
    - [5.4 MLflow & H2O Model Serving](#54-mlflow--h2o-model-serving)
    - [5.5 Apache Spark Batch](#55-apache-spark-batch)
    - [5.6 PostgreSQL / PostGIS](#56-postgresql--postgis)
    - [5.7 Apache Airflow](#57-apache-airflow)
    - [5.8 FastAPI Dashboard Backend](#58-fastapi-dashboard-backend)
    - [5.9 Next.js Dashboard Frontend](#59-nextjs-dashboard-frontend)
    - [5.10 Monitoring Stack (Prometheus + Grafana + Blackbox)](#510-monitoring-stack)
6. [Dashboard Pages & Features](#6-dashboard-pages--features)
7. [Throughput & Latency Optimization](#7-throughput--latency-optimization)
8. [CI/CD Pipeline](#8-cicd-pipeline)
9. [Key Configuration Parameters](#9-key-configuration-parameters)
10. [Setup Guide](#10-setup-guide)

---

## 1. System Overview

The Traffic Risk Assessment Platform is a **real-time streaming analytics system** that ingests traffic accident data from two sources:

| Source | Type | Description |
|--------|------|-------------|
| **US Accidents (Kaggle)** | Replay / Historical | A ~7 million-row CSV from Kaggle (2016–2023) replayed through Kafka via 3 parallel producers |
| **TomTom Traffic API** | Live / Streaming | Real-time NYC traffic incident data polled every 60 seconds |

**End-to-end pipeline:**  
`Producer → Kafka → Flink (enrich + feature engineering + ML inference) → PostgreSQL → FastAPI → Next.js Dashboard`

The system performs:
- Feature engineering (20+ features per event)
- H2O AutoML-based severity prediction (4-class: 1–4)
- Unified risk scoring (0.0–1.0 continuous scale)
- Geospatial hotspot analysis via PostGIS
- Real-time dashboard with map, charts, and pipeline monitoring

---

## 2. Architecture & Data Flow

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           Node 2 — Streaming (Spot VM)                   │
│                                                                          │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐                 │
│  │ US Producer 0 │   │ US Producer 1 │   │ US Producer 2 │   (3 shards) │
│  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘                 │
│         │                  │                  │                          │
│  ┌──────┴──────────────────┴──────────────────┴───────┐                 │
│  │            TomTom Producer (NYC live API)           │                 │
│  └──────────────────────┬─────────────────────────────┘                 │
│                         │                                                │
│  ┌──────────────────────┴─────────────────────────────┐                 │
│  │   Kafka Cluster (3 brokers, 3 partitions, RF=3)    │                 │
│  │   Topics: traffic.us.raw / traffic.tomtom.raw      │                 │
│  └──────────────────────┬─────────────────────────────┘                 │
│                         │                                                │
│  ┌──────────────────────┴─────────────────────────────┐                 │
│  │           Flink Streaming Job (Python)              │                 │
│  │  • Feature engineering (build_features)             │                 │
│  │  • Silver GCS JSONL writes                          │                 │
│  │  • Micro-batch MLflow inference (100 evt/req)       │                 │
│  │  • Unified risk scoring                             │                 │
│  └──────────────────────┬─────────────────────────────┘                 │
└─────────────────────────┼────────────────────────────────────────────────┘
                          │
         ┌────────────────┼────────────────┐
         │                │                │
         ▼                ▼                ▼
┌────────────────┐ ┌──────────────┐ ┌────────────────────────────┐
│   Node 1       │ │   GCS Silver │ │        Node 3 — Batch       │
│   (Standard)   │ │  (JSONL)     │ │         (Spot VM)           │
│                │ │              │ │                             │
│  • PostgreSQL  │ └──────┬───────┘ │  • Spark feature generation │
│  • MLflow      │        │         │  • H2O after-2020 training  │
│  • MLflow Serve│        ▼         │  • Parquet exports          │
│  • FastAPI     │ ┌──────────────┐ │                             │
│  • Dashboard   │ │  GCS Gold    │ └────────────────────────────┘
│  • Airflow     │ │ (Retrain)    │
│  • Prometheus  │ └──────────────┘
│  • Grafana     │
└────────────────┘
```

### Data Flow Summary

| Stage | Technology | Input | Output |
|-------|-----------|-------|--------|
| Ingestion | Python (confluent-kafka) | GCS CSV / TomTom API | Kafka `traffic.us.raw` / `traffic.tomtom.raw` |
| Streaming Enrichment | Flink + Python UDF | Kafka messages | Enriched features |
| Feature Engineering | `processing/feature_engineering.py` | Raw JSON | 20-dim feature vector |
| Silver Storage | GCS (gcsfs) | Features dict | GCS JSONL (date-partitioned) |
| ML Inference | MLflow Serving + H2O | Feature rows (batch) | Severity prediction 1-4 |
| Risk Scoring | `shared/risk_scoring.py` | Severity + context | 0.0-1.0 risk score |
| Sink | psycopg2 (batch insert) | Prediction rows | PostgreSQL (ON CONFLICT upsert) |
| API | FastAPI | SQL queries | JSON responses |
| Frontend | Next.js + MapLibre | API responses | Dashboard UI |

---

## 3. Infrastructure & Node Topology

| VM Name | Zone | Machine Type | IP (Internal / External) | Spot/Standard | CPU/Mem |
|---------|------|-------------|--------------------------|---------------|---------|
| `node1-control` | us-central1-a | e2-standard-2 | 10.128.0.4 / 35.224.149.110 | Standard | 2 vCPUs / 8 GB |
| `node2-streaming` | us-central1-a | e2-standard-2 | 10.128.0.5 / 35.225.231.57 | Spot | 2 vCPUs / 8 GB |
| `node3-batch` | us-central1-a | e2-standard-2 | 10.128.0.8 / 34.63.78.147 | Spot | 2 vCPUs / 8 GB |

### Node Roles

| Node | Responsibilities | Services |
|------|-----------------|----------|
| **node1-control** | Control plane, dashboard, model registry | PostgreSQL/PostGIS, MLflow, MLflow Serving, FastAPI, Next.js, Airflow, Prometheus, Grafana, Blackbox Exporter |
| **node2-streaming** | Real-time data pipeline | ZooKeeper, 3× Kafka brokers, 3× US Producers, TomTom Producer, Flink JobManager, Flink TaskManager, Redis |
| **node3-batch** | Batch model retraining | Spark Master, Spark Worker |

### Networking

- All nodes share a Docker overlay network named `capstone-net`
- Internal communication uses 10.128.0.x IPs over VPC
- External access:
  - Dashboard: `http://35.224.149.110:3001`
  - FastAPI: `http://35.224.149.110:8000`
  - Flink UI: `http://35.225.231.57:8081`
  - Airflow: `http://35.224.149.110:8080`
  - Grafana: `http://35.224.149.110:3000`

---

## 4. Google Cloud Storage (GCS) Bucket Strategy

| Bucket | Layer | Purpose |
|--------|-------|---------|
| `big-data-group-4-bronze` | Bronze | Raw ingested data, US pipeline CSV, TomTom raw, env configs |
| `big-data-group-4-silver` | Silver | Flink feature JSONL batches (date-partitioned), Spark feature outputs |
| `big-data-group-4-gold` | Gold | Retraining datasets (CSV and Parquet), model-ready features |
| `big-data-group-4-backups` | Operational | Flink checkpoints, system backups |
| `big-data-group-4-ml-artifacts` | ML-specific | MLflow model artifacts, trained H2O models |

### Key Paths

```
gs://big-data-group-4-bronze/
├── process/
│   ├── us_pipeline_from_2020.csv      ← US replay source (post-2020 data)
│   └── us_train_offline_before_2020.csv ← H2O training data (pre-2020)
├── raw/
│   └── tomtom/                        ← Archived TomTom API responses
└── env/
    └── .env.cloud                     ← Deployed environment config

gs://big-data-group-4-silver/
└── process/
    └── flink_features/
        └── YYYY/MM/DD/batches/        ← Flink JSONL output (date-partitioned)

gs://big-data-group-4-gold/
└── features/
    └── retrain/
        ├── csv/                       ← H2O retraining CSV
        └── parquet/                   ← Spark Parquet output

gs://big-data-group-4-backups/
└── checkpoints/
    └── flink/                         ← Flink checkpoint storage
```

---

## 5. Component Deep Dive

### 5.1 Data Ingestion (Kafka Producers)

**Files:** `ingestion/kafka/us_producer.py`, `ingestion/kafka/tomtom_producer.py`

#### US Accidents Producer (3 replicas × row_index modulo)
```python
# Key env vars:
STREAM_LOOP_FOREVER=false   # Single-pass replay, no infinite loops
TOTAL_PRODUCERS=3           # 3 parallel producers
PRODUCER_INDEX=0/1/2        # Each produces rows where row_index % 3 == index
STREAM_THROTTLE_SECONDS=0.0  # No artificial delay
PRODUCER_FLUSH_EVERY_N_RECORDS=5000
```

- Reads `us_pipeline_from_2020.csv` (post-2020 events) from GCS
- Produces ~7,200 rows/sec across 3 producers
- Kafka message format: JSON with `_ingested_at_utc` timestamp

#### TomTom Producer (Live API)
```python
TOMTOM_POLL_SECONDS=60      # Poll every 60 seconds
TOMTOM_BBOXES=US:New_York:-74.25909,40.477399,-73.700181,40.917577
TOMTOM_RUN_ONCE=false       # Continuous polling
```

- Polls TomTom Traffic Incident Details API v5
- Extracts: incident_id, severity, icon_category, delay_seconds, geometry
- Skips unchanged incidents to reduce load
- Typically produces ~10–30 new/updated events per poll cycle

### 5.2 Apache Kafka

- **3 brokers** (kafka-1:29092, kafka-2:29092, kafka-3:29092)
- **Topics:**
  - `traffic.us.raw` — 3 partitions, RF=3, min ISR=2
  - `traffic.tomtom.raw` — 3 partitions, RF=3, min ISR=2
- **ZooKeeper:** Single-node (adequate for 3-broker cluster)
- **Heap:** 256 MB–768 MB per broker

### 5.3 Apache Flink Streaming

**File:** `processing/flink_streaming.py`  
**Parallelism:** 4 task slots  
**Checkpoint interval:** 30 seconds

#### Processing Pipeline (per event):

```
Kafka message → JSON parse → Feature Engineering → MLflow Inference → Risk Scoring → PostgreSQL
```

#### Micro-batch MLflow Inference (Throughput Optimization)

The system implements **micro-batch inference** to dramatically increase throughput:

- Events are buffered into `_ML_INFERENCE_BUFFER` (size controlled by `ML_BATCH_SIZE=250`)
- When buffer reaches batch size, **all 250 events** are sent to MLflow Serving in **one HTTP request**
- MLflow `/invocations` endpoint supports `dataframe_split` format with multiple rows
- This reduces HTTP round-trips from 1000 → 10 for 1000 events (~100× reduction)

```python
# Payload format for batch inference:
{
  "dataframe_split": {
    "columns": ["lat", "lon", "hour", ...],
    "data": [
      [40.7, -73.9, 14, 3, ...],
      [40.8, -73.8, 15, 4, ...],
      ...  # up to ML_BATCH_SIZE rows
    ]
  }
}
```

#### PostgreSQL Batch Insert

- Uses `psycopg2.extras.execute_values` for batch inserts
- Batch size: `PG_BATCH_SIZE=200`
- Connection pooling: 1–4 connections
- ON CONFLICT (event_id) DO UPDATE for upserts

#### Silver GCS Writes

- Writes JSONL batches to `gs://big-data-group-4-silver/process/flink_features/YYYY/MM/DD/batches/`
- Batch size: `SILVER_FLUSH_EVERY_N=2000`

### 5.4 MLflow & H2O Model Serving

**Deployment:** node1-control  

| Service | Port | Purpose |
|---------|------|---------|
| MLflow Tracking | 5000 | Experiment tracking, model registry |
| MLflow Serving | 5001 | Model inference (H2O MOJO) |

#### Model Training

**Files:** `ml/training/h2o_before_2020.py` (offline), `ml/training/h2o_after_2020.py` (batch retrain)

- **Pre-2020 model:** Trained once at system startup on ~3,000,000 events (2016–2019)
- **Post-2020 model:** Retrained periodically via Airflow DAG on streaming data
- Uses H2O AutoML with max runtime 600 seconds
- 4-class classification: severity levels 1, 2, 3, 4
- Model artifacts stored in GCS bucket `big-data-group-4-ml-artifacts`

#### Inference Flow

```
Flink → MLflow Serving (port 5001) → H2O MOJO model → prediction
POST /invocations
Content-Type: application/json
{
  "dataframe_split": {
    "columns": [...20 features...],
    "data": [[...row1...], [...row2...]]
  }
}
```

#### Resource Optimization

- MLflow Serving: **4 GB RAM, 2 CPUs** (upgraded from 2 GB)
- MLflow Tracking: 1 GB RAM
- Timeout: 300 seconds via Gunicorn `--timeout`

### 5.5 Apache Spark Batch

**File:** `processing/spark_batch.py`  
**Deployment:** node3-batch (Spot VM)

- Reads Silver JSONL features from GCS
- Generates gold-layer retraining datasets
- Writes Parquet to `gs://big-data-group-4-gold/features/retrain/parquet/`
- Triggered via Airflow DAG `dag_ml_pipeline.py`

### 5.6 PostgreSQL / PostGIS

**Deployment:** node1-control (Docker: `postgis/postgis:16-3.4-alpine`)  

| Table | Purpose | Key | Rows (approx) |
|-------|---------|-----|---------------|
| `traffic_risk_predictions` | US Accidents predictions | event_id | ~1.1M rows |
| `traffic_tomtom_incidents` | TomTom live incidents | event_id | ~6k rows |

#### Upsert Strategy
```sql
INSERT INTO traffic_risk_predictions (...) VALUES (...)
ON CONFLICT (event_id) DO UPDATE SET
  predicted_severity = EXCLUDED.predicted_severity,
  risk_score = EXCLUDED.risk_score,
  ...
  geom = EXCLUDED.geom,
  updated_at = NOW();
```

#### PostGIS
- Geometry column: `geom GEOMETRY(Point, 4326)` (EPSG:4326 = WGS 84)
- Enables spatial queries: nearest-N, bounding box, distance calculations
- Used for: Hotspot clustering, map visualization, risk heatmaps

### 5.7 Apache Airflow

**Deployment:** node1-control  
**Version:** 2.9.0 (LocalExecutor)  

#### DAGs

| DAG | Schedule | Purpose |
|-----|---------|---------|
| `dag_ml_pipeline.py` | `*/5 * * * *` | Trigger Spark batch + H2O retraining from the latest Silver data |
| `dag_stream_replay_monitor.py` | `*/2 * * * *` | Monitor streaming health and replay freshness |

#### Airflow Tasks

1. **Spark Silver -> Gold:** SSH from Airflow on Node 1 to Node 3, then run `scripts/gcp/run-node3.sh`
2. **H2O Retrain:** Train the after-2020 model from the Gold dataset and register fresh runs in MLflow
3. **Notify Success:** Mark the batch/retrain cycle complete so the dashboard can display it in retrain history

### 5.8 FastAPI Dashboard Backend

**File:** `dashboard/backend/app/app.py`  
**Port:** 8000  

#### CORS Configuration
```python
allow_origins=[
    "http://localhost:3000", "http://localhost:3001",
    "http://35.224.149.110:3001",   # Cloud dashboard
],
allow_origin_regex=r"https?://.*",   # Allow both HTTP and HTTPS
allow_methods=["*"],
allow_headers=["*"],
```

#### Route Structure

| Prefix | Purpose | Key Endpoints |
|--------|---------|---------------|
| `/api/v1/overview` | Dashboard overview metrics | Total events, risk distribution, latest predictions |
| `/api/v1/predictions` | Prediction CRUD | Paginated predictions, filtering by severity/date |
| `/api/v1/hotspots` | Geospatial hotspots | Top-N high-risk locations, cluster analysis |
| `/api/v1/scenarios` | Scenario simulation | What-if analysis for weather/time conditions |
| `/api/v1/analytics` | Analytics/charts | Weather histogram, severity trends, time-series |
| `/api/v1/pipeline` | Pipeline health | Producer status, Kafka lag, Flink checkpointing |
| `/api/v1/system` | System monitoring | Disk, CPU, memory metrics |
| `/api/v1/model` | Model management | Current model version, performance metrics |

#### Middleware
- **CORS:** Handles preflight OPTIONS requests
- **Prometheus metrics:** `/metrics` endpoint exposes HTTP request counts and latencies
- **Health check:** `/health` endpoint for load balancer probes

### 5.9 Next.js Dashboard Frontend

**File:** `dashboard/frontend/app/page.tsx`  
**Port:** 3001 (mapped from container port 3000)  

#### Key Components

| Component | File | Description |
|-----------|------|-------------|
| **Risk Map** | `components/RiskMap.tsx` | MapLibre GL JS map with PostGIS risk markers |
| **Data State** | `components/DataState.tsx` | Real-time pipeline throughput indicators |
| **Providers** | `app/providers.tsx` | React context for API base URL |

#### Configuration
```env
NEXT_PUBLIC_API_BASE_URL=http://35.224.149.110:8000
```

#### Build Process
- `npm ci && npm run build && npm run start` (production mode)

### 5.10 Monitoring Stack

| Service | Port | Purpose |
|---------|------|---------|
| **Prometheus** | 9090 | Metrics collection (30-day retention in cloud) |
| **Grafana** | 3000 | Pre-built dashboards for pipeline health |
| **Blackbox Exporter** | 9115 | HTTP/TCP endpoint probing |

#### Prometheus Metrics (Custom)
```python
# FastAPI custom metrics:
traffic_api_requests_total{method, path, status_code}
traffic_api_request_latency_seconds{method, path}
```

#### Grafana Dashboards
- Pre-provisioned via `config/monitoring/grafana/provisioning/`
- Datasource: Prometheus at `http://prometheus:9090`
- Dashboards: Pipeline Health, System Infrastructure, ML Model Performance

---

## 6. Dashboard Pages & Features

| Page | Route | Features |
|------|-------|----------|
| **Live Risk Map** | `/` | Interactive MapLibre map with PostGIS risk markers, color-coded severity, TomTom incident overlay, real-time data flow animation |
| **Risk Analytics** | `/` (tabs) | Weather histogram, severity distribution pie, time-series charts, risk trend analysis |
| **Hotspot Analysis** | `/` (tabs) | Top-10 highest risk locations, cluster density heatmap, temporal hotspot patterns |
| **Pipeline Health** | `/pipeline` | Producer throughput (msg/s), Kafka consumer lag, Flink checkpoint status, model retraining history, Postgres insert rate |
| **Scenario Simulation** | `/scenario` | What-if weather/time/road condition analysis, risk score prediction without DB write |
| **System Infrastructure** | `/` (tabs) | CPU/Memory/Disk per node, Kafka broker health, service uptime |

---

## 7. Throughput & Latency Optimization

### Optimizations Applied

| Optimization | Before | After | Impact |
|-------------|--------|-------|--------|
| **Micro-batch MLflow inference** | 1000 events = 1000 HTTP calls | 1000 events = 4 HTTP calls (250 rows each) | ~250× fewer HTTP round-trips |
| **PostgreSQL batch insert** | 1 row per INSERT | 200 rows per execute_values | ~200× fewer SQL transactions |
| **MLflow Serving resources** | 2 GB memory | 4 GB memory + 2 CPUs | Higher inference throughput |
| **US Producer loop** | `STREAM_LOOP_FOREVER=true` | `STREAM_LOOP_FOREVER=false` | Single-pass replay avoids duplicate processing |
| **Connection pooling** | New PG connection per batch | Pool (1–4 connections, reused) | Lower connection overhead |
| **Flink parallelism** | Default | 4 task slots | Parallel processing across partitions |

### Bottleneck Analysis

| Component | CPU Usage | Status |
|-----------|-----------|--------|
| MLflow Serving (H2O) | ~141% | **Bottleneck** — single-threaded H2O frame conversion |
| Flink JobManager | ~121% | Moderate — checkpointing overhead |
| PostgreSQL | ~0.12% | Not bottlenecked |
| FastAPI | ~0.20% | Not bottlenecked |

**Primary bottleneck:** H2O model inference (`H2ODependencyWarning: Converting H2O frame to pandas dataframe using single-thread`). Mitigated by micro-batch inference reducing HTTP overhead but core H2O limitation remains.

### Future Optimization Opportunities

1. **H2O multi-threading:** Configure `h2o.init(nthreads=-1)` in serving environment
2. **Model quantization:** Convert H2O model to ONNX for faster inference
3. **Redis caching:** Cache frequent prediction results for identical feature sets
4. **Flink operator chaining:** Optimize Flink job graph for lower serialization overhead

### How Core Metrics Are Calculated

- **Unified risk score:** `shared/risk_scoring.py` uses a severity-first base score of `{1: 0.00, 2: 0.25, 3: 0.55, 4: 0.85}` and then applies bounded adjustments for delay, incident length, night driving, weekend, highway roads, and bad weather. The final score is clamped to `[0.0, 1.0]`.
- **Throughput (`events_per_second`):** FastAPI counts rows in the selected serving tables whose `processed_time` falls inside the requested lookback window, then divides by `window_seconds`.
- **Latency (`p50`, `p95`, `p99`, `avg`):** FastAPI unions recent latency samples from the serving tables and computes SQL percentiles with `percentile_cont(...) WITHIN GROUP (ORDER BY latency_ms)`.
- **End-to-end latency:** `processing/flink_streaming.py` calculates `end_to_end_latency_ms = processed_time - ingestion_time` in milliseconds for each row written to PostgreSQL.
- **Replay freshness:** the dashboard pipeline page should anchor to `processed_time`, not `created_at`, because replay rows are upserted and can stay active long after the first insert timestamp.

---

## 8. CI/CD Pipeline

**File:** `.github/workflows/ci-cd.yaml`  
**Triggers:** Push to `main`, `develop`, `hung1` (PRs run lint+test only)

### Job Flow

```
┌─────────────┐     ┌──────────────────┐     ┌──────────────────────┐
│ Lint & Test │ ──▶ │ Build & Push     │ ──▶ │ Deploy to 3 GCP VMs  │
│ (always)    │     │ Docker Images    │     │ (push only)          │
│             │     │ (push only)      │     │                      │
│ - pytest    │     │ - FastAPI image  │     │ - SSH into each VM   │
│ - compile   │     │ - Push to GAR    │     │ - git pull latest    │
│ - compose   │     │                  │     │ - docker compose up  │
│   validate  │     │                  │     │ - verify containers  │
└─────────────┘     └──────────────────┘     └──────────────────────┘
```

### Key Features

- **Non-destructive deployment:** GitHub Actions creates a source archive from the pushed commit, syncs it into `/opt/traffic` with `rsync`, and then runs the node-specific launch scripts so stateful services keep their data volumes
- **Safe Node 1 restart:** Skips restart if active H2O training process is detected (PID file check)
- **No full reset:** Preserves Kafka topics, Flink checkpoints, PostgreSQL data, and MLflow models
- **Runtime SSH consistency:** CI/CD must write the same `HUNG_SSH_USER` that matches the private key copied into Node 1; otherwise Airflow cannot SSH to Node 3 for `model_retrain_hourly`
- **Concurrency:** `cancel-in-progress: true` prevents overlapping deployments

### Deployment Safety Notes

- Node 1/2/3 deploy helpers must invoke `/opt/traffic/scripts/gcp/run-node*.sh` with absolute paths after syncing code into `/opt/traffic`; invoking `scripts/gcp/run-node*.sh` from an unrelated working directory can fail even when the files exist on disk.
- CI/CD source sync should exclude generated frontend directories such as `dashboard/frontend/node_modules/` and `dashboard/frontend/.next/` so `rsync --delete` does not fight with the running Next.js container.

### Deployment Commands (per Node)

```bash
# Node 1 (Control Plane)
sudo -E bash scripts/gcp/run-node1.sh

# Node 2 (Streaming)  
sudo -E bash scripts/gcp/run-node2.sh

# Node 3 (Batch)
sudo -E bash scripts/gcp/run-node3.sh
```

### GCP Artifact Registry

- Registry: `us-central1-docker.pkg.dev/big-data-group-4/capstone`
- Image: `fastapi:latest` (tagged with git SHA and `latest`)

---

## 9. Key Configuration Parameters

### Environment Variables (`.env.cloud`)

```bash
# Google Cloud
GCP_PROJECT_ID=big-data-group-4
GCP_REGION=us-central1
GCP_ZONE=us-central1-a

# GCS Buckets
GCS_BUCKET_BRONZE=big-data-group-4-bronze
GCS_BUCKET_SILVER=big-data-group-4-silver
GCS_BUCKET_GOLD=big-data-group-4-gold

# Node IPs
POSTGRES_HOST=10.128.0.4
NODE1_INTERNAL_IP=10.128.0.4
NODE2_INTERNAL_IP=10.128.0.5
NODE3_INTERNAL_IP=10.128.0.8
HUNG_SSH_USER=runner            # Must match the private key mounted into Node 1 / Airflow

# Streaming
STREAM_LOOP_FOREVER=false          # Single-pass US replay
STREAM_MAX_RECORDS=0               # All records
STREAM_THROTTLE_SECONDS=0.0        # No throttling
TOTAL_PRODUCERS=3                  # 3 US producers
FLINK_PARALLELISM=4                # 4 task slots
FLINK_CHECKPOINT_INTERVAL=30000    # 30 seconds
ML_BATCH_SIZE=100                  # Micro-batch 100 events per inference call
PG_BATCH_SIZE=200                  # Batch insert 200 rows
SILVER_FLUSH_EVERY_N=250           # GCS flush every 250 features

# MLflow
MLFLOW_TRACKING_URI=http://10.128.0.4:5000
MLFLOW_SERVING_ENDPOINT=http://10.128.0.4:5001/invocations
ML_MODEL_NAME=traffic-risk-model
ML_TIMEOUT_SECONDS=5

# TomTom
TOMTOM_POLL_SECONDS=60
TOMTOM_BBOXES=US:New_York:-74.25909,40.477399,-73.700181,40.917577

# Dashboard
DASHBOARD_PORT=3001
NEXT_PUBLIC_API_BASE_URL=http://35.224.149.110:8000
```

---

## 10. Setup Guide

### Prerequisites

- Google Cloud Platform project with billing enabled
- 3 VM instances (e2-standard-2) in us-central1-a
- Service account with Storage Admin + Compute Admin roles
- GitHub repository with secrets configured

### Step 1: GCP Infrastructure

```bash
# Run the setup script (creates VMs, firewall rules, GCS buckets)
bash scripts/gcp/setup_gcp.sh
```

### Step 2: SSH Key Setup

```bash
# Check existing keys
ls -la ~/.ssh

# Generate if needed
ssh-keygen -t rsa -b 4096 -C "your-email@example.com" -f ~/.ssh/google_compute_engine

# Add to GCP metadata
gcloud compute instances add-metadata node1-control \
  --zone=us-central1-a \
  --metadata="ssh-keys=hung:$(cat ~/.ssh/google_compute_engine.pub)"
```

### Step 3: GitHub Secrets

Configure the following in GitHub repository settings:

| Secret | Description |
|--------|-------------|
| `GCP_SA_KEY` | Service account JSON key |
| `GCP_PROJECT_ID` | GCP project ID |
| `HUNG_SSH_PRIVATE_KEY` | Private SSH key for VM access |
| `ENV_CLOUD` | Full `.env.cloud` content |

### Step 4: Trigger Deployment

```bash
# Push to main/develop/hung1 to trigger CI/CD
git add -A
git commit -m "feat: deploy traffic risk platform"
git push origin main
```

### Step 5: Verify Deployment

```bash
# Check Node 1 services
ssh hung@35.224.149.110 "docker ps --format 'table {{.Names}}\t{{.Status}}'"

# Check Node 2 services
ssh hung@35.225.231.57 "docker ps --format 'table {{.Names}}\t{{.Status}}'"

# Open dashboard
open http://35.224.149.110:3001
```

### Step 6: Verify Data Flow

```bash
# Check Postgres row counts
curl -s http://35.224.149.110:8000/api/v1/overview/summary | jq .

# Check Kafka topic sizes
ssh hung@35.225.231.57 "docker exec node2-kafka-1 kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic traffic.us.raw"

# Check Flink job status
curl -s http://35.225.231.57:8081/jobs | jq .
```

---

## Appendix A: File Structure (Key Files)

```
.
├── .github/workflows/ci-cd.yaml          ← CI/CD pipeline
├── deployment/
│   ├── node1-control/docker-compose.yaml  ← Node 1 services
│   ├── node2-streaming/docker-compose.yaml ← Node 2 services
│   └── node3-batch/docker-compose.yaml    ← Node 3 services
├── ingestion/kafka/
│   ├── us_producer.py                     ← US Accidents producer
│   └── tomtom_producer.py                 ← TomTom live producer
├── processing/
│   ├── flink_streaming.py                 ← Unified Flink job
│   ├── feature_engineering.py             ← Feature extraction
│   ├── spark_batch.py                     ← Spark batch processing
│   └── streaming_enrichment.py            ← TomTom event enrichment
├── dashboard/
│   ├── backend/app/app.py                 ← FastAPI entrypoint
│   └── frontend/app/page.tsx              ← Next.js dashboard page
├── ml/training/
│   ├── h2o_before_2020.py                 ← Initial model training
│   └── h2o_after_2020.py                  ← Retraining script
├── orchestration/dags/
│   ├── dag_ml_pipeline.py                 ← ML retraining DAG
│   └── dag_stream_replay_monitor.py       ← Pipeline monitor DAG
├── scripts/gcp/
│   ├── run-node1.sh                       ← Node 1 launch script
│   ├── run-node2.sh                       ← Node 2 launch script
│   ├── run-node3.sh                       ← Node 3 launch script
│   └── setup_gcp.sh                       ← GCP infrastructure setup
└── shared/
    └── risk_scoring.py                    ← Unified risk score formula
```

## Appendix B: Port Map

| Port | Node | Service |
|------|------|---------|
| 3001 | node1 | Dashboard Frontend (Next.js) |
| 8000 | node1 | FastAPI Backend |
| 5000 | node1 | MLflow Tracking |
| 5001 | node1 | MLflow Model Serving |
| 5432 | node1 | PostgreSQL |
| 8080 | node1 | Airflow Web UI |
| 3000 | node1 | Grafana |
| 9090 | node1 | Prometheus |
| 9092 | node2 | Kafka Broker 1 |
| 9093 | node2 | Kafka Broker 2 |
| 9094 | node2 | Kafka Broker 3 |
| 2181 | node2 | ZooKeeper |
| 8081 | node2 | Flink Web UI |
| 6379 | node2 | Redis |
| 7077 | node3 | Spark Master |
| 8080 | node3 | Spark Web UI |
