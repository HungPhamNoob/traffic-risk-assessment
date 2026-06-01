# Traffic Risk Assessment Platform Report

## 1. Purpose

This project is a cloud-based traffic risk assessment platform with two main goals:

1. Replay a large US accidents dataset as a realtime stream.
2. Combine that replay stream with live TomTom traffic incidents to drive a dashboard, model inference, retraining, and monitoring workflow.

The platform is split across three Google Compute Engine VMs so streaming, control-plane services, and batch/retraining work can run independently.

## 2. High-Level Architecture

### Node 1: Control Plane

Responsibilities:

- PostgreSQL/PostGIS storage
- FastAPI backend for dashboard APIs
- Next.js frontend dashboard
- MLflow tracking server
- MLflow model serving endpoint
- Airflow scheduler and web UI
- Prometheus and Grafana

Main services:

- `node1-postgres`
- `node1-fastapi`
- `node1-dashboard-frontend`
- `node1-mlflow`
- `node1-mlflow-serving`
- `node1-airflow`
- `node1-prometheus`
- `node1-grafana`

### Node 2: Streaming Plane

Responsibilities:

- Zookeeper
- 3 Kafka brokers
- Kafka topic bootstrap
- 3 US replay producers
- 1 TomTom live producer
- 1 unified PyFlink job
- Redis

Main services:

- `node2-zookeeper`
- `node2-kafka-1`
- `node2-kafka-2`
- `node2-kafka-3`
- `node2-kafka-topic-init`
- `node2-producer-0`
- `node2-producer-1`
- `node2-producer-2`
- `node2-tomtom-producer`
- `node2-flink-jm`
- `node2-flink-tm`
- `node2-flink-python-job`

### Node 3: Batch / Retraining Plane

Responsibilities:

- Spark master and workers
- Silver-to-Gold batch processing
- Gold dataset materialization
- H2O AutoML retraining from latest replay features

Main services:

- `node3-spark-master`
- `node3-spark-worker-1`
- `node3-spark-worker-2`
- `node3-spark-worker-3`

## 3. End-to-End Data Flow

### 3.1 US Replay Flow

1. Source file:
   `gs://big-data-group-4-bronze/process/us_pipeline_from_2020.csv`
2. Three Kafka producers split the CSV by `row_index % TOTAL_PRODUCERS`.
3. Each row is published to Kafka topic `traffic.us.raw`.
4. PyFlink reads raw rows from Kafka.
5. `processing.feature_engineering.build_features` normalizes and engineers model features.
6. Feature rows are buffered and written to Silver storage in GCS.
7. Features are sent to MLflow Serving for severity inference.
8. Risk score is computed from predicted severity plus context features.
9. Results are batch-upserted into PostgreSQL table `traffic_risk_predictions`.
10. FastAPI reads this table for the dashboard map, overview cards, pipeline metrics, hotspots, and analytics.

### 3.2 TomTom Live Flow

1. TomTom producer polls the incident API on a schedule.
2. Incidents are written to Kafka topic `traffic.tomtom.raw`.
3. PyFlink reads live incident messages.
4. `processing.streaming_enrichment.enrich_tomtom_event` expands the payload.
5. Shared feature engineering runs again.
6. Severity is derived with rule-based logic instead of the US H2O model.
7. Unified risk score is computed from delay, severity, weather, road context, and time context.
8. Results are batch-upserted into PostgreSQL table `traffic_tomtom_incidents`.
9. The dashboard renders TomTom rows as live triangle markers on the map.

### 3.3 Retraining Flow

1. Flink writes replay features into Silver storage:
   `gs://big-data-group-4-silver/process/flink_features`
2. Airflow triggers Node 3 retraining work.
3. Node 3 syncs Silver data from GCS to local disk.
4. Spark reads the Silver snapshot and produces Gold retraining data.
5. Gold outputs are written locally, then synced back to GCS:
   - `GOLD_RETRAIN_PARQUET_PATH`
   - `GOLD_RETRAIN_CSV_PATH`
6. H2O AutoML trains a refreshed model from the Gold data.
7. The best model is logged and registered in MLflow.
8. Node 1 MLflow Serving can then serve the refreshed model version.

## 4. Backend API Structure

### Overview

Route: `dashboard/backend/app/routes/overview.py`

Main endpoint:

- `GET /api/v1/overview/summary`

Returns:

- total events
- high-risk event count
- average risk score
- latest event timestamp
- current mode (`replay`, `live`, `full`)
- latest selected model metrics from MLflow history

### Predictions

Route: `dashboard/backend/app/routes/predictions.py`

Main endpoints:

- `GET /api/v1/predictions/map`
- `GET /api/v1/predictions/latest`
- `GET /api/v1/predictions/{event_id}`

Purpose:

- Provide map markers for replay and live sources
- Return latest rows for the predictions table
- Return full detail for one event

Important behavior:

- Replay rows come from `traffic_risk_predictions`
- Live rows come from `traffic_tomtom_incidents`
- `mode=full` balances both sources so TomTom rows do not crowd out replay rows
- US replay rows now expose severity consistently to the dashboard tooltip path

### Analytics

Route: `dashboard/backend/app/routes/analytics.py`

Main endpoints:

- `GET /api/v1/analytics/severity-distribution`
- `GET /api/v1/analytics/risk-by-hour`
- `GET /api/v1/analytics/weather-histogram`
- `GET /api/v1/analytics/timeseries`

Purpose:

- Power dashboard charts
- Support replay-only, live-only, or combined views

### Pipeline

Route: `dashboard/backend/app/routes/pipeline.py`

Main endpoints:

- `GET /api/v1/pipeline/throughput`
- `GET /api/v1/pipeline/latency`
- `GET /api/v1/pipeline/checkpoints`
- `GET /api/v1/pipeline/replay-health`

Purpose:

- Show whether data is flowing
- Show recent latency
- Show freshness for checkpoint and Gold outputs
- Show source-level row counts, latest event time, latest insert time, and model statuses

### System

Route: `dashboard/backend/app/routes/system.py`

Purpose:

- Return the active runtime config that the dashboard needs to explain topology and service endpoints

## 5. Frontend Dashboard Structure

### Dashboard Page

File:
`dashboard/frontend/app/page.tsx`

Main sections:

- Overview KPI cards
- Replay / live / full map switcher
- Risk heatmap and point overlay
- Hotspot list
- Hourly risk chart
- Severity distribution chart
- Weather histograms
- Latest prediction table

Map semantics:

- Replay points: circles
- TomTom live points: triangles
- Replay tooltip now shows predicted severity, true severity, and risk
- Live tooltip shows severity and display risk

### Pipeline Page

File:
`dashboard/frontend/app/pipeline/page.tsx`

Main sections:

- Throughput and latency KPIs
- Stream flow KPI
- Retrain loop KPI
- System topology
- Source freshness panel
- Latency chart
- Model performance trend
- Retrain history table

Important UX change:

- The destructive reset card/button should not be the main operator control path.
- The page now focuses on whether data is flowing, whether inserts are fresh, and whether retraining is healthy.

## 6. Airflow Workflows

### `model_retrain_hourly`

File:
`orchestration/dags/dag_ml_pipeline.py`

Current workflow:

1. Airflow uses the mounted VM SSH key and internal IP for Node 3.
2. It runs `scripts/gcp/run-node3.sh`.
3. That Node 3 script:
   - ensures Spark services are running
   - waits for Silver data
   - syncs Silver from GCS to local disk
   - runs Spark Silver-to-Gold
   - syncs Gold back to GCS
   - runs H2O online retraining
4. Airflow then records completion.

Operational guardrail:

- `max_active_runs=1` prevents overlapping retrain runs from piling up.

### `streaming_health_check`

File:
`orchestration/dags/dag_stream_replay_monitor.py`

Current workflow:

1. Check Kafka broker ports directly over the internal network.
2. Check Flink JobManager HTTP on Node 2.
3. Check that at least one Flink job is in `RUNNING` state.
4. Check Spark UI HTTP on Node 3.
5. Always emit a summary task result.

Operational guardrail:

- The DAG is intentionally read-only.
- It should not restart or reset the pipeline just because a health check failed.
- `max_active_runs=1` and `retries=0` prevent the health DAG from stacking up many overlapping runs.

## 7. Critical Runtime Scripts

### `scripts/gcp/run-node1.sh`

Purpose:

- Start or reconcile Node 1 services
- Bootstrap offline model training only when needed
- Restart MLflow serving after the model is available

### `scripts/gcp/run-node2.sh`

Purpose:

- Start Kafka, producers, Redis, and Flink services on Node 2

Important behavior:

- Removes stale Node 2 containers first
- Rebuilds and starts the Node 2 compose stack
- Verifies the mounted project root

### `scripts/gcp/run-node3.sh`

Purpose:

- Start Spark services
- Sync Silver data locally
- Run Spark batch
- Sync Gold outputs back to GCS
- Execute H2O retraining

### `scripts/gcp/dashboard-full-realtime-reset-run.sh`

Purpose:

- Previously used as a dashboard reset entrypoint

Operator note:

- This path should be treated carefully because reset-style workflows are disruptive.
- The dashboard should prefer health visibility and targeted restarts over destructive resets.

## 8. Key Configuration Values

### Storage and Data Paths

- `US_PIPELINE_REPLAY_PATH`
- `SILVER_FEATURES_PATH`
- `GOLD_RETRAIN_PATH`
- `GOLD_RETRAIN_PARQUET_PATH`
- `GOLD_RETRAIN_CSV_PATH`
- `FLINK_CHECKPOINT_DIR`
- `SPARK_CHECKPOINT_DIR`

### PostgreSQL

- `POSTGRES_HOST`
- `POSTGRES_DB`
- `POSTGRES_PREDICTION_TABLE`
- `POSTGRES_US_PREDICTION_TABLE`
- `POSTGRES_TOMTOM_TABLE`
- `PG_BATCH_SIZE`
- `PG_POOL_MAX_CONN`

Why they matter:

- Table names must match the dashboard/backend query layer.
- Batch size and pool size materially affect ingest throughput.

### Streaming

- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC_RAW`
- `KAFKA_TOPIC_TOMTOM_RAW`
- `FLINK_PARALLELISM`
- `STREAM_MAX_RECORDS`
- `STREAM_THROTTLE_SECONDS`
- `STREAM_LOOP_FOREVER`
- `PRODUCER_FLUSH_EVERY_N_RECORDS`

Why they matter:

- `STREAM_THROTTLE_SECONDS` caps producer rate if nonzero.
- `STREAM_LOOP_FOREVER=true` keeps replay production alive after a full CSV pass.
- `FLINK_PARALLELISM`, `PG_BATCH_SIZE`, and `PG_POOL_MAX_CONN` together shape end-to-end throughput.

### MLflow / Model Serving

- `MLFLOW_TRACKING_URI`
- `MLFLOW_SERVING_ENDPOINT`
- `ML_MODEL_NAME`
- `ML_TIMEOUT_SECONDS`
- `MLFLOW_GUNICORN_WORKERS`

Why they matter:

- The Flink US replay path calls the serving endpoint for inference.
- More serving workers can improve concurrent inference throughput.

### Airflow

- `AIRFLOW_MODEL_RETRAIN_SCHEDULE`
- `AIRFLOW_STREAM_HEALTH_SCHEDULE`
- `HUNG_SSH_USER`
- `NODE2_INTERNAL_IP`
- `NODE3_INTERNAL_IP`

Why they matter:

- The retrain DAG needs a working path from Airflow to Node 3.
- The health DAG needs stable internal service addresses.

## 9. Observed Operational Issues and Fix Themes

### Replay appeared frozen

Observed behavior:

- Dashboard totals stopped moving.
- US replay producers had already exited after finishing one full CSV pass.

Fix direction:

- Keep replay producers alive with looped playback when desired.

### Throughput looked too low

Observed behavior:

- Only a small number of rows were reaching PostgreSQL relative to the input volume.

Likely contributors:

- Per-event HTTP inference to MLflow
- Conservative PostgreSQL batch settings
- Limited MLflow serving workers

Fix direction:

- Increase `PG_BATCH_SIZE`
- Increase `PG_POOL_MAX_CONN`
- Increase `MLFLOW_GUNICORN_WORKERS`
- Keep replay generation continuous so throughput is visible on the dashboard

### Retraining was not visible in the dashboard

Observed behavior:

- Airflow retrain runs were failing immediately.

Root cause:

- Airflow tried to SSH as `airflow@node3-batch` without a usable key.

Fix direction:

- Mount the VM SSH key into the Airflow container
- Use explicit VM user and internal IP
- Prevent overlapping retrain runs

### Stream health DAG caused noise

Observed behavior:

- Many `up_for_retry` runs accumulated
- Health checks failed because of the same SSH issue

Fix direction:

- Replace SSH checks with direct internal network probes
- Make the DAG read-only
- Prevent overlapping health runs

## 10. Recommended Operator Workflow

1. Check dashboard `/pipeline` first.
2. Confirm `Throughput`, `Stream flow`, and `Source freshness`.
3. If replay rows stop updating:
   - inspect Node 2 producers
   - inspect Kafka lag
   - inspect Flink logs
4. If retraining stops:
   - inspect `model_retrain_hourly` in Airflow
   - inspect Node 3 Spark/H2O logs
   - confirm Gold output freshness
5. Avoid destructive resets unless there is no safer recovery path.

## 11. Files Most Important for Maintenance

- `dashboard/backend/app/services/prediction_service.py`
- `dashboard/backend/app/services/pipeline_service.py`
- `dashboard/backend/app/services/analytics_service.py`
- `dashboard/frontend/app/page.tsx`
- `dashboard/frontend/app/pipeline/page.tsx`
- `dashboard/frontend/components/RiskMap.tsx`
- `processing/flink_streaming.py`
- `ingestion/kafka/us_producer.py`
- `orchestration/dags/dag_ml_pipeline.py`
- `orchestration/dags/dag_stream_replay_monitor.py`
- `scripts/gcp/run-node1.sh`
- `scripts/gcp/run-node2.sh`
- `scripts/gcp/run-node3.sh`
- `.env.cloud`

## 12. Summary

The platform is a multi-node streaming, inference, monitoring, and retraining system centered on:

- Kafka for ingest
- PyFlink for realtime processing
- PostgreSQL/PostGIS for serving data to the dashboard
- Spark + H2O + MLflow for retraining
- FastAPI + Next.js for presentation
- Airflow for orchestration

The most important operational principle is to prefer continuous, observable reconciliation over destructive reset behavior. In practice, that means:

- keep replay producers alive
- expose freshness clearly in the dashboard
- avoid health checks that mutate the system
- use retraining automation that can actually reach Node 3
