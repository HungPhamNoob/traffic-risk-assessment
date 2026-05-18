# Traffic Risk Assessment Platform

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/Apache%20Kafka-Streaming-231F20?logo=apachekafka&logoColor=white" alt="Kafka">
  <img src="https://img.shields.io/badge/Apache%20Flink-Realtime%20Inference-E6526F?logo=apacheflink&logoColor=white" alt="Flink">
  <img src="https://img.shields.io/badge/Apache%20Spark-Batch%20Processing-E25A1C?logo=apachespark&logoColor=white" alt="Spark">
  <img src="https://img.shields.io/badge/Airflow-Orchestration-017CEE?logo=apacheairflow&logoColor=white" alt="Airflow">
  <img src="https://img.shields.io/badge/MLflow-Model%20Registry-0194E2?logo=mlflow&logoColor=white" alt="MLflow">
  <img src="https://img.shields.io/badge/H2O%20AutoML-Severity%20Prediction-F58220" alt="H2O AutoML">
  <img src="https://img.shields.io/badge/FastAPI-Backend%20API-009688?logo=fastapi&logoColor=white" alt="FastAPI">
  <img src="https://img.shields.io/badge/PostgreSQL%20%2B%20PostGIS-Serving%20Store-336791?logo=postgresql&logoColor=white" alt="PostgreSQL">
  <img src="https://img.shields.io/badge/GCP-3%20Node%20Deployment-4285F4?logo=googlecloud&logoColor=white" alt="GCP">
</p>

Production-oriented Big Data platform for analyzing and predicting traffic accident severity in the United States. The system replays historical accidents as a realtime stream, performs online inference, retrains the model from accumulated features, and exposes operational and analytical APIs for a dashboard layer.

## Problem Statement

Given an accident that has already occurred and includes time, location, weather, and road context, the platform predicts its severity on a 4-level scale (`Severity 1 -> 4`).

This repository focuses on the full data and ML pipeline:

- replaying post-2020 US accident events into Kafka
- streaming feature engineering and inference with Flink
- batch Silver-to-Gold processing with Spark
- scheduled retraining with H2O AutoML and MLflow
- serving predictions and analytics through FastAPI

## Team Members

| Member | Student ID |
| --- | --- |
| Nguyễn Hữu Hải Đăng | 23020524 |
| Phạm Huy Hiếu | 23020535 |
| Phạm Khánh Duy | 23020522 |
| Đặng Quốc Huy | 23020539 |
| Phạm Việt Hưng | 23020542 |

## Architecture

![Traffic Risk Architecture](assets/pipeline.png)

The deployed cloud topology uses three Google Compute Engine VMs:

| Node | Role | Main Services |
| --- | --- | --- |
| `node1-control` | Control plane | PostgreSQL/PostGIS, Airflow, MLflow, FastAPI, Prometheus, Grafana |
| `node2-streaming` | Streaming plane | Kafka, Flink, Redis, replay producers |
| `node3-batch` | Batch plane | Spark Silver-to-Gold processing, H2O retraining |

End-to-end data flow:

```text
Bronze CSV / GCS
  -> Kafka replay producer
  -> Flink streaming feature engineering + MLflow inference
  -> Silver JSONL + PostgreSQL predictions
  -> Spark batch cleaning / dedup / partitioning
  -> Gold Parquet + CSV
  -> H2O AutoML retraining
  -> MLflow Model Registry
  -> FastAPI analytics and prediction APIs
```
## Dataset Strategy

The pipeline uses a strict temporal split to avoid leakage:

| Split | Time Range | Rows | Role |
| --- | --- | ---: | --- |
| `before_2020_raw` | 2016-2019 | 2,976,413 | offline pretraining data |
| `from_2020_raw` | 2020-2023 | 3,786,927 | realtime replay simulation |
| `before_2020_featured` | 2016-2019 | 2,975,837 | engineered training set |

This design enforces:

- `before 2020` for offline model selection and initial training
- `from 2020` for replay, online inference, and hourly retraining inputs

## EDA Highlights

The detailed EDA summary is in [ml/notebooks/eda.md](ml/notebooks/eda.md).

Key findings used in modeling decisions:

1. The temporal split is clean and operationally meaningful.
2. Feature engineering is stable: `before_2020_raw` and `before_2020_featured` differ by only a small number of rows.
3. Severity is highly imbalanced before 2020: class `2` = `67.03%`, class `3` = `29.84%`, class `4` = `3.10%`, class `1` = `0.03%`.
4. Weather, road-type, time-of-day, and night/rush-hour signals all contribute useful structure.
5. There is clear drift after 2020, especially in label distribution, so replay data must not be mixed blindly into initial offline training.

## Core Capabilities

| Layer | What it does |
| --- | --- |
| Ingestion | Reads post-2020 CSV rows and publishes raw JSON events to Kafka topic `traffic.us.raw` |
| Streaming | Flink parses raw events, builds features, calls MLflow Serving, writes Silver JSONL and PostgreSQL predictions |
| Batch | Spark validates schema, fills defaults, removes duplicates, and writes Gold Parquet/CSV |
| Training | H2O AutoML trains or retrains severity models and logs runs to MLflow |
| Orchestration | Airflow triggers hourly retraining and health-check DAGs |
| Serving | FastAPI exposes overview, prediction, hotspot, analytics, system, and model endpoints |
| Monitoring | Prometheus and Grafana collect runtime health metrics |

## Tech Stack

| Category | Stack |
| --- | --- |
| Data processing | Apache Kafka, Apache Flink, Apache Spark |
| ML lifecycle | H2O AutoML, MLflow Model Registry |
| Serving | FastAPI, PostgreSQL, PostGIS, Redis |
| Orchestration | Apache Airflow |
| Monitoring | Prometheus, Grafana |
| Infrastructure | Docker Compose, Google Compute Engine, Google Cloud Storage |
| Language | Python |

## Repository Structure

```text
assets/                      Architecture image and visual assets
dashboard/backend/           FastAPI backend for dashboard and analytics APIs
dashboard/frontend/          Frontend reference baselines and UI concepts
deployment/                  Per-node Docker Compose manifests
docs/                        Runbooks and technical documentation
ingestion/kafka/             Kafka replay producer
ml/notebooks/                EDA notebook and markdown summary
ml/training/                 H2O training and retraining scripts
orchestration/dags/          Airflow DAGs
processing/                  Shared feature engineering, Flink, and Spark jobs
scripts/gcp/                 Cloud provisioning and node operations
scripts/local/               Local smoke pipeline runner
tests/                       Unit and smoke tests
vendor/                      Reference projects from previous cohorts
```

## API Surface

The FastAPI service currently exposes:

- `/health`
- `/metrics`
- `/api/v1/overview/*`
- `/api/v1/predictions/*`
- `/api/v1/hotspots/*`
- `/api/v1/scenarios/*`
- `/api/v1/analytics/*`
- `/api/v1/system/*`
- `/api/v1/model/*`

Example local checks:

```bash
curl -fsS http://localhost:8000/health
curl -fsS http://localhost:8000/api/v1/system/status
curl -fsS http://localhost:8000/api/v1/overview/summary
```

## Local Development

Prerequisites:

- Python `3.10+`
- Docker + Docker Compose
- `uv`

Prepare the workspace:

```bash
cp .env.example .env
uv sync --group dev
```

Run validation:

```bash
make -f makefile/local/Makefile validate
```

Run the local smoke pipeline:

```bash
make -f makefile/local/Makefile pipeline
```

Run the full local pipeline with bounded training:

```bash
LOCAL_SAMPLE_ROWS=0 LOCAL_RUN_TRAINING=true \
make -f makefile/local/Makefile full-pipeline
```

Useful local targets:

```bash
make -f makefile/local/Makefile up
make -f makefile/local/Makefile up-batch
make -f makefile/local/Makefile up-orchestration
make -f makefile/local/Makefile logs
make -f makefile/local/Makefile reset-realtime
```

## Cloud Deployment

This project is cloud-first and targets `big-data-group-4` on GCP.

Validate deployment manifests:

```bash
make -f makefile/gcp/Makefile validate
```

List traffic VMs:

```bash
make -f makefile/gcp/Makefile list
```

Deploy Node 1 and start Node 2 plus Node 3 in sync:

```bash
make -f makefile/gcp/Makefile deploy-all
```

Useful operational commands:

```bash
make -f makefile/gcp/Makefile status
make -f makefile/gcp/Makefile kafka-topic-check
make -f makefile/gcp/Makefile reset-realtime
```

For the full cloud runbook, see [docs/run.md](docs/run.md).

## Current Scope And Limitations

- The backend API is implemented and usable.
- The frontend application is not yet a finalized production dashboard; `dashboard/frontend/` currently contains design baselines and reference screens.
- US Accidents is the only active production dataset.
- The main modeling challenge is severe class imbalance, especially for class `1`.

## References

- US Accidents dataset paper: <https://arxiv.org/pdf/1906.05409>
- Project EDA summary: [ml/notebooks/eda.md](ml/notebooks/eda.md)
- Cloud runbook: [docs/run.md](docs/run.md)
- Backend API notes: [docs/api.md](docs/api.md)
