# Road Accident Risk Platform

Production-oriented Big Data pipeline for road accident risk scoring.

The active scope is the data and ML platform:

- Kafka producers replay US accident events from the post-2020 split period.
- Flink runtime hosts the streaming inference worker.
- Spark builds unified batch ML features from historical CSV data.
- Airflow orchestrates the batch ML workflow and streaming health checks.
- H2O AutoML trains the risk model.
- MLflow tracks experiments and registers trained models.
- GCP deployment uses three Compute Engine VMs: control, streaming, and batch.

Dashboard backend work is included under `dashboard/backend`. The frontend folder is intentionally left blank.

## Repository Layout

```text
dashboard/backend/         FastAPI service for overview, map, hotspot, analytics, and system endpoints
data/process/              Processed US feature data for offline training
data/split/                US replay split from 2020 onward
deployment/                Per-node Docker Compose deployment files
ingestion/kafka/           Kafka replay producer
ml/training/               H2O + MLflow model training
orchestration/dags/        Airflow DAGs
processing/                Shared feature engineering, Flink, and Spark jobs
scripts/gcp/               GCP VM setup and operations
scripts/local/             Local smoke pipeline that writes simulation artifacts and API JSON outputs
scripts/maintenance/       Backup and maintenance helpers
tests/                     Unit and smoke tests
vendor/                    Reference projects from previous cohorts
```

## Quick Start

```bash
cp .env.example .env
uv sync --group dev
make local-up
uv run pytest -q
```

Run the local smoke pipeline. It reads the post-2020 replay split, writes local simulation outputs, seeds PostgreSQL, and stores API responses under `data/simulation/api`.

```bash
make local-pipeline
```

Run batch feature engineering and H2O training:

```bash
uv run spark-submit --master local[*] processing/spark_batch.py
uv run python ml/training/train_before_2020.py
```

## GCP Target

The cloud project uses `/opt/traffic` as the VM project root and `.env.cloud` as the deployment source of truth.

Your current GCP project is `big-data-group-4`, with VMs already created:

- `node1-control`
- `node2-streaming`
- `node3-batch`

Use `scripts/gcp/setup_gcp.sh` for a fresh setup, or use `scripts/gcp/manage-nodes.sh` to start, stop, and inspect existing VMs.
