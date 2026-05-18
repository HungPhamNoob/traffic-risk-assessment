# Cloud Infrastructure Inventory

Updated from live VM command output collected on 2026-05-14 (UTC).

## Scope

This document summarizes the actual cloud infrastructure currently running, based on:

- Repo deployment definitions under `deployment/`
- GCP VM creation script at `deployment/gcp/create-vms.sh`
- Runtime audit outputs captured from:
  - `node1-control`
  - `node2-streaming`
  - `node3-batch`

## Environment Summary

| Node | Role | Zone | Internal IP | External IP | Machine Type | vCPU | RAM | Root Disk | OS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `node1-control` | control / monitoring / API / ML | `us-central1-a` | `10.128.0.4` | `34.61.176.172` | `e2-medium` | 2 | 3.8 GiB | 40 GB | Debian 12 |
| `node2-streaming` | streaming / messaging | `us-central1-a` | `10.128.0.5` | `23.236.57.67` | `e2-custom-medium-8192` | 2 | 7.8 GiB | 30 GB | Debian 12 |
| `node3-batch` | batch / Spark | `us-central1-a` | `10.128.0.8` | `34.63.78.147` | `e2-standard-2` | 2 | 7.8 GiB | 30 GB | Debian 12 |

## Node Details

### `node1-control`

Purpose:
Control plane, observability stack, transactional database, API service, and ML endpoints.

Runtime summary:

- Hostname: `node1-control`
- Docker: `29.4.2`
- Docker Compose: `v2.24.0`
- Memory pressure is high: `3.7 / 3.8 GiB` used
- Swap is exhausted: `3.0 / 3.0 GiB` used
- Disk usage: `21 / 40 GB`

Services:

| Container | Image | Status | Ports | Restart |
| --- | --- | --- | --- | --- |
| `node1-postgres` | `postgis/postgis:16-3.4-alpine` | healthy | `5432` | `unless-stopped` |
| `node1-airflow-db` | `postgres:15-alpine` | healthy | internal only | `unless-stopped` |
| `node1-airflow` | `apache/airflow:2.9.0-python3.10` | unhealthy | `8080` | `unless-stopped` |
| `node1-prometheus` | `prom/prometheus:v2.51.0` | running | `9090` | `unless-stopped` |
| `node1-grafana` | `grafana/grafana:10.4.1` | running | `3000` | `unless-stopped` |
| `node1-mlflow` | `ghcr.io/mlflow/mlflow:v2.12.1` | running | `5000` | `on-failure` |
| `node1-mlflow-serving` | `ghcr.io/mlflow/mlflow:v2.12.1` | running | `5001` | `on-failure` |
| `node1-fastapi` | `us-central1-docker.pkg.dev/big-data-group-4/capstone/fastapi:latest` | healthy | `8000` | `unless-stopped` |
| `node1-blackbox-exporter` | `prom/blackbox-exporter:v0.25.0` | running | `9115` | `unless-stopped` |

Important mounts observed:

- Main runtime path is `/opt/traffic`
- Airflow mounts DAGs and logs from `/opt/traffic/orchestration`
- Prometheus config comes from `/opt/traffic/config/monitoring/prometheus.cloud.yml`
- Grafana provisioning comes from `/opt/traffic/config/monitoring/grafana/provisioning`
- GCP secret mount resolves to `/dev/null` in multiple containers

Observed concerns:

- `node1-airflow` is unhealthy
- `node1-mlflow-serving` uses about `1.687 GiB / 2 GiB`
- `node1-postgres` shows long checkpoint durations
- This VM is likely undersized for the number of control-plane services hosted here

### `node2-streaming`

Purpose:
Kafka/Flink/Redis streaming stack and ingestion producers.

Runtime summary:

- Hostname: `node2-streaming`
- Docker: `29.4.3`
- Docker Compose: `v5.1.3`
- Disk usage is high: `25 / 30 GB` used (`88%`)
- Memory usage is moderate: `3.2 / 7.8 GiB`

Services:

| Container | Image | Status | Ports | Restart |
| --- | --- | --- | --- | --- |
| `node2-kafka` | `confluentinc/cp-kafka:7.6.0` | health starting | internal only | `unless-stopped` |
| `node2-zookeeper` | `confluentinc/cp-zookeeper:7.6.0` | healthy | `2181` | `unless-stopped` |
| `node2-redis` | `redis:7-alpine` | healthy | `6379` | `unless-stopped` |
| `node2-flink-jm` | `flink:1.19-scala_2.12` | healthy | `8081` | `on-failure` |
| `node2-flink-tm` | `flink:1.19-scala_2.12` | running | internal only | `on-failure` |
| `node2-kafka-ui` | `ghcr.io/kafbat/kafka-ui:latest` | running | `8087` | `unless-stopped` |
| `node2-flink-python-job` | `python:3.10-slim` | running | internal only | `on-failure` |
| `node2-producer-0` | `python:3.10-slim` | running | internal only | `no` |
| `node2-producer-1` | `python:3.10-slim` | running | internal only | `no` |
| `node2-producer-2` | `python:3.10-slim` | running | internal only | `no` |

Important mounts observed:

- Flink containers mount code from `/opt/capstone/processing/flink`
- Several producer/job containers mount runtime files from `/opt/traffic`
- Kafka data uses Docker volume `node2-streaming_kafka_data`
- Redis data uses Docker volume `node2-streaming_redis_data`

Observed concerns:

- `node2-kafka` is not yet healthy and shows very high CPU consumption
- `node2-kafka-ui` is near memory limit at about `234 MiB / 256 MiB`
- Producer containers repeatedly log Kafka delivery timeouts
- Producers cannot resolve `kafka-1:29092`, `kafka-2:29092`, `kafka-3:29092`
- Root disk has only about `3.5 GB` free

Operational interpretation:

- The producer configuration appears to target a 3-broker Kafka cluster
- The currently running deployment does not expose those broker DNS names
- This is likely the direct cause of the ingestion delivery failures

### `node3-batch`

Purpose:
Spark batch processing cluster.

Runtime summary:

- Hostname: `node3-batch`
- Docker: `29.4.3`
- Docker Compose: `v5.1.3`
- Memory headroom is good: `1.6 / 7.8 GiB` used
- Disk usage is low: `6.5 / 30 GB`

Services:

| Container | Image | Status | Ports | Restart |
| --- | --- | --- | --- | --- |
| `node3-spark-master` | `apache/spark:3.5.0` | healthy | `7077`, `8080` | `unless-stopped` |
| `node3-spark-worker-1` | `apache/spark:3.5.0` | healthy | `8083` | `on-failure` |
| `node3-spark-worker-2` | `apache/spark:3.5.0` | healthy | `8084` | `on-failure` |
| `node3-spark-worker-3` | `apache/spark:3.5.0` | healthy | `8085` | `on-failure` |

Important mounts observed:

- Runtime path is `/opt/traffic`
- Spark master mounts `/opt/traffic`, `/opt/traffic/data`, and `/opt/traffic/ml`
- Workers mount `/opt/traffic` and shared checkpoint storage
- GCP secret mount resolves to `/dev/null`

Observed behavior:

- Spark cluster is healthy
- Master logs show successful registration of 3 workers
- A batch job named `SilverToGoldRetrainDataset` ran successfully and then exited

## Exposed Ports Matrix

| Node | Port | Service |
| --- | --- | --- |
| `node1-control` | `3000` | Grafana |
| `node1-control` | `5000` | MLflow |
| `node1-control` | `5001` | MLflow serving |
| `node1-control` | `5432` | Postgres |
| `node1-control` | `8000` | FastAPI |
| `node1-control` | `8080` | Airflow |
| `node1-control` | `9090` | Prometheus |
| `node1-control` | `9115` | Blackbox exporter |
| `node2-streaming` | `2181` | Zookeeper |
| `node2-streaming` | `6379` | Redis |
| `node2-streaming` | `8081` | Flink JobManager UI |
| `node2-streaming` | `8087` | Kafka UI |
| `node3-batch` | `7077` | Spark master |
| `node3-batch` | `8080` | Spark master UI |
| `node3-batch` | `8083` | Spark worker 1 UI |
| `node3-batch` | `8084` | Spark worker 2 UI |
| `node3-batch` | `8085` | Spark worker 3 UI |

## Repo vs Runtime Drift

### `node1-control`

Expected from repo:

- Compose defines `postgres`, `airflow-db`, `airflow`, `prometheus`, `grafana`, `mlflow`, `fastapi-app`
- GCP VM script specifies `e2-medium` with `20 GB` boot disk

Observed in runtime:

- Actual root disk is `40 GB`
- Runtime includes additional services:
  - `node1-blackbox-exporter`
  - `node1-mlflow-serving`
- `fastapi` runs from a pushed registry image, not from a local Docker build
- Runtime config paths use `/opt/traffic`, not the repo-relative paths implied by local compose use

### `node2-streaming`

Expected from repo:

- Kafka in KRaft mode
- No `zookeeper`
- One Kafka broker, one Flink JM, one Flink TM, one Redis, one Kafka UI
- GCP VM script specifies `e2-standard-2`

Observed in runtime:

- Actual machine type is `e2-custom-medium-8192`
- Runtime includes `zookeeper`
- Runtime includes:
  - `node2-flink-python-job`
  - `node2-producer-0`
  - `node2-producer-1`
  - `node2-producer-2`
- Running Kafka topology and producer configuration do not match the repo compose model

### `node3-batch`

Expected from repo:

- One Spark master
- One Spark worker

Observed in runtime:

- One Spark master and three Spark workers are running
- Runtime mount structure is based on `/opt/traffic`

## Current Operational Risks

1. `node1-control` is under memory pressure and has full swap, which can degrade Airflow, API, and database stability.
2. `node1-airflow` is unhealthy and should be investigated before relying on scheduled orchestration.
3. `node2-streaming` has active producer delivery failures caused by Kafka broker name resolution mismatch.
4. `node2-streaming` root disk is close to full and may impact Kafka/Flink durability and restart behavior.
5. Runtime infrastructure has drifted from the repo definitions, so the repo is no longer a complete source of truth.

## Recommended Next Actions

1. Align repo deployment manifests with the actual runtime topology, especially for `node2-streaming` and `node3-batch`.
2. Capture the real compose files or startup scripts currently used under `/opt/traffic`.
3. Fix Kafka producer bootstrap server configuration to match the deployed broker naming scheme.
4. Resize or rebalance `node1-control` workloads, or move ML serving off the control node.
5. Investigate why GCP credentials are mounted as `/dev/null` in multiple containers.

