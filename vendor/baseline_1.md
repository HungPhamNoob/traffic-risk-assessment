# Baseline 1 - Fraud Detection Big Data Pipeline

## Scope

Baseline 1 is a fraud detection pipeline that combines realtime ingestion, stream processing, BigQuery storage, Spark batch jobs, and Grafana reporting. The business domain is financial fraud, not traffic risk, but the infrastructure pattern is directly useful for this project because it demonstrates measurable throughput, latency, and dashboard observability.

## Architecture

The project is organized around four main layers:

| Layer | Components | Role |
|---|---|---|
| Ingestion | Kafka producer | Sends transaction records into Kafka for realtime processing. |
| Streaming | Kafka, Flink, Kafka Connect | Processes transaction events, computes fraud predictions, and writes processed output. |
| Storage | BigQuery | Stores raw transactions, model predictions, latency fields, and dashboard-ready aggregates. |
| Analytics | Spark, Grafana | Runs batch analytics and visualizes fraud distribution, fraud ratio, throughput, latency, and geography. |

## Data Flow

1. `transactions_input.csv` provides input transaction data.
2. Kafka producers publish transaction events.
3. Flink processes events and calculates fraud signals.
4. Kafka Connect or related sink logic writes predictions into BigQuery.
5. Grafana uses SQL panels against BigQuery views to show monitoring and business metrics.

## Monitoring Pattern

The strongest part of this baseline is observability. The Grafana folder contains dashboard SQL for:

- Fraud distribution.
- Fraud ratio.
- Geographic distribution.
- Latency.
- Throughput.

This project adopts the same idea, but maps the business metrics to traffic risk:

- Fraud alert latency becomes traffic event inference latency.
- Fraud throughput becomes accident replay TPS.
- Fraud distribution becomes severity and risk distribution.
- Geographic fraud panels become traffic hotspot and live risk map panels.

## Comparison With This Traffic Project

| Area | Baseline 1 | Traffic Risk Project |
|---|---|---|
| Domain | Financial fraud detection | US traffic accident severity and risk scoring |
| Streaming | Kafka + Flink | Kafka + Flink |
| Batch | Spark | Spark |
| Storage | BigQuery | PostgreSQL/PostGIS + local/GCS Gold |
| Model Ops | External model artifact pattern | H2O AutoML + MLflow registry |
| Dashboard | Grafana on BigQuery SQL | FastAPI dashboard API + Prometheus/Grafana monitoring |
| Latency metric | Explicit latency SQL | `ingestion_time`, `processed_time`, `end_to_end_latency_ms` fields |
| Throughput metric | SQL throughput panel | Local performance JSON and Prometheus API metrics |

## Lessons Adopted

- Monitoring must be treated as part of the pipeline, not as a final visualization step.
- Latency and throughput should be persisted as first-class fields.
- Dashboard queries should use serving-ready tables instead of expensive raw scans.
- Grafana is useful for operational health, while the product backend is better for interactive map and scenario APIs.
