# Service Endpoints — Traffic Risk Assessment Platform

All services run on Google Cloud VMs in `us-central1-a`.  
Access them from your local laptop via the external IP of each node.

---

## Node 1 — Control Plane (`35.224.149.110`)

| Service | URL | Credentials |
|---|---|---|
| **Next.js Dashboard** | http://35.224.149.110:3001 | — |
| **FastAPI Backend** | http://35.224.149.110:8000 | — |
| **FastAPI Docs (Swagger)** | http://35.224.149.110:8000/docs | — |
| **MLflow Tracking UI** | http://35.224.149.110:5000 | — |
| **MLflow Model Serving** | http://35.224.149.110:5001/invocations | POST only |
| **Airflow Webserver** | http://35.224.149.110:8080 | admin / admin |
| **Prometheus** | http://35.224.149.110:9090 | — |
| **Grafana** | http://35.224.149.110:3000 | admin / 123 |
| **Blackbox Exporter** | http://35.224.149.110:9115 | — |
| **PostgreSQL** | 35.224.149.110:5432 | capstone / 123 / capstone_db |

The cloud validation runner records live request/response previews for:

- `GET /health`
- `GET /api/v1/system/status`
- `GET /api/v1/overview/summary?mode=full`
- `GET /api/v1/pipeline/throughput?window=5m`
- `GET /api/v1/pipeline/latency?metric=p95`
- `GET /api/v1/model/retrain-history?limit=1`

---

## Node 2 — Streaming (`35.225.231.57`)

| Service | URL | Notes |
|---|---|---|
| **Flink JobManager UI** | http://35.225.231.57:8081 | Job status, task managers, checkpoints |
| **Kafka Broker 1** | 35.225.231.57:9092 | Internal: 10.128.0.5:9092 |
| **Kafka Broker 2** | 35.225.231.57:9093 | Internal: 10.128.0.5:9093 |
| **Kafka Broker 3** | 35.225.231.57:9094 | Internal: 10.128.0.5:9094 |

---

## Node 3 — Batch (`34.63.78.147`)

| Service | URL | Notes |
|---|---|---|
| **Spark Master UI** | http://34.63.78.147:8080 | Workers, jobs, stages |
| **Spark Master REST** | spark://10.128.0.8:7077 | Internal only (Airflow/submit) |

---

## Data Pipeline Flow

```
TomTom API ──▶ Kafka (traffic.tomtom.raw) ──▶ Flink ──▶ table: traffic_tomtom_incidents ──▶ Dashboard (Live ▲)
                                                   │
US Replay  ──▶ Kafka (traffic.us.raw)    ──▶ Flink ──▶ MLflow/H2O ──▶ table: traffic_risk_predictions ──▶ Dashboard (Replay ●)
                                                                  │
                                               GCS Silver ──▶ Spark ──▶ GCS Gold ──▶ H2O Retrain (Airflow every 5 min)
```

---

## GCS Buckets

| Bucket | Purpose |
|---|---|
| `big-data-group-4-bronze` | Raw US CSV, raw env files, replay split CSVs |
| `big-data-group-4-silver` | Flink feature output (JSON per event) |
| `big-data-group-4-gold` | Spark aggregated Parquet for H2O retraining |
| `big-data-group-4-ml-artifacts` | MLflow model artifacts |
| `big-data-group-4-backups` | Flink and Spark checkpoints |

---

## Kafka Topics

| Topic | Producer | Consumer |
|---|---|---|
| `traffic.us.raw` | `ingestion/kafka/us_producer.py` (Node 2) | Flink US stream |
| `traffic.tomtom.raw` | `ingestion/kafka/tomtom_producer.py` (Node 2) | Flink TomTom stream |

---

## PostgreSQL Tables

| Table | Source | Dashboard Mode |
|---|---|---|
| `traffic_risk_predictions` | US replay → Flink → H2O inference | **Replay ●** |
| `traffic_tomtom_incidents` | TomTom API → Flink → rule-based (display risk derived) | **Live ▲** |

---

## Prometheus Metrics Scraped

- FastAPI: `http://fastapi:8000/metrics`
- Flink: `http://flink-jobmanager:9249/metrics`
- Kafka (JMX): `http://kafka:7071/metrics`
- Blackbox probes: all external endpoints
- Node exporter: all three VMs

---

## Grafana Dashboard

Open http://35.224.149.110:3000, log in with `admin / 123`.  
The **Traffic Risk Platform** dashboard is pre-provisioned under `config/monitoring/grafana/provisioning/`.

Key panels:
- Kafka producer throughput (msg/s)
- Flink task manager lag
- End-to-end latency (P50 / P95 / P99)
- PostgreSQL row insertion rate
- H2O model version active in MLflow serving
- Spark job duration

If throughput stays low and latency stays high after a successful run, the main reasons are:

1. Kafka replay currently uses **one topic partition**.
2. The Flink streaming job currently uses **parallelism = 1**.
3. Each US replay event performs **one synchronous MLflow HTTP call**.
4. Each event opens **one PostgreSQL connection** for insert/upsert.
5. Each US event writes **one GCS object** to the Silver layer.

---

## SSH Access

```bash
# Node 1
ssh -i ~/.ssh/gcp_key hung@35.224.149.110

# Node 2
ssh -i ~/.ssh/gcp_key hung@35.225.231.57

# Node 3
ssh -i ~/.ssh/gcp_key hung@34.63.78.147
