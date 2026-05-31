# Distributed MLOps Platform for Traffic Risk Assessment

FastAPI, NextJS, PostgreSQL, MLflow, H2O.ai, Flink, Kafka, Spark, Airflow, CI/CD, Google Cloud

Team size: 5 members | Role: Leader, Machine Learning Engineer

## Summary

This project is a distributed Big Data and MLOps platform for traffic risk assessment.
It combines two coordinated streams:

1. **Historical US accident replay after 2020** for realtime inference, monitoring, and retraining.
2. **Live TomTom traffic incidents** for continuously updated dashboard visibility.

The platform trains the initial H2O baseline on **pre-2020 US accidents**, serves online inference through **MLflow**, processes live events with **Flink**, prepares retraining datasets with **Spark**, orchestrates scheduled jobs with **Airflow**, and exposes operational analytics through **FastAPI** and the **dashboard**.

## Key Contributions

1. Contributed to the ML and data analysis components of an end-to-end traffic accident severity prediction system using **7M+ US accident records**.
2. Performed exploratory data analysis (EDA) and engineered **20+ features** from temporal, weather, geospatial, and road-context data for imbalanced severity classification.
3. Trained and evaluated H2O AutoML models with MLflow experiment tracking, achieving **79.5% accuracy** and **78.5% weighted F1-score**.
4. Participated in CI/CD and deployment workflows using Docker, GitHub Actions, and Google Cloud for model serving and backend integration.
5. Built a dual-source streaming architecture that processes US replay and TomTom live incidents through a unified PyFlink job, preserving separate data contracts for PostgreSQL serving tables.
6. Designed and implemented the full 3-VM cloud topology covering control plane, streaming plane, and batch plane, with Prometheus and Grafana monitoring integrated end-to-end.

## Architecture Overview

### Deployment Topology (3 GCP VMs)

| Node | IP | Role | Key Services |
|------|-----|------|--------------|
| `node1-control` | 35.224.149.110 | Control plane | PostgreSQL/PostGIS, MLflow, FastAPI, Next.js Dashboard, Airflow, Prometheus, Grafana |
| `node2-streaming` | 35.225.231.57 | Streaming plane | Kafka (3 brokers), Flink JobManager, Redis, replay/TomTom producers |
| `node3-batch` | 34.63.78.147 | Batch plane | Spark Master/Worker, H2O retraining |

### Data Pipeline

```
US pre-2020 Bronze CSV / GCS
  → H2O offline training
  → MLflow Model Registry

US post-2020 Bronze CSV / GCS
  → Kafka replay producer
  → Flink feature engineering + MLflow inference
  → Silver JSONL + PostgreSQL traffic_risk_predictions
  → Spark batch cleaning / dedup / partitioning
  → Gold Parquet + CSV
  → H2O AutoML retraining
  → MLflow Model Registry

TomTom Incident API
  → Kafka TomTom producer
  → Flink TomTom enrichment + rule-based severity
  → PostgreSQL traffic_tomtom_incidents
```

### Dataset Strategy

| Split | Time Range | Rows | Role |
|-------|-----------|-----:|------|
| `before_2020_raw` | 2016-2019 | 2,976,413 | Offline pretraining data |
| `from_2020_raw` | 2020-2023 | 3,786,927 | Realtime replay simulation |
| `before_2020_featured` | 2016-2019 | 2,975,837 | Engineered training set |

## Service Endpoints

| Service | URL | Node |
|---------|-----|------|
| Next.js Dashboard | http://35.224.149.110:3001 | node1 |
| FastAPI Backend | http://35.224.149.110:8000 | node1 |
| FastAPI Swagger Docs | http://35.224.149.110:8000/docs | node1 |
| MLflow Tracking UI | http://35.224.149.110:5000 | node1 |
| MLflow Model Serving | http://35.224.149.110:5001/invocations | node1 |
| Airflow Webserver | http://35.224.149.110:8080 | node1 |
| Prometheus | http://35.224.149.110:9090 | node1 |
| Grafana | http://35.224.149.110:3000 | node1 |
| Blackbox Exporter | http://35.224.149.110:9115 | node1 |
| PostgreSQL | 35.224.149.110:5432 | node1 |
| Flink JobManager UI | http://35.225.231.57:8081 | node2 |
| Kafka Broker | 35.225.231.57:9092 | node2 |
| Spark Master UI | http://34.63.78.147:8080 | node3 |
| Spark Master REST | spark://10.128.0.8:7077 | node3 |

## CI/CD

- **Provider**: GitHub Actions
- **Workflow**: `.github/workflows/ci-cd.yaml`
- **Triggers**: Push/PR to `main`, `develop`, `hung1`
- **Stages**: Lint → Test → Build Docker images → Deploy to all 3 GCP VMs
- **Auto-restart**: Failures trigger automatic re-deploy with state reset

## Outcome

The result is a cloud-first platform that can be reset and rerun end-to-end, starting from offline pretraining and continuing into realtime replay plus live TomTom ingestion, with monitoring and dashboard views that follow the active data flow.