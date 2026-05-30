# Distributed MLOps Platform for Traffic Risk Assessment

A high-performance, real-time distributed data pipeline and MLOps platform built to ingest, enrich, predict, and monitor traffic accident risks in the US and live incident streams from the TomTom API.

## Technical Architecture Stack
- **Languages & Frameworks:** Python, TypeScript, FastAPI, Next.js, SQL
- **Stream Processing:** Apache Flink (PyFlink 1.19), Apache Kafka (3-broker cluster)
- **Batch Processing:** Apache Spark (Master + 3 Workers)
- **Machine Learning & MLOps:** H2O.ai AutoML, MLflow Model Registry, MLflow Model Serving
- **Orchestration & Automation:** Apache Airflow, Docker Compose, GitHub Actions CI/CD
- **Cloud Infrastructure:** Google Cloud Platform (Compute Engine VMs, Cloud Storage GCS)
- **Databases & Cache:** PostgreSQL / PostGIS, Redis
- **Monitoring:** Prometheus, Grafana, Blackbox Exporter

---

## Role & Project Highlights
- **Team Size:** 5 members
- **Role:** Leader & Lead Machine Learning Engineer

### Core Contributions & Metrics:
- **Scalable ML Pipeline:** Contributed to the ML and data analysis components of an end-to-end traffic accident severity prediction system using **7M+ US accident records**.
- **Advanced Feature Engineering:** Performed exploratory data analysis (EDA) and engineered **20+ features** from temporal, weather, geospatial, and road-context data to resolve imbalanced severity classification.
- **AutoML & Tracking:** Trained and evaluated H2O AutoML models with MLflow experiment tracking, achieving **79.5% accuracy** and **78.5% weighted F1-score**.
- **Hybrid Realtime Inference:** Designed and deployed a dual-ingestion streaming pipeline in PyFlink that simultaneously processes historical replays and live TomTom API coordinates, making sub-second inferences via the MLflow HTTP serving endpoint.
- **CI/CD & Cloud MLOps:** Participated in CI/CD and deployment workflows using Docker, GitHub Actions, and Google Cloud Platform (3 VM nodes) for model serving and backend integration.
