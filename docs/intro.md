# Distributed MLOps Platform for Traffic Risk Assessment

FastAPI, Next.js, PostgreSQL, MLflow, H2O.ai, Flink, Kafka, Spark, Airflow, Docker, Google Cloud

Team size: 5 members | Role: Leader, Machine Learning Engineer

## Summary

This project is a distributed Big Data and MLOps platform for traffic risk assessment.  
It combines two coordinated streams:

1. **Historical US accident replay after 2020** for realtime inference, monitoring, and retraining.
2. **Live TomTom traffic incidents** for continuously updated dashboard visibility.

The platform trains the initial H2O baseline on **pre-2020 US accidents**, serves online inference through **MLflow**, processes live events with **Flink**, prepares retraining datasets with **Spark**, orchestrates scheduled jobs with **Airflow**, and exposes operational analytics through **FastAPI** and the **dashboard**.

## Key Contributions

1. Contributed to the ML and data analysis components of an end-to-end traffic accident severity prediction system using **7M+ US accident records**.
2. Performed exploratory data analysis and engineered **20+ features** from temporal, weather, geospatial, and road-context signals for imbalanced multi-class severity prediction.
3. Trained and evaluated H2O AutoML models with MLflow experiment tracking, achieving **79.5% accuracy** and **78.5% weighted F1-score** on the offline baseline workflow.
4. Built a dual-source streaming pipeline where **US replay** and **TomTom live incidents** are processed together in PyFlink while preserving separate data contracts.
5. Participated in CI/CD and deployment workflows using Docker, GitHub Actions, and Google Cloud across a **3-VM topology** for serving, orchestration, streaming, and batch processing.

## Runtime Topology

1. **Node 1 – Control plane**  
   Hosts PostgreSQL/PostGIS, MLflow, FastAPI, the dashboard frontend, Prometheus, Grafana, and Airflow.
2. **Node 2 – Streaming plane**  
   Hosts Kafka, replay producers, the TomTom producer, Redis, and the unified Flink streaming job.
3. **Node 3 – Batch plane**  
   Hosts Spark and runs the Silver-to-Gold retraining preparation plus online H2O retraining.

## Outcome

The result is a cloud-first platform that can be reset and rerun end-to-end, starting from offline pretraining and continuing into realtime replay plus live TomTom ingestion, with monitoring and dashboard views that follow the active data flow.
