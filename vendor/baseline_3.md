# Baseline 3 - Kubeflow and MLflow MLOps Platform

## Scope

Baseline 3 is a Kubernetes-centered MLOps platform. It integrates Kubeflow, MLflow, KServe, MinIO, MySQL, Istio, and Spark-oriented example jobs. The domain examples include stock-price processing, but the main value is the production MLOps pattern.

## Architecture

| Layer | Components | Role |
|---|---|---|
| Orchestration | Kubeflow Pipelines | Runs reproducible ML workflows. |
| Experiment tracking | MLflow | Tracks experiments, artifacts, and registered models. |
| Model serving | KServe | Serves registered models in Kubernetes. |
| Artifact storage | MinIO | Stores model artifacts and pipeline outputs. |
| Metadata storage | MySQL | Stores MLflow and platform metadata. |
| Network/security | Istio, AuthorizationPolicy, NetworkPolicy | Controls service-to-service access. |
| Batch/stream examples | Spark, Kafka producer/consumer | Demonstrates data processing and model consumption. |

## Workflow

1. Provision Kubernetes and Kubeflow.
2. Install MLflow backed by MySQL and MinIO.
3. Configure Istio and network policies for secure access.
4. Submit ML pipelines from notebooks or services.
5. Register model artifacts in MLflow.
6. Serve models through KServe or MLflow-compatible serving paths.

## Comparison With This Traffic Project

| Area | Baseline 3 | Traffic Risk Project |
|---|---|---|
| Runtime platform | Kubernetes + Kubeflow | GCP Compute Engine VMs + Docker Compose |
| Model registry | MLflow | MLflow |
| Model serving | KServe / Kubernetes-native | MLflow serving on Node 1 |
| Storage | MinIO + MySQL | GCS + PostgreSQL/PostGIS + local simulation folders |
| Batch processing | Spark examples | Spark Silver-to-Gold retraining features |
| Streaming | Kafka examples | Kafka + Flink traffic replay/inference |
| Monitoring | Kubernetes platform observability | Prometheus/Grafana plus FastAPI system endpoints |
| Operational complexity | High | Medium, suitable for capstone VM deployment |

## Lessons Adopted

- MLflow should have a stable backend store and artifact root.
- Model registry and serving must be separated from training code.
- Infrastructure documentation should include credentials, ports, and service dependencies.
- Security and service boundaries matter when moving from local simulation to cloud.

## Key Difference

Baseline 3 is stronger as a general MLOps platform, but it is heavier than needed for the current course project. This traffic project intentionally uses Docker Compose on three GCP VMs so it remains demonstrable, debuggable, and aligned with Big Data course infrastructure requirements.
