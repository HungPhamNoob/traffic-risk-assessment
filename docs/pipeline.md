# Pipeline tong quan

## Muc tieu
Tai lieu nay mo ta toan bo workflow cua he thong tu input den output, gom ca streaming, batch, va ML.

## Mermaid workflow
```mermaid
flowchart LR
    A[Raw CSV US Accidents] --> B[Kafka replay producer]
    B --> C[(Kafka topic traffic.us.raw)]

    C --> D[Flink US streaming inference]
    D --> E[US Silver features JSONL]
    D --> F[PostGIS US predictions table]
    D --> G[MLflow serving]

    E --> H[Spark batch: silver to gold]
    H --> I[Gold retrain dataset (Parquet/CSV)]
    I --> J[H2O AutoML training]
    J --> K[MLflow tracking + model registry]
    K --> G

    T[TomTom Incident API] --> U[TomTom producer]
    U --> V[(Kafka topic traffic.tomtom.raw)]
    V --> W[Flink TomTom rule ingestion]
    W --> X[PostGIS TomTom incidents table]

    F --> L[FastAPI backend]
    X --> L
    G --> L
    L --> M[Dashboard / clients]

    N[Airflow orchestration] -.-> D
    N -.-> W
    N -.-> H
    N -.-> J
```

## Mo ta tung lop du lieu
- US Bronze (raw): CSV US Accidents. Ban replay tu file split sau 2020 cho streaming.
- US Silver: JSONL feature records do Flink US ghi ra. Dung lam nguon cho Spark batch.
- US Gold: Parquet/CSV cho retrain (Spark lam sach, dedupe, partition theo nam).
- TomTom live: incident events tu API, di rieng qua Kafka/Flink/PostGIS va khong vao Silver/Gold/ML.

## Streaming path (real time)
### US replay
1. Kafka replay producer doc CSV va day raw rows vao topic `traffic.us.raw`.
2. Flink US doc tu Kafka, feature engineering, goi MLflow serving de suy dien.
3. Flink US ghi:
   - Silver JSONL (feature),
   - PostGIS table `traffic_risk_predictions` chua prediction + metadata.

### TomTom live
1. TomTom producer goi Incident Details API va day event vao topic `traffic.tomtom.raw`.
2. Flink TomTom doc topic rieng, tinh `severity` bang rule `magnitudeOfDelay + iconCategory`.
3. Flink TomTom tinh `tomtom_rule_score = (severity - 1) / 3` va ghi table `traffic_tomtom_incidents`.
4. TomTom khong goi MLflow, khong ghi Silver cho Spark, khong tham gia H2O retraining.

## Batch path (retrain)
1. Spark doc Silver, lam sach va dedupe.
2. Spark ghi Gold Parquet/CSV.
3. H2O AutoML train/retrain tren Gold, log MLflow va dang ky model.

## Orchestration va monitoring
- Airflow DAG `model_retrain_hourly`: US Spark silver->gold, sau do retrain H2O.
- Airflow DAG `streaming_health_check`: kiem tra Kafka, 2 topic raw, 2 Flink jobs, va TomTom producer tren Node 2.
- FastAPI co `/metrics` cho Prometheus/Grafana.

## Output
- PostGIS table `traffic_risk_predictions`: US prediction + thong tin su kien.
- PostGIS table `traffic_tomtom_incidents`: TomTom live incident + rule severity.
- MLflow: metrics, artifacts, model registry.
- API JSON: overview, hotspots, analytics, scenarios, system status.
