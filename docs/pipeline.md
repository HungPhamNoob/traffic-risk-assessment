# Pipeline tong quan

## Muc tieu
Tai lieu nay mo ta toan bo workflow cua he thong tu input den output, gom ca streaming, batch, va ML.

## Mermaid workflow
```mermaid
flowchart LR
    A[Raw CSV US Accidents] --> B[Kafka replay producer]
    B --> C[(Kafka topic traffic.us.raw)]

    C --> D[Flink streaming inference]
    D --> E[Silver features JSONL]
    D --> F[PostGIS predictions table]
    D --> G[MLflow serving]

    E --> H[Spark batch: silver to gold]
    H --> I[Gold retrain dataset (Parquet/CSV)]
    I --> J[H2O AutoML training]
    J --> K[MLflow tracking + model registry]
    K --> G

    F --> L[FastAPI backend]
    G --> L
    L --> M[Dashboard / clients]

    N[Airflow orchestration] -.-> D
    N -.-> H
    N -.-> J
```

## Mo ta tung lop du lieu
- Bronze (raw): CSV US Accidents. Ban replay tu file split sau 2020 cho streaming.
- Silver: JSONL feature records do Flink ghi ra. Dung lam nguon cho Spark batch.
- Gold: Parquet/CSV cho retrain (Spark lam sach, dedupe, partition theo nam).

## Streaming path (real time)
1. Kafka replay producer doc CSV va day raw rows vao topic `traffic.us.raw`.
2. Flink doc tu Kafka, feature engineering, goi MLflow serving de suy dien.
3. Flink ghi:
   - Silver JSONL (feature),
   - PostGIS table chua prediction + metadata.

## Batch path (retrain)
1. Spark doc Silver, lam sach va dedupe.
2. Spark ghi Gold Parquet/CSV.
3. H2O AutoML train/retrain tren Gold, log MLflow va dang ky model.

## Orchestration va monitoring
- Airflow DAG `model_retrain_hourly`: Spark silver->gold, sau do retrain H2O.
- Airflow DAG `streaming_health_check`: kiem tra Kafka/Flink/Spark, co the restart cap node 2-3.
- FastAPI co `/metrics` cho Prometheus/Grafana.

## Output
- PostGIS table: prediction + thong tin su kien (dung cho dashboard va API).
- MLflow: metrics, artifacts, model registry.
- API JSON: overview, hotspots, analytics, scenarios, system status.
