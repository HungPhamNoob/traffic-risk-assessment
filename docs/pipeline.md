# Pipeline Overview

This project runs two coordinated traffic streams with separate data contracts.

## Data Flows

```mermaid
flowchart LR
    A[US Accidents before 2020] --> B[H2O offline training]
    B --> C[MLflow Model Registry]
    C --> D[MLflow Serving]

    E[US Accidents from 2020 onward] --> F[Kafka topic traffic.us.raw]
    F --> G[Unified Flink job]
    G --> H[Silver GCS features]
    G --> I[PostgreSQL traffic_risk_predictions]
    G --> D
    H --> J[Spark Silver to Gold]
    J --> K[Gold retrain dataset]
    K --> L[H2O retraining]
    L --> C

    M[TomTom Incident API] --> N[Kafka topic traffic.tomtom.raw]
    N --> G
    G --> O[PostgreSQL traffic_tomtom_incidents]

    I --> P[FastAPI]
    O --> P
    P --> Q[Dashboard]
    P --> R[Prometheus and Grafana]
```

## US Replay Stream

The US stream keeps the existing ML workflow:

- The initial H2O model is trained only on pre-2020 US accident data.
- Post-2020 US records are replayed through Kafka as realtime events.
- Flink builds the same feature contract used by offline training.
- Flink writes feature records to Silver GCS for Spark/H2O retraining.
- Flink calls MLflow Serving and writes H2O predictions to `traffic_risk_predictions`.

## TomTom Live Stream

The TomTom stream is intentionally separate from Spark, MLflow, and H2O:

- TomTom incidents are fetched from the live Incident Details API.
- The producer publishes normalized raw events to `traffic.tomtom.raw`.
- The same unified Flink job reads TomTom and US topics in parallel.
- TomTom enrichment maps `magnitudeOfDelay` and `iconCategory` to severity `1-4`.
- The dashboard risk score is derived from that severity only for display coloring.
- Flink writes live records to `traffic_tomtom_incidents`.

TomTom is not passed through the US-trained H2O model because its label is rule-based and its timestamp distribution is current live traffic, not the historical US replay timeline.

## Dashboard Modes

The map exposes three modes:

- `Replay`: US replay only, circular markers, color by H2O `risk_score`.
- `Live`: TomTom only, triangular markers, color by TomTom rule-based display risk.
- `Full`: US replay and TomTom live layers together with separate marker shapes.

## Monitoring

FastAPI exposes `/metrics` for Prometheus. Grafana is provisioned with a `Traffic Risk Platform` dashboard that tracks FastAPI throughput, request latency, and blackbox health probes.

After a full run, use:

```bash
make -f makefile/gcp/Makefile collect-metrics
```

The command writes measured evidence to `logs/cloud_runs/<run-id>/cloud-metrics.md`, including producer log rates, PostgreSQL row counts, end-to-end latency, Kafka offsets, Prometheus samples, and Docker service status.
