# Database Guide

This project uses PostgreSQL + PostGIS as the serving store for the API and
dashboard.

## Core Tables

Two independent Flink jobs maintain two serving tables:

| Table | Source | Writer | Purpose |
| --- | --- | --- | --- |
| `traffic_risk_predictions` | US replay + MLflow/H2O inference | `processing/flink_streaming.py` | US replay ML prediction table |
| `traffic_tomtom_incidents` | TomTom live + rule-based severity | `processing/flink_tomtom_streaming.py` | TomTom live incident table |

The backend reads both tables and merges them based on the requested dashboard
mode. Backend JSON responses are served from PostgreSQL, not directly from
Redis, Kafka, Gold parquet, Spark, or MLflow.

## Writers

### US replay writer

File: `processing/flink_streaming.py`

Tasks:

1. Read raw US replay events from Kafka topic `traffic.us.raw`.
2. Build features with `processing.feature_engineering.build_features`.
3. Write US feature JSONL records to the Silver path for Spark/H2O retraining.
4. Call MLflow serving for H2O prediction.
5. Upsert into `traffic_risk_predictions`.

This is the production writer for US replay predictions.

### TomTom live writer

File: `processing/flink_tomtom_streaming.py`

Tasks:

1. Read TomTom raw events from Kafka topic `traffic.tomtom.raw`.
2. Compute `severity` with the rule based on `magnitudeOfDelay + iconCategory`.
3. Compute `tomtom_rule_score = (severity - 1) / 3`.
4. Upsert into `traffic_tomtom_incidents`.

This job does not call MLflow, does not write Silver data for Spark, and does
not participate in H2O retraining.

## US Replay Table

Table: `traffic_risk_predictions`

Key columns:

- `event_id` (primary key)
- `event_year`, `event_time`
- `lat`, `lon`, `geom`
- `true_severity`, `predicted_severity`, `risk_score`
- `weather_code`, `temperature_f`, `humidity`, `wind_speed_mph`, `visibility_mi`
- `road_type_code`, `hour`, `day_of_week`
- `is_weekend`, `is_rush_hour`, road/POI flags
- `model_status`, `inference_latency_ms`
- `ingestion_time`, `processed_time`, `end_to_end_latency_ms`
- `created_at`

`predicted_severity` and `risk_score` are H2O/MLflow prediction outputs. This
semantic must not be reused for TomTom.

## TomTom Live Table

Table: `traffic_tomtom_incidents`

Key columns:

- `event_id` (primary key)
- `incident_id`
- `event_time`
- `lat`, `lon`, `geom`
- `severity`
- `tomtom_rule_score`
- `icon_category`, `delay_magnitude`, `delay_seconds`, `length_meters`
- `incident_code`, `incident_description`
- `from_road`, `to_road`, `road_numbers`
- `time_validity`, `probability_of_occurrence`, `number_of_reports`
- `last_report_time`
- `ingestion_time`, `processed_time`, `processing_latency_ms`
- `raw_payload`
- `created_at`

TomTom `severity` is deterministic rule output. `tomtom_rule_score` is only a
TomTom display/ranking score and is not the same semantic as US `risk_score`.

## Data Lineage

### US replay

1. `ingestion/kafka/us_producer.py` reads the post-2020 US replay split.
2. Producer publishes raw rows to Kafka topic `traffic.us.raw`.
3. `processing/flink_streaming.py` builds features, writes Silver JSONL,
   calls MLflow serving, and upserts `traffic_risk_predictions`.
4. Spark reads only the US Silver path and writes Gold data for retraining.
5. H2O retraining consumes Gold data and registers new models in MLflow.

### TomTom live

1. `ingestion/kafka/tomtom_producer.py` calls TomTom Incident Details API.
2. Producer publishes raw incident events to Kafka topic `traffic.tomtom.raw`.
3. `processing/flink_tomtom_streaming.py` computes rule-based severity and
   upserts `traffic_tomtom_incidents`.

TomTom data does not enter Spark, Gold retraining, H2O, or MLflow serving.

## Backend Read Layer

Core files:

- `dashboard/backend/app/core/database.py`
- `dashboard/backend/app/services/prediction_service.py`
- `dashboard/backend/app/services/hotspot_service.py`
- `dashboard/backend/app/services/analytics_service.py`
- `dashboard/backend/app/routes/system.py`

Configuration is read from environment variables:

- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_PREDICTION_TABLE`
- `POSTGRES_US_PREDICTION_TABLE`
- `POSTGRES_TOMTOM_TABLE`

`fetch_one()` and `fetch_all()` handle undefined tables by returning empty
responses. That keeps demos from failing before a writer has created its table,
but an empty API response does not prove the pipeline is healthy.

## Operational Notes

- PostgreSQL must have PostGIS installed and `CREATE EXTENSION IF NOT EXISTS
  postgis` must be allowed for the writer role.
- The project does not currently use Alembic or versioned SQL migrations.
  Runtime writers create/evolve schemas.
- Both writers use `INSERT ... ON CONFLICT (event_id) DO UPDATE`, so each table
  stores the latest state per event id, not a full history of versions.
- Map and analytics endpoints rely heavily on `event_time`; invalid timestamps
  will break latest, timeseries, and filter behavior.

## Quick Checks

TomTom row count:

```sql
SELECT count(*) AS tomtom_incidents, max(event_time) AS latest_tomtom_time
FROM traffic_tomtom_incidents;
```

Check TomTom did not enter the US table:

```sql
SELECT count(*)
FROM traffic_risk_predictions
WHERE event_id LIKE 'tomtom-%';
```

Expected result for the second query is `0`.
