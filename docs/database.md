# Database Guide

This project uses PostgreSQL + PostGIS as the serving store for the dashboard.

## Core Tables

Two tables are maintained by the Flink streaming job:

| Table | Source | Purpose |
| --- | --- | --- |
| `traffic_risk_predictions` | US replay + MLflow/H2O inference | US replay serving table |
| `traffic_tomtom_incidents` | TomTom live + rule-based severity | TomTom live serving table |

The backend reads both tables and merges them based on the requested `mode`.

## Writers

`processing/flink_streaming.py` creates and upserts both tables:

- `ensure_us_schema()` and `insert_us_prediction()`
- `ensure_tomtom_schema()` and `insert_tomtom_incident()`

## US Replay Table (traffic_risk_predictions)

Key columns:

- `event_id` (PK)
- `event_time`, `event_year`
- `lat`, `lon`, `geom`
- `true_severity`, `predicted_severity`, `risk_score`
- `model_status`, `inference_latency_ms`
- `ingestion_time`, `processed_time`, `end_to_end_latency_ms`

US `risk_score` and `predicted_severity` come from MLflow serving.

## TomTom Live Table (traffic_tomtom_incidents)

Key columns:

- `event_id` (PK)
- `event_time`, `lat`, `lon`, `geom`
- `severity` (rule-based), `risk_score` (derived for display)
- `icon_category`, `delay_magnitude`, `delay_seconds`, `length_meters`
- `state_or_region`, `city`, `from_road`, `to_road`, `geometry_wkt`
- `model_status` (always `rule_based`)
- `ingestion_time`, `processed_time`, `end_to_end_latency_ms`

TomTom events do not go through Spark, MLflow, or H2O. The risk score is
derived directly from the rule-based severity for visualization only.

## Notes

- If a table does not exist yet, the backend returns empty responses rather
  than failing with HTTP 500.
- PostGIS is enabled to support bbox queries and distance-based hotspots.