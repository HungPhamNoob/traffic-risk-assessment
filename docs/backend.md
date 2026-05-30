# Backend Guide

This document explains how the dashboard backend works and where to change
behavior safely.

## What the Backend Does

The backend lives in `dashboard/backend` and is a FastAPI service. It does not
run Kafka, Flink, Spark, or H2O training jobs. Its responsibilities are:

1. Read US replay and TomTom live tables from PostgreSQL/PostGIS.
2. Expose JSON APIs for the dashboard.
3. Call MLflow serving for scenario (what-if) endpoints only.
4. Export Prometheus metrics under `/metrics`.

## Data Flow Used by the Backend

US replay (post-2020):

1. Kafka topic `traffic.us.raw`
2. Flink feature engineering + MLflow inference
3. PostgreSQL table `traffic_risk_predictions`

TomTom live:

1. Kafka topic `traffic.tomtom.raw`
2. Flink enrichment + rule-based severity
3. PostgreSQL table `traffic_tomtom_incidents`

The backend reads both tables and merges them on demand based on the `mode`
parameter (`replay`, `live`, `full`).

## Key Files

- `dashboard/backend/app/app.py`: FastAPI entrypoint, CORS, metrics middleware.
- `dashboard/backend/app/core/config.py`: settings from `.env.cloud` and `.env`.
- `dashboard/backend/app/core/database.py`: PostgreSQL access helpers.
- `dashboard/backend/app/routes/*.py`: HTTP layer only.
- `dashboard/backend/app/services/*.py`: query and formatting logic.

## Practical Examples

### `GET /api/v1/predictions/map`

Flow:

1. Route: `dashboard/backend/app/routes/predictions.py`
2. Service: `dashboard/backend/app/services/prediction_service.py::map_points`
3. PostgreSQL query by mode
4. Response formatting:
   - ISO timestamps
   - `risk_level` mapping
   - `data_source` and `marker_shape` hints

### Scenario Prediction

Scenario endpoints (`/api/v1/scenarios/*`) call MLflow serving directly and do
not depend on PostgreSQL row contents.

## Common Edits

- Change KPI aggregates: `prediction_service.py::overview_summary`
- Adjust map filters: `prediction_service.py::map_points`
- Update pipelines status: `pipeline_service.py`
- Modify scenario inputs: `schemas/scenario.py`