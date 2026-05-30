# API Reference

## Overview

The backend is a FastAPI service located at
`dashboard/backend/app/app.py`.

Base URLs:

- Local: `http://localhost:8000`
- Cloud: `http://<node1-external-ip>:8000`

All dashboard endpoints are under `/api/v1`.

Extra endpoints:

- `GET /health`
- `GET /metrics`

## Common Conventions

- Timestamps are returned as ISO-8601 strings.
- `mode` controls which dataset is queried:
  - `replay` = US replay only
  - `live` = TomTom live only
  - `full` = both datasets
- `risk_level` uses this mapping:
  - `high`: `risk_score >= 0.7`
  - `medium`: `0.4 <= risk_score < 0.7`
  - `low`: `risk_score < 0.4`
- `bbox` format: `min_lon,min_lat,max_lon,max_lat`.
- If the target tables do not exist yet, endpoints return empty payloads
  instead of HTTP 500.

## Health and Metrics

### `GET /health`

Returns a lightweight health response for load balancers and CI.

### `GET /metrics`

Prometheus metrics in text format. The FastAPI middleware records request
count and latency buckets.

## Overview

### `GET /api/v1/overview/summary`

Returns aggregate KPIs for the requested `mode` (default: `full`).

Example output:

```json
{
  "total_events": 125000,
  "high_risk_events": 18400,
  "avg_risk_score": 0.4381,
  "latest_event_time": "2026-05-26T10:31:06+00:00",
  "latest_model_version": "US H2O + TomTom rule-based",
  "mode": "full"
}
```

## Predictions

### `GET /api/v1/predictions/map`

Returns map points for the dashboard.

Query parameters:

| Param | Type | Default | Notes |
| --- | --- | --- | --- |
| `bbox` | string | `null` | `min_lon,min_lat,max_lon,max_lat` |
| `min_risk` | float | `0.0` | Range `0..1` |
| `start_time` | string | `null` | ISO timestamp |
| `end_time` | string | `null` | ISO timestamp |
| `limit` | int | `5000` | Range `1..20000` |
| `mode` | string | `full` | `replay`, `live`, or `full` |

Each point includes:

- `data_source`: `us_replay` or `tomtom_live`
- `marker_shape`: `circle` for US, `triangle` for TomTom
- `predicted_severity` / `true_severity`

For TomTom points, `risk_score` is derived from the rule-based severity.

### `GET /api/v1/predictions/latest`

Returns the most recent prediction rows with the same schema as map points.

### `GET /api/v1/predictions/{event_id}`

Returns a single event from either table when available.

## Hotspots

### `GET /api/v1/hotspots`

Hotspots are computed from the US replay prediction table only.

### `GET /api/v1/hotspots/nearby`

Returns nearby events around a lat/lon point.

## Analytics

- `GET /api/v1/analytics/risk-by-hour`
- `GET /api/v1/analytics/severity-distribution`
- `GET /api/v1/analytics/timeseries`

## Pipeline Health

- `GET /api/v1/pipeline/throughput`
- `GET /api/v1/pipeline/latency`
- `GET /api/v1/pipeline/checkpoints`
- `GET /api/v1/pipeline/replay-health`

These endpoints query PostgreSQL and provide best-effort runtime metrics for
the dashboard health views and Grafana panels.