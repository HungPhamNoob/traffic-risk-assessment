# Mapping TomTom API -> PostgreSQL (Live)

Target table: `public.traffic_tomtom_incidents`

Primary key: `event_id`

PostGIS geometry: `geom` is created from `lon` and `lat`.

## Mapping Contract

| Column | Source | Transform rule | Default | Owner stage |
| --- | --- | --- | --- | --- |
| `event_id` | TomTom `properties.id` | Prefix with `tomtom-` | Required | Producer |
| `incident_id` | TomTom `properties.id` | Raw id for traceability | Nullable | Producer |
| `event_time` | `startTime` or `lastReportTime` | ISO-8601 string | Required | Producer |
| `lat`, `lon` | GeoJSON coordinates | First point of geometry | Required | Producer |
| `severity` | `magnitudeOfDelay`, `iconCategory` | Normalize to 1-4 | 1 | Enrichment |
| `risk_score` | `severity` | `(severity - 1) / 3` | 0.0 | Enrichment |
| `icon_category` | `iconCategory` | Integer cast | Nullable | Producer |
| `delay_magnitude` | `magnitudeOfDelay` | Integer cast | Nullable | Producer |
| `delay_seconds` | `delay` | Float cast | Nullable | Producer |
| `length_meters` | `length` | Float cast | Nullable | Producer |
| `state_or_region` | Configured bbox name | Static label | Nullable | Producer |
| `city` | Configured bbox name | Static label | Nullable | Producer |
| `from_road`, `to_road` | `from`, `to` | Raw strings | Nullable | Producer |
| `geometry_wkt` | GeoJSON geometry | WKT line/point | Nullable | Producer |
| `weather_code` | Open-Meteo | Project weather code | 0 | Enrichment |
| `temperature_f`, `humidity`, `wind_speed_mph`, `visibility_mi` | Open-Meteo | Float cast | Defaults | Enrichment |
| `road_type_code` | Road text | Encoded road category | 0 | Enrichment |
| `hour`, `day_of_week` | `event_time` | Derived in UTC | Derived | Enrichment |
| `is_weekend`, `is_rush_hour`, `is_junction`, `has_traffic_signal`, `is_crossing`, `is_roundabout`, `is_stop`, `is_station`, `is_railway`, `is_night` | Derived flags | `0/1` | Defaults | Enrichment |
| `model_status` | Constant | `rule_based` | `rule_based` | Flink sink |
| `ingestion_time` | Producer clock | UTC ISO | Current UTC | Producer |
| `processed_time` | Flink clock | UTC ISO | Current UTC | Flink sink |
| `end_to_end_latency_ms` | `ingestion_time` and `processed_time` | Delta in ms | Nullable | Flink sink |
| `geom` | `lon`, `lat` | PostGIS point | Derived | Flink sink |
| `created_at` | PostgreSQL default | `now()` | DB default | PostgreSQL |

## Notes

- TomTom live records do not go to Spark, MLflow, or H2O.
- `risk_score` is used only for map coloring and risk level badges.