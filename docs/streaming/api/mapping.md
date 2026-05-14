# Mapping TomTom API -> PostgreSQL/Training

Target table: `public.traffic_risk_predictions`

Primary key: `event_id`

PostGIS indexes:

- `idx_traffic_risk_predictions_geom` on `geom`
- `idx_traffic_risk_predictions_risk_score` on `risk_score`
- `idx_traffic_risk_predictions_time` on `event_timestamp`

## Mapping Contract

| Column | Source | Transform rule | Default/null policy | Owner stage |
| --- | --- | --- | --- | --- |
| `event_id` | TomTom `properties.id` | Prefix as `tomtom-{id}` | Required | Producer |
| `latitude` | GeoJSON first point | Coordinates are `[lon, lat]` | Required for valid event | Producer |
| `longitude` | GeoJSON first point | Coordinates are `[lon, lat]` | Required for valid event | Producer |
| `geom` | `longitude`, `latitude` | `ST_SetSRID(ST_MakePoint(lon, lat), 4326)` | Null only if coordinates invalid | PostGIS sink |
| `grid_cell_id` | `latitude`, `longitude` | Internal grid id from enrichment bounds | Required after enrichment | Normalize/enrich |
| `risk_score` | Model output | Parse MLflow response risk score | `-1` if model fails | Inference |
| `risk_level` | `risk_score` | `0=unknown`, `1..5` converted to label in DB row | `unknown` if model fails | Inference/PostGIS sink |
| `severity` | TomTom `magnitudeOfDelay`, `iconCategory` | Normalize to 1-4 | `1` if no signal | Producer/enrich |
| `speed` | TomTom Incident Details | Not provided by API | `0.0` | Producer |
| `weather_condition` | Open-Meteo | Current weather code label | `unknown` on lookup failure | Normalize/enrich |
| `road_type` | Redis road feature store if available | Physical road type, not TomTom `iconCategory` | `unknown` | Normalize/enrich |
| `event_timestamp` | TomTom `startTime`, fallback `lastReportTime` | ISO-8601 string parsed by DB/Spark | Required for valid event | Producer |
| `prediction_timestamp` | Inference time | UTC ISO timestamp | Current UTC time | Inference |
| `source` | Constant | `tomtom` | `tomtom` | Producer |
| `lat` | `latitude` | Alias for table compatibility | Same as `latitude` | Normalize/enrich |
| `lng` | `longitude` | Alias for table compatibility | Same as `longitude` | Normalize/enrich |
| `lon` | `longitude` | Alias for table compatibility | Same as `longitude` | Normalize/enrich |
| `predicted_severity` | `risk_score` | Convert risk score to 1-4 | `0` if model fails | Inference |
| `true_severity` | Normalized TomTom severity | Same value as `severity` | Same as `severity` | Producer/enrich |
| `event_time` | `event_timestamp` | Alias for batch/training compatibility | Same as `event_timestamp` | Normalize/enrich |
| `model_status` | Inference status | `SUCCESS` or `FAILED` | `FAILED` on fallback | Inference |
| `hour` | `event_timestamp` | UTC hour 0-23 | Derived | Normalize/enrich |
| `weather_code` | Open-Meteo weather label | Project weather code string | `0` unknown | Normalize/enrich |
| `event_year` | `event_timestamp` | UTC year | Derived | Normalize/enrich |
| `temperature_f` | Open-Meteo | Current temperature in Fahrenheit | `0.0` on failure | Normalize/enrich |
| `humidity` | Open-Meteo | Relative humidity percent | `0.0` on failure | Normalize/enrich |
| `wind_speed_mph` | Open-Meteo | Current wind speed in mph | `0.0` on failure | Normalize/enrich |
| `visibility_mi` | Open-Meteo | Visibility meters converted to miles | `0.0` on failure | Normalize/enrich |
| `road_type_code` | `road_type` | `unknown=0`, `highway=1`, `major=2`, `minor=3`, `local=4` | `0` | Normalize/enrich |
| `day_of_week` | `event_timestamp` | Spark-style `1=Sunday..7=Saturday` | Derived | Normalize/enrich |
| `is_weekend` | `event_timestamp` | `1` for Sat/Sun else `0` | Derived | Normalize/enrich |
| `is_rush_hour` | `event_timestamp` | `1` for weekday 7-9 or 17-19 UTC else `0` | Derived | Normalize/enrich |
| `is_junction` | Redis road feature store | Boolean encoded as `0/1` | `0` | Normalize/enrich |
| `has_traffic_signal` | Redis road feature store | Boolean encoded as `0/1` | `0` | Normalize/enrich |
| `is_crossing` | Redis road feature store | Boolean encoded as `0/1` | `0` | Normalize/enrich |
| `is_roundabout` | Redis road feature store | Boolean encoded as `0/1` | `0` | Normalize/enrich |
| `is_stop` | Redis road feature store | Boolean encoded as `0/1` | `0` | Normalize/enrich |
| `is_station` | Redis road feature store | Boolean encoded as `0/1` | `0` | Normalize/enrich |
| `is_railway` | Redis road feature store | Boolean encoded as `0/1` | `0` | Normalize/enrich |
| `is_night` | `event_timestamp` | `1` for 22:00-05:59 UTC else `0` | Derived | Normalize/enrich |
| `inference_latency_ms` | Inference timer | Model request latency | `0.0` if model not called | Inference |
| `ingestion_time` | Producer clock | UTC ISO timestamp when API response is parsed | Current UTC time | Producer |
| `processed_time` | Enrichment clock | UTC ISO timestamp after enrichment | Current UTC time | Normalize/enrich |
| `end_to_end_latency_ms` | `event_timestamp`, `prediction_timestamp` | Non-negative delta in ms | `0.0` if timestamp invalid | Inference |
| `created_at` | PostgreSQL default | `now()` | DB default | PostGIS |

## TomTom Fields Kept for Trace/Training

These fields are kept in Kafka/bronze/enriched payloads even though they are not
first-class columns in `traffic_risk_predictions`:

| Field | TomTom source | Purpose |
| --- | --- | --- |
| `incident_id` | `properties.id` | Stable TomTom id |
| `icon_category` | `properties.iconCategory` | Incident type signal |
| `delay_magnitude` | `properties.magnitudeOfDelay` | Severity/risk signal |
| `delay_seconds` | `properties.delay` | Delay signal |
| `length_meters` | `properties.length` | Affected segment length |
| `incident_code` | `properties.events[0].code` | Warning code |
| `incident_description` | `properties.events[0].description` | Human-readable context |
| `geometry_wkt` | GeoJSON geometry | Audit/spatial debugging |
| `state_or_region`, `city` | Configured bbox region | US/UK regional alignment |

## Missing Field Strategy

- Weather is enriched through Open-Meteo by default because it does not require an API key.
- Road infrastructure flags default to `0` unless Redis/PostGIS road attributes exist.
- `road_type` stays `unknown` unless a physical road type source exists. Do not map TomTom `iconCategory` to `road_type`.
- `true_severity` for TomTom equals normalized incident severity; `predicted_severity` comes from model risk output.
