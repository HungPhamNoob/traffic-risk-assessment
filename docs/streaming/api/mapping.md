# Mapping TomTom API -> PostgreSQL

Target table: `public.traffic_tomtom_incidents`

Primary key: `event_id`

PostGIS indexes:

- `idx_traffic_tomtom_incidents_geom` on `geom`
- `idx_traffic_tomtom_incidents_event_time` on `event_time`
- `idx_traffic_tomtom_incidents_severity` on `severity`
- `idx_traffic_tomtom_incidents_rule_score` on `tomtom_rule_score`

TomTom live incidents are rule-based. They do not call MLflow, do not write US
Silver features for Spark, and do not participate in H2O retraining.

## Mapping Contract

| Column | Source | Transform rule | Default | Owner stage |
| --- | --- | --- | --- | --- |
| `event_id` | TomTom `properties.id` | Prefix as `tomtom-{id}` | Required | Producer |
| `incident_id` | TomTom `properties.id` | Stable TomTom id | Fallback to `event_id` | Producer/Flink |
| `lat` | GeoJSON first point | Coordinates are `[lon, lat]` | Required | Producer/Flink |
| `lon` | GeoJSON first point | Coordinates are `[lon, lat]` | Required | Producer/Flink |
| `geom` | `lon`, `lat` | `ST_SetSRID(ST_MakePoint(lon, lat), 4326)` | Not written if coordinates invalid | PostGIS sink |
| `event_time` | `startTime`, fallback `lastReportTime` | Parse ISO-8601 timestamp | Required | Producer/Flink |
| `severity` | `magnitudeOfDelay`, `iconCategory` | Normalize to 1-4 | `1` if no signal | Flink |
| `tomtom_rule_score` | `severity` | `(severity - 1) / 3` | Derived | Flink |
| `icon_category` | `properties.iconCategory` | Integer | Nullable | Producer |
| `delay_magnitude` | `properties.magnitudeOfDelay` | Integer | Nullable | Producer |
| `delay_seconds` | `properties.delay` | Float seconds | Nullable | Producer |
| `length_meters` | `properties.length` | Float meters | Nullable | Producer |
| `incident_code` | `properties.events[0].code` | Integer | Nullable | Producer |
| `incident_description` | `properties.events[0].description` | Text | Empty string if missing | Producer |
| `from_road` | `properties.from` | Text | Empty string if missing | Producer |
| `to_road` | `properties.to` | Text | Empty string if missing | Producer |
| `road_numbers` | `properties.roadNumbers` | JSONB array | `[]` | Producer/Flink |
| `time_validity` | `properties.timeValidity` | Text | Empty string if missing | Producer |
| `probability_of_occurrence` | `properties.probabilityOfOccurrence` | Text | Empty string if missing | Producer |
| `number_of_reports` | `properties.numberOfReports` | Integer | Nullable | Producer |
| `last_report_time` | `properties.lastReportTime` | Parse ISO-8601 timestamp | Nullable | Producer/Flink |
| `ingestion_time` | Producer clock | Parse ISO-8601 timestamp | Nullable | Producer/Flink |
| `processed_time` | Flink clock | UTC processing time | Current UTC time | Flink |
| `processing_latency_ms` | Flink timer | Message processing latency | Nullable | Flink |
| `raw_payload` | Full TomTom incident/event | JSONB | `{}` if missing | Producer/Flink |
| `created_at` | PostgreSQL default | `now()` | DB default | PostGIS |

## Severity Rule

| TomTom signal | `severity` |
| --- | --- |
| `magnitudeOfDelay >= 4` | `4` |
| `magnitudeOfDelay == 3` | `3` |
| `magnitudeOfDelay == 2` | `2` |
| `magnitudeOfDelay == 0/1/null` | `1` |
| `iconCategory == 8` road closure | at least `4` |
| `iconCategory == 1` accident | at least `3` |
| `iconCategory == 9` roadworks | at least `2` |

`tomtom_rule_score` is only a TomTom display/ranking score. It is not the same
semantic as the US H2O `risk_score` stored in `traffic_risk_predictions`.
