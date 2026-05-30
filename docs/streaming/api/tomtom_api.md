# TomTom Traffic API for Streaming

## Endpoint

The streaming producer uses TomTom Traffic **Incident Details**:

```text
GET https://api.tomtom.com/traffic/services/5/incidentDetails
```

Incident Details provides incident-centric signals (`iconCategory`,
`magnitudeOfDelay`, geometry) which are required for rule-based severity.

Official docs:

- https://docs.tomtom.com/traffic-api/documentation/tomtom-maps/traffic-incidents/incident-details

## Request Parameters

Configured through environment variables in `ingestion/kafka/tomtom_producer.py`:

| Env var | Default | Purpose |
| --- | --- | --- |
| `TOMTOM_ENDPOINT` | `https://api.tomtom.com/traffic/services/5/incidentDetails` | API endpoint |
| `TOMTOM_API_KEY` | empty | API key |
| `TOMTOM_BBOX` | empty | Single bbox override `minLon,minLat,maxLon,maxLat` |
| `TOMTOM_BBOXES` | `US:New York:...` | Multi-region bboxes |
| `TOMTOM_LANGUAGE` | `en-US` | Incident text language |
| `TOMTOM_TIME_VALIDITY` | `present` | Only currently valid incidents |
| `TOMTOM_FIELDS` | configured | Select response fields |

Default fields are tuned for incidents and delay signals.

## Response Shape

Incident Details returns GeoJSON-like features:

```json
{
  "incidents": [
    {
      "type": "Feature",
      "properties": {
        "id": "incident-id",
        "iconCategory": 6,
        "magnitudeOfDelay": 2,
        "startTime": "2026-05-12T06:00:00Z",
        "from": "Road A",
        "to": "Road B",
        "length": 850.0,
        "delay": 180,
        "lastReportTime": "2026-05-12T06:05:00Z",
        "events": [
          { "code": 101, "description": "Slow traffic", "iconCategory": 6 }
        ]
      },
      "geometry": {
        "type": "LineString",
        "coordinates": [[-74.001, 40.73], [-73.995, 40.735]]
      }
    }
  ]
}
```

Coordinates are `[lon, lat]`.

## Pipeline Usage

The TomTom stream is **live-only** and does not enter Spark, MLflow, or H2O.

```text
TomTom API
-> Kafka topic traffic.tomtom.raw
-> processing/flink_tomtom_streaming.py
-> PostgreSQL table traffic_tomtom_incidents
-> Dashboard Live mode
```

The US stream remains separate:

```text
US replay
-> Kafka topic traffic.us.raw
-> processing/flink_streaming.py
-> Silver GCS + MLflow serving
-> PostgreSQL table traffic_risk_predictions
```

## Raw Kafka Event

These are the fields saved from the TomTom API response before Flink writes the
dedicated PostgreSQL table:

| TomTom response field | Bronze field | Type | Notes |
| --- | --- | --- | --- |
| `properties.id` | `incident_id` | string | Stable TomTom incident identifier |
| `properties.id` | `flow_segment_id` | string | Reused as streaming key because Incident Details has no flow segment id |
| generated | `event_id` | string | `tomtom-{incident_id}` |
| constant | `source` | string | `tomtom` |
| first geometry point | `latitude` | double | GeoJSON coordinate latitude |
| first geometry point | `longitude` | double | GeoJSON coordinate longitude |
| `properties.startTime` fallback `lastReportTime` | `timestamp` | string | Event timestamp |
| constant | `speed` | double | `0.0`, only for raw streaming compatibility |
| `properties.iconCategory` | `icon_category` | int | Incident category/type |
| `properties.magnitudeOfDelay` | `delay_magnitude` | int | Delay severity bucket |
| `properties.delay` | `delay_seconds` | int | Delay duration in seconds, nullable |
| `properties.length` | `length_meters` | double | Affected length in meters |
| `geometry` | `geometry_wkt` | string | WKT `POINT` or `LINESTRING` |
| `properties.events[0].description` | `incident_description` | string | Human-readable incident detail |
| `properties.events[0].code` | `incident_code` | int | TomTom warning code |
| `properties.from` | `from_road` | string | Start affected road/place |
| `properties.to` | `to_road` | string | End affected road/place |
| `properties.roadNumbers` | `road_numbers` | array<string> | Affected road numbers |
| `properties.timeValidity` | `time_validity` | string | `present` or `future` |
| `properties.probabilityOfOccurrence` | `probability_of_occurrence` | string | Probability label |
| `properties.numberOfReports` | `number_of_reports` | int | End-user report count |
| `properties.lastReportTime` | `last_report_time` | string | Latest report timestamp |
| whole incident feature | `raw_payload` | object | Kept for audit/debug |

The raw Kafka event still keeps the fields required by
`docs/streaming/streaming_details.md`:

```json
{
  "event_id": "tomtom-incident-id",
  "source": "tomtom",
  "flow_segment_id": "incident-id",
  "latitude": 40.73,
  "longitude": -74.001,
  "speed": 0.0,
  "timestamp": "2026-05-12T06:00:00Z",
  "raw_payload": {}
}
```

## PostgreSQL Projection

TomTom records are processed by `processing/flink_tomtom_streaming.py` into
`traffic_tomtom_incidents`.

| Table field | TomTom source | Rule |
| --- | --- | --- |
| `event_id` | raw `event_id` | Already prefixed with `tomtom-` |
| `incident_id` | raw `incident_id` | Stable TomTom id |
| `event_time` | `timestamp`, fallback `last_report_time` | Parsed timestamp |
| `lat` | `latitude` | Required |
| `lon` | `longitude` | Required |
| `severity` | `delay_magnitude`, `icon_category` | Normalized to 1-4 |
| `tomtom_rule_score` | `severity` | `(severity - 1) / 3` |
| `geom` | `lon`, `lat` | PostGIS point |
| `raw_payload` | raw payload | JSONB audit/debug payload |

## Severity Rule

| TomTom signal | Severity |
| --- | --- |
| `magnitudeOfDelay >= 4` | `4` |
| `magnitudeOfDelay == 3` | `3` |
| `magnitudeOfDelay == 2` | `2` |
| `magnitudeOfDelay <= 1` | `1` |
| `iconCategory == 8` | at least `4` |
| `iconCategory == 1` | at least `3` |
| `iconCategory == 9` | at least `2` |

Do not map `iconCategory` into US model features. TomTom `severity` and
`tomtom_rule_score` are rule-based live incident signals, not H2O predictions.
