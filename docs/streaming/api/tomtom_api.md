# TomTom Traffic API for Streaming

## Endpoint

The streaming producer uses TomTom Traffic **Incident Details**:

```
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
| `TOMTOM_BBOXES` | `US:New York:...;UK:London:...` | Multi-region bboxes |
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

Flow:

```
TomTom API -> Kafka topic traffic.tomtom.raw -> Flink enrichment
-> PostgreSQL table traffic_tomtom_incidents -> Dashboard (Live mode)
```

Severity normalization (rule-based):

| TomTom signal | Severity |
| --- | --- |
| `magnitudeOfDelay >= 4` | `4` |
| `magnitudeOfDelay == 3` | `3` |
| `magnitudeOfDelay == 2` | `2` |
| `magnitudeOfDelay <= 1` | `1` |
| `iconCategory == 8` | at least `4` |
| `iconCategory == 1` | at least `3` |
| `iconCategory == 9` | at least `2` |

The dashboard uses a derived `risk_score = (severity - 1) / 3` for coloring and
risk levels.