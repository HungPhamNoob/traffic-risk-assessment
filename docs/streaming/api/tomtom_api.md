# TomTom Traffic API for Streaming

## Endpoint Chosen

Use TomTom Traffic API **Incident Details** for the TomTom streaming producer:

```text
GET https://api.tomtom.com/traffic/services/5/incidentDetails
```

Reason: the batch-side TomTom schema in this repo is incident-oriented
(`schemas/tomtom_incident.avsc`) and stores fields such as `icon_category`,
`delay_seconds`, `length_meters`, and `geometry_wkt`. This project is about
traffic risk, so the streaming source should ingest incident/risk signals rather
than traffic flow speed segments.

Official docs used:

- https://docs.tomtom.com/traffic-api/documentation/tomtom-maps/traffic-incidents/incident-details

## Request Parameters

Configured through environment variables in the producer:

| Env var | Default | Purpose |
| --- | --- | --- |
| `TOMTOM_ENDPOINT` | `https://api.tomtom.com/traffic/services/5/incidentDetails` | API endpoint |
| `TOMTOM_API_KEY` | empty | TomTom API key |
| `TOMTOM_BBOX` | empty | Optional single bbox override: `minLon,minLat,maxLon,maxLat` |
| `TOMTOM_BBOXES` | `US:New York:-74.25909,40.477399,-73.700181,40.917577;UK:London:-0.510375,51.28676,0.334015,51.691874` | Multi-region bboxes |
| `TOMTOM_LANGUAGE` | `en-US` | Incident text language |
| `TOMTOM_TIME_VALIDITY` | `present` | Only currently valid incidents |
| `TOMTOM_FIELDS` | all fields used by this pipeline | Controls response shape |

Default fields:

```text
{incidents{type,geometry{type,coordinates},properties{id,iconCategory,
magnitudeOfDelay,events{description,code,iconCategory},startTime,endTime,
from,to,length,delay,roadNumbers,timeValidity,probabilityOfOccurrence,
numberOfReports,lastReportTime}}}
```

## Response Shape

Incident Details returns JSON. The exact fields depend on the `fields` request
parameter. The root response contains `incidents`, an array of traffic incidents
inside or intersecting the requested bbox. Each incident is GeoJSON-like:

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
        "endTime": null,
        "from": "Road A",
        "to": "Road B",
        "length": 850.0,
        "delay": 180,
        "roadNumbers": [],
        "timeValidity": "present",
        "probabilityOfOccurrence": "certain",
        "numberOfReports": 1,
        "lastReportTime": "2026-05-12T06:05:00Z",
        "events": [
          {
            "code": 101,
            "description": "Slow traffic",
            "iconCategory": 6
          }
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

GeoJSON coordinates are ordered as `[longitude, latitude]`.

Important `properties` fields:

| Field | Meaning |
| --- | --- |
| `id` | Traffic incident ID |
| `iconCategory` | Main incident category |
| `magnitudeOfDelay` | Delay magnitude bucket |
| `events[].description` | Localized incident description |
| `events[].code` | Predefined warning code |
| `startTime`, `endTime` | Incident validity timestamps |
| `from`, `to` | Start/end affected road names |
| `length` | Affected length in meters |
| `delay` | Delay in seconds |
| `roadNumbers` | Affected road numbers |
| `timeValidity` | `present` or `future` |
| `probabilityOfOccurrence` | Probability label |
| `numberOfReports` | Number of end-user reports |
| `lastReportTime` | Most recent report timestamp |

For invalid requests, TomTom returns JSON under `detailedError` with `code` and
`message`.

## Mapping to Streaming Fields

The active implementation uses two contracts:

1. **TomTom raw Kafka event**: keeps TomTom incident-specific fields.
2. **TomTom PostGIS table**: stores live rule-based incidents in
   `traffic_tomtom_incidents`.

TomTom does not write US Silver features, does not feed Spark/H2O, and does not
call MLflow serving.

### Raw Kafka Event

These are the fields that should be saved from the TomTom API response before
Flink writes the dedicated PostgreSQL table:

| TomTom response field | Bronze field | Type | Notes |
| --- | --- | --- |
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
| whole incident feature | `raw_payload` | object | Kept in streaming raw event for audit/debug |

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

`speed` is set to `0.0` only to satisfy the existing raw streaming event shape.
It is not used as the TomTom risk signal. The TomTom risk-relevant fields are
`icon_category`, `delay_magnitude`, `delay_seconds`, `length_meters`, event
description/code, timestamps, and geometry.

### PostgreSQL Projection

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
| `raw_payload` | raw payload | Kept as JSONB for audit/debug |

Severity normalization:

| TomTom signal | severity |
| --- | --- |
| `magnitudeOfDelay >= 4` | `4` |
| `magnitudeOfDelay == 3` | `3` |
| `magnitudeOfDelay == 2` | `2` |
| `magnitudeOfDelay == 0/1/null` | `1` |
| `iconCategory == 8` road closure | at least `4` |
| `iconCategory == 1` accident | at least `3` |
| `iconCategory == 9` roadworks | at least `2` |

Do not map `iconCategory` into US model features. TomTom `severity` and
`tomtom_rule_score` are rule-based live incident signals, not H2O predictions.

## Direct Request Result

On 2026-05-12, a live request was tested with the workspace `.env`
`TOMTOM_API_KEY`.

```text
GET https://api.tomtom.com/traffic/services/5/incidentDetails
  bbox=-74.25909,40.477399,-73.700181,40.917577
  language=en-US
  timeValidityFilter=present
  fields={incidents{type,geometry{type,coordinates},properties{id,...}}}
  key=<TOMTOM_API_KEY>
```

Result:

```text
HTTP 200
content-type: application/json; charset=utf-8
us_new_york incident count: 359
uk_london incident count: 912
```

Sample parsed fields:

```json
{
  "event_id": "tomtom-TTI-...",
  "state_or_region": "US",
  "city": "New York",
  "latitude": 40.8850863629,
  "longitude": -74.2590125497,
  "icon_category": 6,
  "delay_magnitude": 3
}
```

Current defaults use New York and London bboxes so TomTom streaming aligns with
the US/UK batch datasets. These are urban demo bboxes, not full country extents.
