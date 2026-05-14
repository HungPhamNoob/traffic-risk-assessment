"""
Unit tests for TomTom producer parsing.
"""
from ingestion.kafka.producers.tomtom_producer import (
    create_traffic_event,
    create_traffic_events,
    get_tomtom_regions,
    map_tomtom_severity,
)


def _sample_incident():
    return {
        "type": "Feature",
        "properties": {
            "id": "abc123",
            "iconCategory": 6,
            "magnitudeOfDelay": 2,
            "startTime": "2026-05-12T06:00:00Z",
            "from": "Road A",
            "to": "Road B",
            "length": 850.0,
            "delay": 180,
            "roadNumbers": ["QL1A"],
            "timeValidity": "present",
            "probabilityOfOccurrence": "certain",
            "numberOfReports": 3,
            "lastReportTime": "2026-05-12T06:05:00Z",
            "events": [{"code": 101, "description": "Slow traffic", "iconCategory": 6}],
        },
        "geometry": {
            "type": "LineString",
            "coordinates": [[-74.001, 40.73], [-73.995, 40.735]],
        },
    }


def test_create_traffic_event_maps_incident_fields():
    event = create_traffic_event(_sample_incident(), fetched_at="2026-05-12T06:10:00Z")

    assert event["event_id"] == "tomtom-abc123"
    assert event["source"] == "tomtom"
    assert event["flow_segment_id"] == "abc123"
    assert event["latitude"] == 40.73
    assert event["longitude"] == -74.001
    assert event["timestamp"] == "2026-05-12T06:00:00Z"
    assert event["event_timestamp"] == "2026-05-12T06:00:00Z"
    assert event["ingestion_time"] == "2026-05-12T06:10:00Z"
    assert event["speed"] == 0.0
    assert event["severity"] == 2
    assert event["true_severity"] == 2
    assert event["road_type"] == "unknown"
    assert event["incident_id"] == "abc123"
    assert event["icon_category"] == 6
    assert event["delay_magnitude"] == 2
    assert event["delay_seconds"] == 180
    assert event["length_meters"] == 850.0
    assert event["geometry_wkt"] == "LINESTRING (-74.001 40.73, -73.995 40.735)"
    assert event["incident_description"] == "Slow traffic"
    assert event["incident_code"] == 101
    assert event["raw_payload"]["properties"]["id"] == "abc123"


def test_map_tomtom_severity_overrides():
    assert map_tomtom_severity(icon_category=8, delay_magnitude=0) == 4
    assert map_tomtom_severity(icon_category=1, delay_magnitude=1) == 3
    assert map_tomtom_severity(icon_category=9, delay_magnitude=1) == 2
    assert map_tomtom_severity(icon_category=6, delay_magnitude=4) == 4


def test_create_traffic_event_skips_missing_geometry():
    incident = _sample_incident()
    incident["geometry"] = {"type": "LineString", "coordinates": []}

    assert create_traffic_event(incident) is None


def test_create_traffic_events_iterates_incidents():
    response = {"incidents": [_sample_incident(), _sample_incident()]}

    events = list(create_traffic_events(response, region="uk_london", country="UK", city="London"))

    assert len(events) == 2
    assert all(event["incident_id"] == "abc123" for event in events)
    assert all(event["state_or_region"] == "UK" for event in events)
    assert all(event["city"] == "London" for event in events)


def test_get_tomtom_regions_country_city_bbox(monkeypatch):
    monkeypatch.setenv("TOMTOM_BBOX", "")
    monkeypatch.setenv("TOMTOM_BBOXES", "FR:Paris:2.2241,48.8156,2.4699,48.9022")

    regions = get_tomtom_regions()

    assert regions == [
        {
            "region": "fr_paris",
            "country": "FR",
            "city": "Paris",
            "bbox": "2.2241,48.8156,2.4699,48.9022",
        }
    ]
