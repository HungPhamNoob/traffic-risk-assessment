"""Unit tests for TomTom incident producer parsing."""

import sys
import types


class DummyProducer:
    def __init__(self, *args, **kwargs):
        pass


sys.modules["confluent_kafka"] = types.SimpleNamespace(Producer=DummyProducer)
sys.modules.setdefault("dotenv", types.SimpleNamespace(load_dotenv=lambda: None))

from ingestion.kafka.tomtom_producer import (  # noqa: E402
    first_coordinate,
    geometry_to_wkt,
    normalize_incident,
)


def test_tomtom_incident_is_normalized_to_raw_event_contract():
    incident = {
        "properties": {
            "id": "TTI-1",
            "iconCategory": 6,
            "magnitudeOfDelay": 3,
            "startTime": "2026-05-12T06:00:00Z",
            "from": "I-95 N",
            "delay": 180,
        },
        "geometry": {
            "type": "LineString",
            "coordinates": [[-74.001, 40.73], [-73.995, 40.735]],
        },
    }

    event = normalize_incident(incident, "US", "New York")

    assert event is not None
    assert event["event_id"] == "tomtom-TTI-1"
    assert event["source"] == "tomtom"
    assert event["latitude"] == 40.73
    assert event["longitude"] == -74.001
    assert event["state_or_region"] == "US"
    assert event["city"] == "New York"
    assert event["speed"] == 0.0
    assert event["geometry_wkt"] == "LINESTRING (-74.001 40.73, -73.995 40.735)"


def test_tomtom_geometry_helpers_use_geojson_lon_lat_order():
    geometry = {"type": "Point", "coordinates": [-73.99, 40.75]}

    assert first_coordinate(geometry) == (40.75, -73.99)
    assert geometry_to_wkt(geometry) == "POINT (-73.99 40.75)"
