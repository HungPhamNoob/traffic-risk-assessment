"""Unit tests for TomTom incident producer parsing."""

import sys
import types


class DummyProducer:
    def __init__(self, *args, **kwargs):
        pass


sys.modules["confluent_kafka"] = types.SimpleNamespace(Producer=DummyProducer)
sys.modules.setdefault("dotenv", types.SimpleNamespace(load_dotenv=lambda: None))

from ingestion.kafka.tomtom_producer import (  # noqa: E402
    KAFKA_TOPIC,
    event_state_signature,
    first_coordinate,
    geometry_to_wkt,
    normalize_incident,
    publish_event_if_changed,
    resolve_kafka_topic,
)


class RecordingProducer:
    def __init__(self):
        self.messages = []

    def produce(self, topic, key, value, callback):
        self.messages.append({"topic": topic, "key": key, "value": value})
        callback(None, None)

    def poll(self, timeout):
        return None


def test_tomtom_producer_defaults_to_tomtom_topic():
    assert KAFKA_TOPIC == "traffic.tomtom.raw"


def test_tomtom_producer_ignores_shared_us_topic(monkeypatch):
    monkeypatch.delenv("KAFKA_TOPIC_TOMTOM_RAW", raising=False)
    monkeypatch.setenv("KAFKA_TOPIC_RAW", "traffic.us.raw")

    assert resolve_kafka_topic() == "traffic.tomtom.raw"


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


def test_tomtom_state_signature_ignores_ingestion_time():
    event = {
        "event_id": "tomtom-TTI-1",
        "timestamp": "2026-05-12T06:00:00Z",
        "delay_magnitude": 3,
        "icon_category": 6,
        "geometry_wkt": "LINESTRING (-74.001 40.73, -73.995 40.735)",
        "ingestion_time": "2026-05-12T06:00:01Z",
    }
    same_state = {**event, "ingestion_time": "2026-05-12T06:01:01Z"}

    assert event_state_signature(event) == event_state_signature(same_state)


def test_tomtom_same_event_state_is_published_once():
    producer = RecordingProducer()
    signatures = {}
    event = {
        "event_id": "tomtom-TTI-1",
        "timestamp": "2026-05-12T06:00:00Z",
        "delay_magnitude": 3,
        "icon_category": 6,
        "geometry_wkt": "LINESTRING (-74.001 40.73, -73.995 40.735)",
    }

    assert (
        publish_event_if_changed(producer, "traffic.tomtom.raw", event, signatures)
        == "new"
    )
    assert (
        publish_event_if_changed(producer, "traffic.tomtom.raw", event, signatures)
        == "unchanged"
    )

    assert len(producer.messages) == 1
    assert producer.messages[0]["key"] == "tomtom-TTI-1"


def test_tomtom_same_event_id_with_changed_delay_is_published_as_update():
    producer = RecordingProducer()
    signatures = {}
    event = {
        "event_id": "tomtom-TTI-1",
        "timestamp": "2026-05-12T06:00:00Z",
        "delay_magnitude": 3,
        "icon_category": 6,
        "geometry_wkt": "LINESTRING (-74.001 40.73, -73.995 40.735)",
    }
    changed = {**event, "delay_magnitude": 4}

    assert (
        publish_event_if_changed(producer, "traffic.tomtom.raw", event, signatures)
        == "new"
    )
    assert (
        publish_event_if_changed(producer, "traffic.tomtom.raw", changed, signatures)
        == "update"
    )

    assert len(producer.messages) == 2


def test_tomtom_same_event_id_with_changed_geometry_is_published_as_update():
    producer = RecordingProducer()
    signatures = {}
    event = {
        "event_id": "tomtom-TTI-1",
        "timestamp": "2026-05-12T06:00:00Z",
        "delay_magnitude": 3,
        "icon_category": 6,
        "geometry_wkt": "LINESTRING (-74.001 40.73, -73.995 40.735)",
    }
    changed = {
        **event,
        "geometry_wkt": "LINESTRING (-74.002 40.731, -73.996 40.736)",
    }

    assert (
        publish_event_if_changed(producer, "traffic.tomtom.raw", event, signatures)
        == "new"
    )
    assert (
        publish_event_if_changed(producer, "traffic.tomtom.raw", changed, signatures)
        == "update"
    )

    assert len(producer.messages) == 2


def test_tomtom_different_event_ids_are_published_independently():
    producer = RecordingProducer()
    signatures = {}
    event = {
        "event_id": "tomtom-TTI-1",
        "timestamp": "2026-05-12T06:00:00Z",
        "delay_magnitude": 3,
        "icon_category": 6,
        "geometry_wkt": "LINESTRING (-74.001 40.73, -73.995 40.735)",
    }
    other = {**event, "event_id": "tomtom-TTI-2"}

    assert (
        publish_event_if_changed(producer, "traffic.tomtom.raw", event, signatures)
        == "new"
    )
    assert (
        publish_event_if_changed(producer, "traffic.tomtom.raw", other, signatures)
        == "new"
    )

    assert len(producer.messages) == 2
    assert {message["key"] for message in producer.messages} == {
        "tomtom-TTI-1",
        "tomtom-TTI-2",
    }
