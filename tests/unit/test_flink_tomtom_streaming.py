"""Unit tests for the TomTom-only Flink processing path."""

import json

from processing import flink_tomtom_streaming as tomtom_job


def test_build_tomtom_incident_record_maps_rule_score():
    raw_row = {
        "source": "tomtom",
        "event_id": "tomtom-TTI-1",
        "incident_id": "TTI-1",
        "latitude": 40.73,
        "longitude": -74.0,
        "timestamp": "2026-05-12T06:00:00Z",
        "delay_magnitude": 3,
        "icon_category": 6,
        "delay_seconds": 180,
        "length_meters": 850.0,
        "incident_code": 101,
        "incident_description": "Slow traffic",
        "from_road": "I-95 N",
        "to_road": "Exit 1",
        "road_numbers": ["I-95"],
        "time_validity": "present",
        "probability_of_occurrence": "certain",
        "number_of_reports": 2,
        "last_report_time": "2026-05-12T06:05:00Z",
        "_ingested_at_utc": "2026-05-12T06:06:00Z",
        "raw_payload": {"properties": {"id": "TTI-1"}},
    }

    record = tomtom_job.build_tomtom_incident_record(
        raw_row,
        processed_time="2026-05-12T06:06:02Z",
        processing_latency_ms=12.5,
    )

    assert record is not None
    assert record["event_id"] == "tomtom-TTI-1"
    assert record["incident_id"] == "TTI-1"
    assert record["event_time"] == "2026-05-12T06:00:00"
    assert record["lat"] == 40.73
    assert record["lon"] == -74.0
    assert record["severity"] == 3
    assert record["tomtom_rule_score"] == 0.666667
    assert record["icon_category"] == 6
    assert record["delay_magnitude"] == 3
    assert record["road_numbers"] == ["I-95"]
    assert record["raw_payload"] == {"properties": {"id": "TTI-1"}}


def test_tomtom_processor_writes_tomtom_table_without_mlflow(monkeypatch):
    inserted = []
    monkeypatch.setattr(tomtom_job, "insert_tomtom_incident", inserted.append)

    raw_row = {
        "event_id": "tomtom-TTI-1",
        "incident_id": "TTI-1",
        "latitude": 40.73,
        "longitude": -74.0,
        "timestamp": "2026-05-12T06:00:00Z",
        "delay_magnitude": 4,
        "icon_category": 8,
    }

    result = tomtom_job.process_tomtom_message(json.dumps(raw_row))

    assert result == "OK: tomtom-TTI-1"
    assert len(inserted) == 1
    assert inserted[0]["severity"] == 4
    assert inserted[0]["tomtom_rule_score"] == 1.0
    assert not hasattr(tomtom_job, "call_mlflow_model")


def test_tomtom_processor_rejects_invalid_event_without_insert(monkeypatch):
    inserted = []
    monkeypatch.setattr(tomtom_job, "insert_tomtom_incident", inserted.append)

    result = tomtom_job.process_tomtom_message(json.dumps({"event_id": "bad"}))

    assert result.startswith("FAIL:")
    assert inserted == []


def test_tomtom_schema_targets_separate_table_and_indexes():
    assert "traffic_tomtom_incidents" in tomtom_job.CREATE_TABLE_SQL
    assert "tomtom_rule_score DOUBLE PRECISION" in tomtom_job.CREATE_TABLE_SQL
    assert any("USING GIST (geom)" in statement for statement in tomtom_job.INDEX_SQL)
    assert any("(event_time)" in statement for statement in tomtom_job.INDEX_SQL)
    assert any("(severity)" in statement for statement in tomtom_job.INDEX_SQL)
    assert any("(tomtom_rule_score)" in statement for statement in tomtom_job.INDEX_SQL)
