"""
Smoke test for the streaming PostGIS prediction sink.

Run this on a node that can reach POSTGRES_HOST/DATABASE_URL:

    python3 processing/flink/smoke_postgis_sink.py
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from processing.flink.sink_to_postgis import TABLE_NAME, _postgres_dsn, write_prediction


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def build_smoke_event() -> dict:
    now = _now_iso()
    return {
        "event_id": f"smoke-postgis-{now}",
        "source": "tomtom",
        "latitude": 40.7411403065,
        "longitude": -74.2590071853,
        "lat": 40.7411403065,
        "lng": -74.2590071853,
        "lon": -74.2590071853,
        "grid_cell_id": "grid_smoke_postgis",
        "event_timestamp": now,
        "event_time": now,
        "prediction_timestamp": now,
        "processed_time": now,
        "ingestion_time": now,
        "severity": 3,
        "true_severity": 3,
        "predicted_severity": 4,
        "risk_score": 0.75,
        "risk_level": 4,
        "model_status": "SUCCESS",
        "speed": 0.0,
        "weather_condition": "overcast",
        "weather_code": 5,
        "temperature_f": 57.0,
        "humidity": 83.0,
        "wind_speed_mph": 4.4,
        "visibility_mi": 10.0,
        "road_type": "unknown",
        "road_type_code": 0,
        "hour": 12,
        "event_year": 2026,
        "day_of_week": 5,
        "is_weekend": 0,
        "is_rush_hour": 0,
        "is_junction": 0,
        "has_traffic_signal": 0,
        "is_crossing": 0,
        "is_roundabout": 0,
        "is_stop": 0,
        "is_station": 0,
        "is_railway": 0,
        "is_night": 0,
        "inference_latency_ms": 10.0,
        "end_to_end_latency_ms": 1000.0,
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    event = build_smoke_event()
    dsn = _postgres_dsn()
    redacted_dsn = dsn.replace("changeme123", "***")
    logging.info("Writing smoke event to %s via %s", TABLE_NAME, redacted_dsn)
    if not write_prediction(event):
        raise SystemExit("PostGIS smoke write failed")
    logging.info("PostGIS smoke write OK: event_id=%s", event["event_id"])


if __name__ == "__main__":
    main()
