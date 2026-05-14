"""
PostGIS sink for streaming risk predictions.

Builds and upserts rows for public.traffic_risk_predictions.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)

TABLE_NAME = "traffic_risk_predictions"

PREDICTION_COLUMNS = [
    "event_id",
    "latitude",
    "longitude",
    "grid_cell_id",
    "risk_score",
    "risk_level",
    "severity",
    "speed",
    "weather_condition",
    "road_type",
    "event_timestamp",
    "prediction_timestamp",
    "source",
    "lat",
    "lng",
    "lon",
    "predicted_severity",
    "true_severity",
    "event_time",
    "model_status",
    "hour",
    "weather_code",
    "event_year",
    "temperature_f",
    "humidity",
    "wind_speed_mph",
    "visibility_mi",
    "road_type_code",
    "day_of_week",
    "is_weekend",
    "is_rush_hour",
    "is_junction",
    "has_traffic_signal",
    "is_crossing",
    "is_roundabout",
    "is_stop",
    "is_station",
    "is_railway",
    "is_night",
    "inference_latency_ms",
    "ingestion_time",
    "processed_time",
    "end_to_end_latency_ms",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _int_flag(value: Any) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return 1 if value else 0
    return 1 if str(value).strip().lower() in {"1", "true", "yes", "y"} else 0


def _risk_level_label(value: Any) -> str | None:
    if value is None:
        return None
    labels = {
        0: "unknown",
        1: "very_low",
        2: "low",
        3: "medium",
        4: "high",
        5: "very_high",
    }
    try:
        return labels.get(int(value), str(value))
    except (TypeError, ValueError):
        return str(value)


def build_prediction_row(event: Dict[str, Any]) -> Dict[str, Any]:
    """Build a deterministic DB row from an enriched prediction event."""
    event_timestamp = event.get("event_timestamp") or event.get("timestamp")
    prediction_timestamp = event.get("prediction_timestamp") or event.get("scored_at") or _now_iso()
    lat = event.get("lat", event.get("latitude"))
    lon = event.get("lon", event.get("longitude"))
    severity = event.get("severity") or event.get("true_severity")
    row = {
        "event_id": event.get("event_id"),
        "latitude": event.get("latitude", lat),
        "longitude": event.get("longitude", lon),
        "grid_cell_id": event.get("grid_cell_id"),
        "risk_score": event.get("risk_score"),
        "risk_level": _risk_level_label(event.get("risk_level")),
        "severity": severity,
        "speed": event.get("speed", 0.0),
        "weather_condition": event.get("weather_condition", "unknown"),
        "road_type": event.get("road_type", "unknown"),
        "event_timestamp": event_timestamp,
        "prediction_timestamp": prediction_timestamp,
        "source": event.get("source", "tomtom"),
        "lat": lat,
        "lng": event.get("lng", lon),
        "lon": lon,
        "predicted_severity": event.get("predicted_severity", 0),
        "true_severity": event.get("true_severity", severity),
        "event_time": event.get("event_time", event_timestamp),
        "model_status": event.get("model_status") or event.get("inference_status"),
        "hour": event.get("hour", event.get("hour_of_day")),
        "weather_code": str(event.get("weather_code", "0")),
        "event_year": event.get("event_year"),
        "temperature_f": event.get("temperature_f", 0.0),
        "humidity": event.get("humidity", 0.0),
        "wind_speed_mph": event.get("wind_speed_mph", 0.0),
        "visibility_mi": event.get("visibility_mi", 0.0),
        "road_type_code": event.get("road_type_code", 0),
        "day_of_week": event.get("day_of_week"),
        "is_weekend": _int_flag(event.get("is_weekend")),
        "is_rush_hour": _int_flag(event.get("is_rush_hour")),
        "is_junction": _int_flag(event.get("is_junction")),
        "has_traffic_signal": _int_flag(event.get("has_traffic_signal")),
        "is_crossing": _int_flag(event.get("is_crossing")),
        "is_roundabout": _int_flag(event.get("is_roundabout")),
        "is_stop": _int_flag(event.get("is_stop")),
        "is_station": _int_flag(event.get("is_station")),
        "is_railway": _int_flag(event.get("is_railway")),
        "is_night": _int_flag(event.get("is_night")),
        "inference_latency_ms": event.get("inference_latency_ms", 0.0),
        "ingestion_time": event.get("ingestion_time"),
        "processed_time": event.get("processed_time") or event.get("processed_at"),
        "end_to_end_latency_ms": event.get("end_to_end_latency_ms", 0.0),
    }
    return {column: row.get(column) for column in PREDICTION_COLUMNS}


def _postgres_dsn() -> str:
    return (
        os.getenv("POSTGIS_CONNECTION_STRING")
        or os.getenv("DATABASE_URL")
        or (
            f"postgresql://{os.getenv('POSTGRES_USER', 'postgres')}:"
            f"{os.getenv('POSTGRES_PASSWORD', 'changeme')}@"
            f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
            f"{os.getenv('POSTGRES_PORT', '5432')}/"
            f"{os.getenv('POSTGRES_DB', 'accident_risk')}"
        )
    )


def ensure_prediction_table(conn) -> None:
    """Create the prediction table and indexes if they do not exist."""
    ddl = f"""
    CREATE EXTENSION IF NOT EXISTS postgis;
    CREATE TABLE IF NOT EXISTS public.{TABLE_NAME} (
        event_id VARCHAR PRIMARY KEY,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        geom geometry(Point,4326),
        grid_cell_id VARCHAR,
        risk_score DOUBLE PRECISION,
        risk_level VARCHAR,
        severity INTEGER,
        speed DOUBLE PRECISION,
        weather_condition VARCHAR,
        road_type VARCHAR,
        event_timestamp TIMESTAMP,
        prediction_timestamp TIMESTAMP DEFAULT now(),
        source VARCHAR DEFAULT 'tomtom',
        lat DOUBLE PRECISION,
        lng DOUBLE PRECISION,
        lon DOUBLE PRECISION,
        predicted_severity INTEGER,
        true_severity INTEGER,
        event_time TIMESTAMP,
        model_status VARCHAR,
        hour INTEGER,
        weather_code VARCHAR,
        event_year INTEGER,
        temperature_f DOUBLE PRECISION,
        humidity DOUBLE PRECISION,
        wind_speed_mph DOUBLE PRECISION,
        visibility_mi DOUBLE PRECISION,
        road_type_code INTEGER,
        day_of_week INTEGER,
        is_weekend INTEGER,
        is_rush_hour INTEGER,
        is_junction INTEGER,
        has_traffic_signal INTEGER,
        is_crossing INTEGER,
        is_roundabout INTEGER,
        is_stop INTEGER,
        is_station INTEGER,
        is_railway INTEGER,
        is_night INTEGER,
        inference_latency_ms DOUBLE PRECISION,
        ingestion_time TIMESTAMP,
        processed_time TIMESTAMP,
        end_to_end_latency_ms DOUBLE PRECISION,
        created_at TIMESTAMP DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_traffic_risk_predictions_geom
        ON public.{TABLE_NAME} USING gist (geom);
    CREATE INDEX IF NOT EXISTS idx_traffic_risk_predictions_risk_score
        ON public.{TABLE_NAME} (risk_score);
    CREATE INDEX IF NOT EXISTS idx_traffic_risk_predictions_time
        ON public.{TABLE_NAME} (event_timestamp);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def upsert_prediction(conn, event: Dict[str, Any]) -> None:
    """Upsert one prediction event into PostGIS."""
    row = build_prediction_row(event)
    columns_sql = ", ".join(PREDICTION_COLUMNS)
    placeholders = ", ".join(["%s"] * len(PREDICTION_COLUMNS))
    updates = ", ".join(
        f"{column}=EXCLUDED.{column}" for column in PREDICTION_COLUMNS if column != "event_id"
    )
    sql = f"""
    INSERT INTO public.{TABLE_NAME} (
        {columns_sql}, geom
    ) VALUES (
        {placeholders},
        ST_SetSRID(ST_MakePoint(%s, %s), 4326)
    )
    ON CONFLICT (event_id) DO UPDATE SET
        {updates},
        geom=EXCLUDED.geom;
    """
    values = [row[column] for column in PREDICTION_COLUMNS]
    values.extend([row.get("lon"), row.get("lat")])
    with conn.cursor() as cur:
        cur.execute(sql, values)
    conn.commit()


def write_prediction(event: Dict[str, Any]) -> bool:
    """Open a connection and upsert one prediction event."""
    try:
        import psycopg2
        with psycopg2.connect(_postgres_dsn()) as conn:
            ensure_prediction_table(conn)
            upsert_prediction(conn, event)
        return True
    except Exception as exc:
        logger.error("Failed to write prediction to PostGIS: %s", exc)
        return False


def sink_stream() -> None:
    """Placeholder entrypoint kept for compatibility."""
    logger.info("Use write_prediction(event) from the streaming inference job.")
