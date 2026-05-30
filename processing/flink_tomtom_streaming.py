#!/usr/bin/env python3
"""
Flink Streaming Job - TomTom Live Incident Ingestion

Architecture:
    1. Read normalized TomTom incident events from Kafka (traffic.tomtom.raw).
    2. Parse JSON and compute rule-based severity from TomTom delay/category.
    3. Insert the live incident record into a dedicated PostgreSQL/PostGIS table.

This job intentionally does not call MLflow, does not write Silver data for
Spark, and does not participate in H2O retraining.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import psycopg2
from dotenv import load_dotenv

from processing.feature_engineering import _safe_float, _safe_int, _safe_string
from processing.streaming_enrichment import normalize_tomtom_severity

try:
    from pyflink.datastream.checkpoint_storage import FileSystemCheckpointStorage
except Exception:  # pragma: no cover - depends on the PyFlink distribution.
    FileSystemCheckpointStorage = None

# ============================================================
# Environment
# ============================================================
load_dotenv()

logging.basicConfig(
    level=os.getenv("STREAMING_LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("flink-tomtom-live")

# ============================================================
# Config
# ============================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
KAFKA_TOPIC_TOMTOM_RAW = os.getenv(
    "KAFKA_TOPIC_TOMTOM_RAW",
    os.getenv("KAFKA_TOPIC_RAW", "traffic.tomtom.raw"),
)
FLINK_TOMTOM_GROUP = os.getenv(
    "FLINK_TOMTOM_GROUP",
    os.getenv("FLINK_INFERENCE_GROUP", "flink-tomtom-inference"),
)
FLINK_CHECKPOINT_INTERVAL = int(os.getenv("FLINK_CHECKPOINT_INTERVAL", "30000"))
FLINK_CHECKPOINT_DIR = os.getenv(
    "FLINK_LOCAL_CHECKPOINT_DIR",
    os.getenv("FLINK_TOMTOM_CHECKPOINT_DIR", "file:///tmp/flink-checkpoints/tomtom"),
)
FLINK_KAFKA_CONNECTOR_JAR = os.getenv("FLINK_KAFKA_CONNECTOR_JAR", "")

PG_HOST = os.getenv("POSTGRES_HOST", "10.128.0.4")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "capstone_db")
PG_USER = os.getenv("POSTGRES_USER", "capstone")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "123")
PG_TOMTOM_TABLE = os.getenv("POSTGRES_TOMTOM_TABLE", "traffic_tomtom_incidents")


def _quoted_table_name(table_name: str) -> str:
    if not table_name.replace("_", "").isalnum():
        raise ValueError(f"Unsafe table name: {table_name}")
    return table_name


PG_TABLE = _quoted_table_name(PG_TOMTOM_TABLE)

CREATE_TABLE_SQL = f"""
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE TABLE IF NOT EXISTS {PG_TABLE} (
    event_id VARCHAR PRIMARY KEY,
    incident_id VARCHAR,
    event_time TIMESTAMP,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    severity INT,
    tomtom_rule_score DOUBLE PRECISION,
    icon_category INT,
    delay_magnitude INT,
    delay_seconds DOUBLE PRECISION,
    length_meters DOUBLE PRECISION,
    incident_code INT,
    incident_description TEXT,
    from_road TEXT,
    to_road TEXT,
    road_numbers JSONB,
    time_validity VARCHAR(50),
    probability_of_occurrence VARCHAR(50),
    number_of_reports INT,
    last_report_time TIMESTAMP,
    ingestion_time TIMESTAMP,
    processed_time TIMESTAMP,
    processing_latency_ms DOUBLE PRECISION,
    geom GEOMETRY(Point, 4326),
    raw_payload JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);
"""

SCHEMA_EVOLUTION_SQL = [
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS incident_id VARCHAR;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS event_time TIMESTAMP;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS lat DOUBLE PRECISION;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS lon DOUBLE PRECISION;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS severity INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS tomtom_rule_score DOUBLE PRECISION;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS icon_category INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS delay_magnitude INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS delay_seconds DOUBLE PRECISION;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS length_meters DOUBLE PRECISION;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS incident_code INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS incident_description TEXT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS from_road TEXT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS to_road TEXT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS road_numbers JSONB;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS time_validity VARCHAR(50);",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS probability_of_occurrence VARCHAR(50);",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS number_of_reports INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS last_report_time TIMESTAMP;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS ingestion_time TIMESTAMP;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS processed_time TIMESTAMP;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS processing_latency_ms DOUBLE PRECISION;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS geom GEOMETRY(Point, 4326);",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS raw_payload JSONB;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();",
]

INDEX_SQL = [
    f"CREATE INDEX IF NOT EXISTS idx_{PG_TABLE}_geom ON {PG_TABLE} USING GIST (geom);",
    f"CREATE INDEX IF NOT EXISTS idx_{PG_TABLE}_event_time ON {PG_TABLE} (event_time);",
    f"CREATE INDEX IF NOT EXISTS idx_{PG_TABLE}_severity ON {PG_TABLE} (severity);",
    f"CREATE INDEX IF NOT EXISTS idx_{PG_TABLE}_rule_score ON {PG_TABLE} (tomtom_rule_score);",
]

INSERT_SQL = f"""
INSERT INTO {PG_TABLE} (
    event_id, incident_id, event_time, lat, lon,
    severity, tomtom_rule_score, icon_category, delay_magnitude,
    delay_seconds, length_meters, incident_code, incident_description,
    from_road, to_road, road_numbers, time_validity,
    probability_of_occurrence, number_of_reports, last_report_time,
    ingestion_time, processed_time, processing_latency_ms, geom, raw_payload
) VALUES (
    %(event_id)s, %(incident_id)s, %(event_time)s, %(lat)s, %(lon)s,
    %(severity)s, %(tomtom_rule_score)s, %(icon_category)s, %(delay_magnitude)s,
    %(delay_seconds)s, %(length_meters)s, %(incident_code)s, %(incident_description)s,
    %(from_road)s, %(to_road)s, %(road_numbers)s, %(time_validity)s,
    %(probability_of_occurrence)s, %(number_of_reports)s, %(last_report_time)s,
    %(ingestion_time)s, %(processed_time)s, %(processing_latency_ms)s,
    ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326), %(raw_payload)s
)
ON CONFLICT (event_id) DO UPDATE SET
    incident_id = EXCLUDED.incident_id,
    event_time = EXCLUDED.event_time,
    lat = EXCLUDED.lat,
    lon = EXCLUDED.lon,
    severity = EXCLUDED.severity,
    tomtom_rule_score = EXCLUDED.tomtom_rule_score,
    icon_category = EXCLUDED.icon_category,
    delay_magnitude = EXCLUDED.delay_magnitude,
    delay_seconds = EXCLUDED.delay_seconds,
    length_meters = EXCLUDED.length_meters,
    incident_code = EXCLUDED.incident_code,
    incident_description = EXCLUDED.incident_description,
    from_road = EXCLUDED.from_road,
    to_road = EXCLUDED.to_road,
    road_numbers = EXCLUDED.road_numbers,
    time_validity = EXCLUDED.time_validity,
    probability_of_occurrence = EXCLUDED.probability_of_occurrence,
    number_of_reports = EXCLUDED.number_of_reports,
    last_report_time = EXCLUDED.last_report_time,
    ingestion_time = EXCLUDED.ingestion_time,
    processed_time = EXCLUDED.processed_time,
    processing_latency_ms = EXCLUDED.processing_latency_ms,
    geom = EXCLUDED.geom,
    raw_payload = EXCLUDED.raw_payload,
    created_at = NOW();
"""

POSTGIS_SCHEMA_READY = False


def _parse_optional_timestamp(value: Any) -> Optional[str]:
    text = _safe_string(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed.isoformat()


def _first_non_empty(*values: Any) -> str:
    for value in values:
        text = _safe_string(value)
        if text:
            return text
    return ""


def _jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list, str, int, float, bool)):
        return value
    return str(value)


def build_tomtom_incident_record(
    raw_row: Dict[str, Any],
    processed_time: Optional[str] = None,
    processing_latency_ms: Optional[float] = None,
) -> Optional[Dict[str, Any]]:
    lat = _safe_float(raw_row.get("latitude", raw_row.get("lat")))
    lon = _safe_float(raw_row.get("longitude", raw_row.get("lon", raw_row.get("lng"))))
    event_id = _first_non_empty(raw_row.get("event_id"), raw_row.get("ID"))
    event_time = _parse_optional_timestamp(
        _first_non_empty(
            raw_row.get("timestamp"),
            raw_row.get("event_timestamp"),
            raw_row.get("last_report_time"),
        )
    )

    if lat is None or lon is None or not event_id or not event_time:
        return None
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return None

    icon_category = _safe_int(raw_row.get("icon_category"))
    delay_magnitude = _safe_int(raw_row.get("delay_magnitude"))
    severity = normalize_tomtom_severity(delay_magnitude, icon_category)

    processed_at = processed_time or datetime.now(timezone.utc).isoformat()
    return {
        "event_id": event_id,
        "incident_id": _first_non_empty(raw_row.get("incident_id"), event_id),
        "event_time": event_time,
        "lat": lat,
        "lon": lon,
        "severity": severity,
        "tomtom_rule_score": round((severity - 1) / 3, 6),
        "icon_category": icon_category,
        "delay_magnitude": delay_magnitude,
        "delay_seconds": _safe_float(raw_row.get("delay_seconds")),
        "length_meters": _safe_float(raw_row.get("length_meters")),
        "incident_code": _safe_int(raw_row.get("incident_code")),
        "incident_description": _safe_string(raw_row.get("incident_description")),
        "from_road": _safe_string(raw_row.get("from_road")),
        "to_road": _safe_string(raw_row.get("to_road")),
        "road_numbers": _jsonable(raw_row.get("road_numbers") or []),
        "time_validity": _safe_string(raw_row.get("time_validity")),
        "probability_of_occurrence": _safe_string(
            raw_row.get("probability_of_occurrence")
        ),
        "number_of_reports": _safe_int(raw_row.get("number_of_reports")),
        "last_report_time": _parse_optional_timestamp(raw_row.get("last_report_time")),
        "ingestion_time": _parse_optional_timestamp(
            _first_non_empty(raw_row.get("_ingested_at_utc"), raw_row.get("ingestion_time"))
        ),
        "processed_time": _parse_optional_timestamp(processed_at),
        "processing_latency_ms": processing_latency_ms,
        "raw_payload": _jsonable(raw_row.get("raw_payload") or raw_row),
    }


def _json_adapted_record(record: Dict[str, Any]) -> Dict[str, Any]:
    from psycopg2.extras import Json

    adapted = dict(record)
    adapted["road_numbers"] = Json(adapted.get("road_numbers") or [])
    adapted["raw_payload"] = Json(adapted.get("raw_payload") or {})
    return adapted


def insert_tomtom_incident(record: Dict[str, Any]) -> None:
    global POSTGIS_SCHEMA_READY

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    try:
        with conn:
            with conn.cursor() as cur:
                if not POSTGIS_SCHEMA_READY:
                    cur.execute(CREATE_TABLE_SQL)
                    for schema_statement in SCHEMA_EVOLUTION_SQL:
                        cur.execute(schema_statement)
                    for index_statement in INDEX_SQL:
                        cur.execute(index_statement)
                    POSTGIS_SCHEMA_READY = True
                cur.execute(INSERT_SQL, _json_adapted_record(record))
    finally:
        conn.close()


def process_tomtom_message(raw_message: str) -> str:
    start = time.time()
    try:
        raw_row = json.loads(raw_message)
        processed_time = datetime.now(timezone.utc).isoformat()
        latency_ms = (time.time() - start) * 1000
        record = build_tomtom_incident_record(raw_row, processed_time, latency_ms)
        if record is None:
            raise ValueError("Invalid TomTom event contract")
        insert_tomtom_incident(record)
        return f"OK: {record.get('event_id')}"
    except Exception as e:
        logger.error("TomTom processing failed for message '%s': %s", raw_message[:100], e)
        return f"FAIL: {e}"


def main() -> None:
    from pyflink.common import Types
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.common.watermark_strategy import WatermarkStrategy
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import (
        KafkaOffsetsInitializer,
        KafkaSource,
    )

    logger.info("=" * 80)
    logger.info("Starting Flink TomTom live ingestion job")
    logger.info("Kafka input: %s [%s]", KAFKA_TOPIC_TOMTOM_RAW, FLINK_TOMTOM_GROUP)
    logger.info("PostGIS table: %s:%s/%s.%s", PG_HOST, PG_PORT, PG_DB, PG_TABLE)
    logger.info("=" * 80)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(FLINK_CHECKPOINT_INTERVAL)
    checkpoint_config = env.get_checkpoint_config()
    if FileSystemCheckpointStorage is not None:
        checkpoint_config.set_checkpoint_storage(
            FileSystemCheckpointStorage(FLINK_CHECKPOINT_DIR)
        )
    else:
        logger.warning(
            "FileSystemCheckpointStorage is unavailable; using default checkpoint storage."
        )

    connector_jars = [
        jar.strip() for jar in FLINK_KAFKA_CONNECTOR_JAR.split(",") if jar.strip()
    ]
    if connector_jars:
        env.add_jars(*connector_jars)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(KAFKA_TOPIC_TOMTOM_RAW)
        .set_group_id(FLINK_TOMTOM_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="kafka-tomtom-raw-source",
    )

    processed_stream = raw_stream.map(
        process_tomtom_message,
        output_type=Types.STRING(),
    )
    processed_stream.print()

    env.execute("Flink TomTom Live Incidents - PostGIS")


if __name__ == "__main__":
    main()
