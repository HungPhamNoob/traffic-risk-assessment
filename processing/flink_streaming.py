#!/usr/bin/env python3
"""
Flink Streaming Job - US Traffic Risk Prediction

Architecture:
    1. Read raw accident events from Kafka (traffic.us.raw).
    2. Parse JSON + feature engineering (shared module).
    3. Save feature-enriched records to GCS silver bucket.
    4. Call MLflow model serving to predict risk severity.
    5. Insert prediction result into PostgreSQL/PostGIS.
    6. DLQ records go to a local log file (or simply logged).

No extra Kafka topics are used - all communication beyond Kafka
goes through GCS and PostgreSQL.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import psycopg2
from dotenv import load_dotenv
from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
import requests

# Feature engineering shared with offline training and batch jobs.
from processing.feature_engineering import build_features
from processing.streaming_enrichment import enrich_tomtom_event

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
logger = logging.getLogger("flink-inference")

# ============================================================
# Config
# ============================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
KAFKA_TOPIC_RAW = os.getenv(
    "KAFKA_TOPIC_RAW",
    "traffic.us.raw",
)
FLINK_INFERENCE_GROUP = os.getenv(
    "FLINK_INFERENCE_GROUP",
    "flink-us-inference",
)
FLINK_CHECKPOINT_INTERVAL = int(os.getenv("FLINK_CHECKPOINT_INTERVAL", "30000"))
FLINK_CHECKPOINT_DIR = os.getenv(
    "FLINK_LOCAL_CHECKPOINT_DIR",
    os.getenv(
        "FLINK_CHECKPOINT_DIR", "file:///tmp/flink-checkpoints/us-accident-inference"
    ),
)
FLINK_KAFKA_CONNECTOR_JAR = os.getenv("FLINK_KAFKA_CONNECTOR_JAR", "")

# MLflow
MLFLOW_SERVING_ENDPOINT = os.getenv(
    "MLFLOW_SERVING_ENDPOINT",
    "http://10.128.0.4:5001/invocations",
)
ML_TIMEOUT_SECONDS = float(os.getenv("ML_TIMEOUT_SECONDS", "5"))
ML_FALLBACK_RISK_SCORE = float(os.getenv("ML_FALLBACK_RISK_SCORE", "-1"))

# GCS silver output
SILVER_FEATURES_PATH = os.getenv(
    "SILVER_FEATURES_PATH",
    "gs://big-data-group-4-silver/features/flink/",
)

# PostgreSQL / PostGIS
PG_HOST = os.getenv("POSTGRES_HOST", "10.128.0.4")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "capstone_db")
PG_USER = os.getenv("POSTGRES_USER", "capstone")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "123")
PG_TABLE = os.getenv("POSTGRES_PREDICTION_TABLE", "traffic_risk_predictions")

# ============================================================
# Model feature order (must match training)
# ============================================================
MODEL_FEATURE_COLUMNS = [
    "lat",
    "lon",
    "hour",
    "day_of_week",
    "is_weekend",
    "is_rush_hour",
    "weather_code",
    "temperature_f",
    "humidity",
    "wind_speed_mph",
    "visibility_mi",
    "road_type_code",
    "is_junction",
    "has_traffic_signal",
    "is_crossing",
    "is_roundabout",
    "is_stop",
    "is_station",
    "is_railway",
    "is_night",
]


# ============================================================
# Helper: GCS writer (silver layer)
# ============================================================
def write_to_gcs_silver(features: Dict[str, Any]) -> None:
    """
    Write one feature-engineered event to the GCS Silver layer.

    GCS object writes are atomic but not append-friendly. One JSON document per
    event keeps the sink idempotent, avoids concurrent append corruption, and
    still lets Spark read the whole partition tree recursively.
    """
    try:
        import gcsfs

        event_time = features.get("event_time")
        if event_time:
            dt = datetime.fromisoformat(str(event_time).replace("Z", "+00:00"))
            prefix = f"{dt.year}/{dt.month:02d}/{dt.day:02d}"
        else:
            prefix = "unknown_date"

        safe_event_id = str(features.get("event_id", "unknown_event")).replace("/", "_")
        path = (
            f"{SILVER_FEATURES_PATH.rstrip('/')}/{prefix}/events/{safe_event_id}.json"
        )

        fs = gcsfs.GCSFileSystem()
        payload = json.dumps(features, ensure_ascii=False) + "\n"
        with fs.open(path, "wb") as f:
            f.write(payload.encode("utf-8"))
        logger.debug("Written features to GCS silver: %s", path)
    except Exception as e:
        logger.error("Failed to write features to GCS silver: %s", e)


# ============================================================
# Helper: MLflow client
# ============================================================
def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _severity_to_risk_score(severity: Any) -> Optional[float]:
    severity_float = _to_float(severity)
    if severity_float is None:
        return None
    return max(0.0, min(1.0, (severity_float - 1.0) / 3.0))


def _class_probabilities_to_risk_score(prediction: Dict[str, Any]) -> Optional[float]:
    probs = []
    for severity in range(1, 5):
        probability = _to_float(prediction.get(f"p{severity}"))
        if probability is not None:
            probs.append((severity, probability))
    if not probs:
        return None

    total_probability = sum(probability for _, probability in probs)
    if total_probability <= 0:
        return None

    expected_severity = (
        sum(severity * probability for severity, probability in probs)
        / total_probability
    )
    return _severity_to_risk_score(expected_severity)


def _extract_prediction(prediction: Any) -> Tuple[Optional[int], Optional[float]]:
    """
    Normalize common MLflow serving response shapes into the database contract.

    H2O and MLflow wrappers can return a scalar class, a dictionary with
    `predict`, or class probability columns such as `p1` to `p4`. The streaming
    sink stores a nullable integer severity plus a 0-1 risk score so the
    dashboard can rank events even when probability output is unavailable.
    """
    if not isinstance(prediction, dict):
        severity = prediction
        return _to_int(severity), _severity_to_risk_score(severity)

    severity = (
        prediction.get("predict")
        or prediction.get("prediction")
        or prediction.get("predicted_severity")
    )
    risk = prediction.get("risk_score") or prediction.get("probability")

    if isinstance(risk, (list, tuple)):
        risk_score = max((_to_float(value) or 0.0) for value in risk) if risk else None
    else:
        risk_score = _to_float(risk)

    if risk_score is None:
        risk_score = _class_probabilities_to_risk_score(prediction)
    if risk_score is None:
        risk_score = _severity_to_risk_score(severity)

    return _to_int(severity), risk_score


def call_mlflow_model(
    features: Dict[str, Any]
) -> Tuple[Optional[int], Optional[float]]:
    """Call MLflow serving and return normalized `(predicted_severity, risk_score)`."""
    row = []
    for col in MODEL_FEATURE_COLUMNS:
        if col not in features:
            raise ValueError(f"Missing feature column: {col}")
        row.append(features[col])

    payload = {
        "dataframe_split": {
            "columns": MODEL_FEATURE_COLUMNS,
            "data": [row],
        }
    }

    try:
        resp = requests.post(
            MLFLOW_SERVING_ENDPOINT,
            json=payload,
            timeout=ML_TIMEOUT_SECONDS,
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        result = resp.json()
        preds = result.get("predictions", [])
        if preds:
            return _extract_prediction(preds[0])
        else:
            return None, None
    except Exception:
        logger.exception("MLflow call failed")
        return None, None


# ============================================================
# Helper: PostgreSQL insert
# ============================================================
CREATE_TABLE_SQL = f"""
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE TABLE IF NOT EXISTS {PG_TABLE} (
    event_id VARCHAR PRIMARY KEY,
    event_year INT,
    event_time TIMESTAMP,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    true_severity INT,
    predicted_severity INT,
    risk_score DOUBLE PRECISION,
    weather_code INT,
    temperature_f DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    wind_speed_mph DOUBLE PRECISION,
    visibility_mi DOUBLE PRECISION,
    road_type_code INT,
    hour INT,
    day_of_week INT,
    is_weekend INT,
    is_rush_hour INT,
    is_junction INT,
    has_traffic_signal INT,
    is_crossing INT,
    is_roundabout INT,
    is_stop INT,
    is_station INT,
    is_railway INT,
    is_night INT,
    model_status VARCHAR(20),
    inference_latency_ms DOUBLE PRECISION,
    ingestion_time TIMESTAMP,
    processed_time TIMESTAMP,
    end_to_end_latency_ms DOUBLE PRECISION,
    geom GEOMETRY(Point, 4326),
    created_at TIMESTAMP DEFAULT NOW()
);
"""

SCHEMA_EVOLUTION_SQL = [
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS event_year INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS event_time TIMESTAMP;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS lat DOUBLE PRECISION;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS lon DOUBLE PRECISION;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS true_severity INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS predicted_severity INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS risk_score DOUBLE PRECISION;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS weather_code INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS temperature_f DOUBLE PRECISION;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS humidity DOUBLE PRECISION;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS wind_speed_mph DOUBLE PRECISION;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS visibility_mi DOUBLE PRECISION;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS road_type_code INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS hour INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS day_of_week INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS is_weekend INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS is_rush_hour INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS is_junction INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS has_traffic_signal INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS is_crossing INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS is_roundabout INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS is_stop INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS is_station INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS is_railway INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS is_night INT;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS model_status VARCHAR(20);",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS inference_latency_ms DOUBLE PRECISION;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS ingestion_time TIMESTAMP;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS processed_time TIMESTAMP;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS end_to_end_latency_ms DOUBLE PRECISION;",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS geom GEOMETRY(Point, 4326);",
    f"ALTER TABLE {PG_TABLE} ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();",
]
POSTGIS_SCHEMA_READY = False

INSERT_SQL = f"""
INSERT INTO {PG_TABLE} (
    event_id, event_year, event_time, lat, lon,
    true_severity, predicted_severity, risk_score,
    weather_code, temperature_f, humidity, wind_speed_mph, visibility_mi,
    road_type_code, hour, day_of_week, is_weekend, is_rush_hour,
    is_junction, has_traffic_signal, is_crossing, is_roundabout,
    is_stop, is_station, is_railway, is_night,
    model_status, inference_latency_ms, ingestion_time, processed_time,
    end_to_end_latency_ms, geom
) VALUES (
    %(event_id)s, %(event_year)s, %(event_time)s, %(lat)s, %(lon)s,
    %(true_severity)s, %(predicted_severity)s, %(risk_score)s,
    %(weather_code)s, %(temperature_f)s, %(humidity)s,
    %(wind_speed_mph)s, %(visibility_mi)s,
    %(road_type_code)s, %(hour)s, %(day_of_week)s,
    %(is_weekend)s, %(is_rush_hour)s,
    %(is_junction)s, %(has_traffic_signal)s, %(is_crossing)s,
    %(is_roundabout)s, %(is_stop)s, %(is_station)s, %(is_railway)s,
    %(is_night)s, %(model_status)s, %(inference_latency_ms)s,
    %(ingestion_time)s, %(processed_time)s, %(end_to_end_latency_ms)s,
    ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)
)
ON CONFLICT (event_id) DO UPDATE SET
    event_year = EXCLUDED.event_year,
    event_time = EXCLUDED.event_time,
    lat = EXCLUDED.lat,
    lon = EXCLUDED.lon,
    true_severity = EXCLUDED.true_severity,
    predicted_severity = EXCLUDED.predicted_severity,
    risk_score = EXCLUDED.risk_score,
    weather_code = EXCLUDED.weather_code,
    temperature_f = EXCLUDED.temperature_f,
    humidity = EXCLUDED.humidity,
    wind_speed_mph = EXCLUDED.wind_speed_mph,
    visibility_mi = EXCLUDED.visibility_mi,
    road_type_code = EXCLUDED.road_type_code,
    hour = EXCLUDED.hour,
    day_of_week = EXCLUDED.day_of_week,
    is_weekend = EXCLUDED.is_weekend,
    is_rush_hour = EXCLUDED.is_rush_hour,
    is_junction = EXCLUDED.is_junction,
    has_traffic_signal = EXCLUDED.has_traffic_signal,
    is_crossing = EXCLUDED.is_crossing,
    is_roundabout = EXCLUDED.is_roundabout,
    is_stop = EXCLUDED.is_stop,
    is_station = EXCLUDED.is_station,
    is_railway = EXCLUDED.is_railway,
    is_night = EXCLUDED.is_night,
    model_status = EXCLUDED.model_status,
    inference_latency_ms = EXCLUDED.inference_latency_ms,
    ingestion_time = EXCLUDED.ingestion_time,
    processed_time = EXCLUDED.processed_time,
    end_to_end_latency_ms = EXCLUDED.end_to_end_latency_ms,
    geom = EXCLUDED.geom,
    created_at = NOW();
"""


def insert_prediction_to_postgis(
    features: Dict[str, Any],
    severity: Optional[int],
    risk_score: Optional[float],
    latency_ms: float,
    ingestion_time: Optional[str],
    processed_time: str,
    end_to_end_latency_ms: Optional[float],
) -> None:
    """Insert a prediction record into PostGIS."""
    global POSTGIS_SCHEMA_READY

    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
        )
        with conn:
            with conn.cursor() as cur:
                if not POSTGIS_SCHEMA_READY:
                    # Existing demo tables can be older than the streaming job.
                    # Evolve them in place so deployments do not require manual
                    # database resets before replaying fresh traffic events.
                    cur.execute(CREATE_TABLE_SQL)
                    for schema_statement in SCHEMA_EVOLUTION_SQL:
                        cur.execute(schema_statement)
                    POSTGIS_SCHEMA_READY = True

                data = {
                    "event_id": features["event_id"],
                    "event_year": features["event_year"],
                    "event_time": features.get(
                        "event_time", datetime.now(timezone.utc).isoformat()
                    ),
                    "lat": features["lat"],
                    "lon": features["lon"],
                    "true_severity": features.get("true_severity"),
                    "predicted_severity": severity,
                    "risk_score": risk_score,
                    "weather_code": features.get("weather_code"),
                    "temperature_f": features.get("temperature_f"),
                    "humidity": features.get("humidity"),
                    "wind_speed_mph": features.get("wind_speed_mph"),
                    "visibility_mi": features.get("visibility_mi"),
                    "road_type_code": features.get("road_type_code"),
                    "hour": features.get("hour"),
                    "day_of_week": features.get("day_of_week"),
                    "is_weekend": features.get("is_weekend"),
                    "is_rush_hour": features.get("is_rush_hour"),
                    "is_junction": features.get("is_junction"),
                    "has_traffic_signal": features.get("has_traffic_signal"),
                    "is_crossing": features.get("is_crossing"),
                    "is_roundabout": features.get("is_roundabout"),
                    "is_stop": features.get("is_stop"),
                    "is_station": features.get("is_station"),
                    "is_railway": features.get("is_railway"),
                    "is_night": features.get("is_night"),
                    "model_status": "ok" if severity is not None else "failed",
                    "inference_latency_ms": latency_ms,
                    "ingestion_time": ingestion_time,
                    "processed_time": processed_time,
                    "end_to_end_latency_ms": end_to_end_latency_ms,
                }
                cur.execute(INSERT_SQL, data)
        conn.close()
        logger.debug("Inserted prediction for event %s", features["event_id"])
    except Exception:
        logger.exception("Failed to insert prediction into PostGIS")
        raise


# ============================================================
# Main processing function (pure Python, called inside Flink)
# ============================================================
def process_raw_message(raw_message: str) -> str:
    """
    Process a single raw JSON message from Kafka.
    Returns a status string (logged by Flink).
    """
    start = time.time()
    try:
        # 1. Parse
        raw_row = json.loads(raw_message)
        ingestion_time = raw_row.get("_ingested_at_utc")

        # 2. TomTom incidents need projection into the shared feature contract.
        if str(raw_row.get("source", "")).strip().lower() == "tomtom":
            feature_input = enrich_tomtom_event(raw_row)
            if feature_input is None:
                raise ValueError("enrich_tomtom_event returned None (missing fields)")
        else:
            feature_input = raw_row

        # 3. Feature engineering
        features = build_features(feature_input)
        if features is None:
            raise ValueError("build_features returned None (missing fields)")

        # 4. Write silver layer to GCS
        write_to_gcs_silver(features)

        # 5. ML prediction
        predicted_severity, risk_score = call_mlflow_model(features)
        if risk_score is None or risk_score < 0:
            risk_score = ML_FALLBACK_RISK_SCORE

        # 6. Insert into PostgreSQL
        processed_time = datetime.now(timezone.utc).isoformat()
        latency = (time.time() - start) * 1000
        end_to_end_latency_ms = None
        if ingestion_time:
            try:
                ingestion_dt = datetime.fromisoformat(
                    str(ingestion_time).replace("Z", "+00:00")
                )
                processed_dt = datetime.fromisoformat(
                    processed_time.replace("Z", "+00:00")
                )
                end_to_end_latency_ms = (
                    processed_dt - ingestion_dt
                ).total_seconds() * 1000
            except ValueError:
                end_to_end_latency_ms = None

        insert_prediction_to_postgis(
            features,
            predicted_severity,
            risk_score,
            latency,
            ingestion_time,
            processed_time,
            end_to_end_latency_ms,
        )

        return f"OK: {features.get('event_id')}"
    except Exception as e:
        logger.error("Processing failed for message '%s': %s", raw_message[:100], e)
        return f"FAIL: {e}"


# ============================================================
# Flink job definition
# ============================================================
def main():
    logger.info("=" * 80)
    logger.info("Starting Flink inference job (GCS + PostGIS sink)")
    logger.info("Kafka input: %s [%s]", KAFKA_TOPIC_RAW, FLINK_INFERENCE_GROUP)
    logger.info("Silver layer: %s", SILVER_FEATURES_PATH)
    logger.info("MLflow: %s", MLFLOW_SERVING_ENDPOINT)
    logger.info("PostGIS: %s:%s/%s", PG_HOST, PG_PORT, PG_DB)
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

    # Kafka source
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(KAFKA_TOPIC_RAW)
        .set_group_id(FLINK_INFERENCE_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="kafka-raw-source",
    )

    # Process each message (side effects: GCS + PostgreSQL)
    processed_stream = raw_stream.map(process_raw_message, output_type=Types.STRING())

    # Print status to keep the job alive (or use a no-op sink)
    processed_stream.print()

    env.execute("Flink Traffic Risk Prediction - GCS + PostGIS")


if __name__ == "__main__":
    main()
