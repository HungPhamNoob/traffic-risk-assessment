#!/usr/bin/env python3
"""
Flink Streaming Job – US Traffic Risk Prediction

Architecture:
    1. Read raw accident events from Kafka (traffic.us.raw).
    2. Parse JSON + feature engineering (shared module).
    3. Save feature-enriched records to GCS silver bucket.
    4. Call MLflow model serving to predict risk severity.
    5. Insert prediction result into PostgreSQL/PostGIS.
    6. DLQ records go to a local log file (or simply logged).

No extra Kafka topics are used – all communication beyond Kafka
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
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
import requests

# Feature engineering shared with Spark
from processing.feature_engineering import build_features

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
    "FLINK_CHECKPOINT_DIR",
    "file:///tmp/flink-checkpoints/us-accident-inference",
)

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
    Append the feature-engineered event to a JSONL file in GCS.
    Partitioned by event_year / event_month / event_day.
    """
    try:
        import gcsfs

        event_time = features.get("event_time")
        if event_time:
            dt = datetime.fromisoformat(str(event_time).replace("Z", "+00:00"))
            prefix = f"{dt.year}/{dt.month:02d}/{dt.day:02d}"
        else:
            prefix = "unknown_date"

        path = f"{SILVER_FEATURES_PATH.rstrip('/')}/{prefix}/events.jsonl"

        fs = gcsfs.GCSFileSystem()
        # Append to existing file or create new one
        line = json.dumps(features, ensure_ascii=False) + "\n"
        with fs.open(path, "ab") as f:
            f.write(line.encode("utf-8"))
        logger.debug("Written features to GCS silver: %s", path)
    except Exception as e:
        logger.error("Failed to write features to GCS silver: %s", e)


# ============================================================
# Helper: MLflow client
# ============================================================
def call_mlflow_model(
    features: Dict[str, Any]
) -> Tuple[Optional[int], Optional[float]]:
    """
    Call the MLflow model serving endpoint and normalize common response shapes.

    MLflow can return a scalar class, a list of probabilities, or a dictionary
    depending on the logged model wrapper. The streaming job stores both a
    severity class and a risk score so the dashboard can rank high-risk events.
    """
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
            first = preds[0]
            if isinstance(first, dict):
                severity = (
                    first.get("predict")
                    or first.get("prediction")
                    or first.get("predicted_severity")
                )
                risk = first.get("risk_score") or first.get("probability")
            else:
                severity = first
                risk = None
            if isinstance(risk, list) and risk:
                risk = max(float(value) for value in risk)
            if risk is None and severity is not None:
                risk = max(0.0, min(1.0, (float(severity) - 1.0) / 3.0))
            return int(float(severity)) if severity is not None else None, (
                float(risk) if risk is not None else None
            )
        else:
            return None, None
    except Exception:
        logger.exception("MLflow call failed")
        return None, None


# ============================================================
# Helper: PostgreSQL insert
# ============================================================
CREATE_TABLE_SQL = f"""
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
    geom GEOMETRY(Point, 4326),
    created_at TIMESTAMP DEFAULT NOW()
);
"""

INSERT_SQL = f"""
INSERT INTO {PG_TABLE} (
    event_id, event_year, event_time, lat, lon,
    true_severity, predicted_severity, risk_score,
    weather_code, temperature_f, humidity, wind_speed_mph, visibility_mi,
    road_type_code, hour, day_of_week, is_weekend, is_rush_hour,
    is_junction, has_traffic_signal, is_crossing, is_roundabout,
    is_stop, is_station, is_railway, is_night,
    model_status, inference_latency_ms, geom
) VALUES (
    %(event_id)s, %(event_year)s, %(event_time)s, %(lat)s, %(lon)s,
    %(true_severity)s, %(predicted_severity)s, %(risk_score)s,
    %(weather_code)s, %(temperature_f)s, %(humidity)s, %(wind_speed_mph)s, %(visibility_mi)s,
    %(road_type_code)s, %(hour)s, %(day_of_week)s, %(is_weekend)s, %(is_rush_hour)s,
    %(is_junction)s, %(has_traffic_signal)s, %(is_crossing)s, %(is_roundabout)s,
    %(is_stop)s, %(is_station)s, %(is_railway)s, %(is_night)s,
    %(model_status)s, %(inference_latency_ms)s,
    ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)
)
ON CONFLICT (event_id) DO UPDATE SET
    event_time = EXCLUDED.event_time,
    true_severity = EXCLUDED.true_severity,
    predicted_severity = EXCLUDED.predicted_severity,
    risk_score = EXCLUDED.risk_score,
    model_status = EXCLUDED.model_status,
    inference_latency_ms = EXCLUDED.inference_latency_ms,
    created_at = NOW();
"""


def insert_prediction_to_postgis(
    features: Dict[str, Any],
    severity: Optional[int],
    risk_score: Optional[float],
    latency_ms: float,
) -> None:
    """Insert a prediction record into PostGIS."""
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
                # ensure table exists
                cur.execute(CREATE_TABLE_SQL)
                # prepare data
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
                }
                cur.execute(INSERT_SQL, data)
        conn.close()
        logger.debug("Inserted prediction for event %s", features["event_id"])
    except Exception as e:
        logger.error("Failed to insert into PostGIS: %s", e)


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
        # 2. Feature engineering. The shared builder defines the single
        # feature contract used by offline training, streaming inference, and
        # Spark retraining.
        features = build_features(raw_row)
        if features is None:
            raise ValueError("build_features returned None (missing fields)")

        # 3. Write silver layer to GCS
        write_to_gcs_silver(features)

        # 4. ML prediction
        predicted_severity, risk_score = call_mlflow_model(features)
        if risk_score is None or risk_score < 0:
            risk_score = ML_FALLBACK_RISK_SCORE

        # 5. Insert into PostgreSQL
        latency = (time.time() - start) * 1000
        insert_prediction_to_postgis(features, predicted_severity, risk_score, latency)

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
    checkpoint_config.set_checkpoint_storage(FLINK_CHECKPOINT_DIR)

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
        watermark_strategy=None,
        source_name="kafka-raw-source",
    )

    # Process each message (side effects: GCS + PostgreSQL)
    processed_stream = raw_stream.map(process_raw_message, output_type=Types.STRING())

    # Print status to keep the job alive (or use a no-op sink)
    processed_stream.print()

    env.execute("Flink Traffic Risk Prediction – GCS + PostGIS")


if __name__ == "__main__":
    main()
