#!/usr/bin/env python3
"""
Flink Streaming Job - US Traffic Risk Prediction

Data flows:

    Kafka traffic.us.raw
    -> feature engineering (processing.feature_engineering.build_features)
    -> Silver GCS write
    -> MLflow / H2O inference
    -> PostgreSQL table: traffic_risk_predictions

TomTom incidents are handled by `processing/flink_tomtom_streaming.py`.
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import psycopg2
import requests
from dotenv import load_dotenv
from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from processing.feature_engineering import build_features

try:
    from pyflink.datastream.checkpoint_storage import FileSystemCheckpointStorage
except Exception:  # pragma: no cover - depends on the PyFlink distribution.
    FileSystemCheckpointStorage = None


# Load environment variables from .env file when running outside Docker.
load_dotenv()

logging.basicConfig(
    level=os.getenv("STREAMING_LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("flink-us-stream")


# ---------------------------------------------------------------------------
# Configuration (resolved from environment variables set by Docker Compose)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_US_RAW = os.getenv(
    "KAFKA_TOPIC_US_RAW",
    os.getenv("KAFKA_TOPIC_RAW", "traffic.us.raw"),
)
FLINK_INFERENCE_GROUP = os.getenv("FLINK_INFERENCE_GROUP", "flink-us-inference")
FLINK_PARALLELISM = int(os.getenv("FLINK_PARALLELISM", "1"))
FLINK_CHECKPOINT_INTERVAL = int(os.getenv("FLINK_CHECKPOINT_INTERVAL", "30000"))
FLINK_CHECKPOINT_DIR = os.getenv(
    "FLINK_LOCAL_CHECKPOINT_DIR",
    os.getenv("FLINK_CHECKPOINT_DIR", "file:///tmp/flink-checkpoints/traffic-risk"),
)
FLINK_KAFKA_CONNECTOR_JAR = os.getenv("FLINK_KAFKA_CONNECTOR_JAR", "")

MLFLOW_SERVING_ENDPOINT = os.getenv(
    "MLFLOW_SERVING_ENDPOINT",
    "http://10.128.0.4:5001/invocations",
)
ML_TIMEOUT_SECONDS = float(os.getenv("ML_TIMEOUT_SECONDS", "5"))
ML_FALLBACK_RISK_SCORE = float(os.getenv("ML_FALLBACK_RISK_SCORE", "-1"))

SILVER_FEATURES_PATH = os.getenv(
    "SILVER_FEATURES_PATH",
    "gs://big-data-group-4-silver/process/flink_features",
)

PG_HOST = os.getenv("POSTGRES_HOST", "10.128.0.4")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "capstone_db")
PG_USER = os.getenv("POSTGRES_USER", "capstone")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "123")
PG_US_TABLE = os.getenv(
    "POSTGRES_US_PREDICTION_TABLE",
    os.getenv("POSTGRES_PREDICTION_TABLE", "traffic_risk_predictions"),
)
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

SCHEMA_READY = {"us": False}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def table_name(value: str) -> str:
    """Return a simple public-schema table name."""
    selected = value.split(".")[-1]
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", selected):
        raise ValueError(f"Invalid PostgreSQL table name: {value}")
    return selected


PG_US_TABLE = table_name(PG_US_TABLE)


def pg_connect():
    """Create a PostgreSQL connection for one sink write."""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def write_to_gcs_silver(features: Dict[str, Any]) -> None:
    """Write one US replay feature record to the Silver layer."""
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
        payload = json.dumps(features, ensure_ascii=False) + "\n"

        fs = gcsfs.GCSFileSystem()
        with fs.open(path, "wb") as file_obj:
            file_obj.write(payload.encode("utf-8"))
        logger.debug("Wrote US features to Silver: %s", path)
    except Exception:
        logger.exception("Failed to write US features to Silver")


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
    probabilities = []
    for severity in range(1, 5):
        probability = _to_float(prediction.get(f"p{severity}"))
        if probability is not None:
            probabilities.append((severity, probability))
    if not probabilities:
        return None

    total_probability = sum(probability for _, probability in probabilities)
    if total_probability <= 0:
        return None

    expected_severity = (
        sum(severity * probability for severity, probability in probabilities)
        / total_probability
    )
    return _severity_to_risk_score(expected_severity)


def _extract_prediction(prediction: Any) -> Tuple[Optional[int], Optional[float]]:
    """Normalize common MLflow response shapes into severity and risk score."""
    if not isinstance(prediction, dict):
        return _to_int(prediction), _severity_to_risk_score(prediction)

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
    """Call MLflow Serving for US replay events."""
    row = []
    for column in MODEL_FEATURE_COLUMNS:
        if column not in features:
            raise ValueError(f"Missing feature column: {column}")
        row.append(features[column])

    payload = {"dataframe_split": {"columns": MODEL_FEATURE_COLUMNS, "data": [row]}}

    try:
        response = requests.post(
            MLFLOW_SERVING_ENDPOINT,
            json=payload,
            timeout=ML_TIMEOUT_SECONDS,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        predictions = response.json().get("predictions", [])
        if not predictions:
            return None, None
        return _extract_prediction(predictions[0])
    except Exception:
        logger.exception("MLflow inference failed for US replay event")
        return None, None


def ensure_us_schema(cursor) -> None:
    """Create or evolve the US replay prediction table."""
    cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {PG_US_TABLE} (
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
            model_status VARCHAR(32),
            inference_latency_ms DOUBLE PRECISION,
            ingestion_time TIMESTAMP,
            processed_time TIMESTAMP,
            end_to_end_latency_ms DOUBLE PRECISION,
            geom GEOMETRY(Point, 4326),
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
    )
    for statement in [
        "event_year INT",
        "event_time TIMESTAMP",
        "lat DOUBLE PRECISION",
        "lon DOUBLE PRECISION",
        "true_severity INT",
        "predicted_severity INT",
        "risk_score DOUBLE PRECISION",
        "weather_code INT",
        "temperature_f DOUBLE PRECISION",
        "humidity DOUBLE PRECISION",
        "wind_speed_mph DOUBLE PRECISION",
        "visibility_mi DOUBLE PRECISION",
        "road_type_code INT",
        "hour INT",
        "day_of_week INT",
        "is_weekend INT",
        "is_rush_hour INT",
        "is_junction INT",
        "has_traffic_signal INT",
        "is_crossing INT",
        "is_roundabout INT",
        "is_stop INT",
        "is_station INT",
        "is_railway INT",
        "is_night INT",
        "model_status VARCHAR(32)",
        "inference_latency_ms DOUBLE PRECISION",
        "ingestion_time TIMESTAMP",
        "processed_time TIMESTAMP",
        "end_to_end_latency_ms DOUBLE PRECISION",
        "geom GEOMETRY(Point, 4326)",
        "created_at TIMESTAMP DEFAULT NOW()",
    ]:
        cursor.execute(
            f"ALTER TABLE {PG_US_TABLE} ADD COLUMN IF NOT EXISTS {statement};"
        )


def parse_latency_ms(ingestion_time: Any, processed_time: str) -> Optional[float]:
    """Compute end-to-end latency from producer ingestion time to sink write time."""
    if not ingestion_time:
        return None
    try:
        ingestion_dt = datetime.fromisoformat(
            str(ingestion_time).replace("Z", "+00:00")
        )
        processed_dt = datetime.fromisoformat(processed_time.replace("Z", "+00:00"))
        return (processed_dt - ingestion_dt).total_seconds() * 1000
    except ValueError:
        return None


def insert_us_prediction(
    features: Dict[str, Any],
    severity: Optional[int],
    risk_score: Optional[float],
    inference_latency_ms: float,
    ingestion_time: Optional[str],
    processed_time: str,
    end_to_end_latency_ms: Optional[float],
) -> None:
    """Insert one US replay prediction into PostgreSQL/PostGIS."""
    connection = pg_connect()
    try:
        with connection:
            with connection.cursor() as cursor:
                if not SCHEMA_READY["us"]:
                    ensure_us_schema(cursor)
                    SCHEMA_READY["us"] = True

                cursor.execute(
                    f"""
                INSERT INTO {PG_US_TABLE} (
                    event_id, event_year, event_time, lat, lon,
                    true_severity, predicted_severity, risk_score,
                    weather_code, temperature_f, humidity, wind_speed_mph,
                    visibility_mi, road_type_code, hour, day_of_week,
                    is_weekend, is_rush_hour, is_junction, has_traffic_signal,
                    is_crossing, is_roundabout, is_stop, is_station, is_railway,
                    is_night, model_status, inference_latency_ms, ingestion_time,
                    processed_time, end_to_end_latency_ms, geom
                ) VALUES (
                    %(event_id)s, %(event_year)s, %(event_time)s, %(lat)s, %(lon)s,
                    %(true_severity)s, %(predicted_severity)s, %(risk_score)s,
                    %(weather_code)s, %(temperature_f)s, %(humidity)s,
                    %(wind_speed_mph)s, %(visibility_mi)s, %(road_type_code)s,
                    %(hour)s, %(day_of_week)s, %(is_weekend)s, %(is_rush_hour)s,
                    %(is_junction)s, %(has_traffic_signal)s, %(is_crossing)s,
                    %(is_roundabout)s, %(is_stop)s, %(is_station)s, %(is_railway)s,
                    %(is_night)s, %(model_status)s, %(inference_latency_ms)s,
                    %(ingestion_time)s, %(processed_time)s,
                    %(end_to_end_latency_ms)s,
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
                    """,
                    {
                        **features,
                        "predicted_severity": severity,
                        "risk_score": risk_score,
                        "model_status": "ok" if severity is not None else "failed",
                        "inference_latency_ms": inference_latency_ms,
                        "ingestion_time": ingestion_time,
                        "processed_time": processed_time,
                        "end_to_end_latency_ms": end_to_end_latency_ms,
                    },
                )
    finally:
        connection.close()

def process_us_message(raw_message: str) -> str:
    """Process one US replay message from Kafka."""
    start = time.time()
    try:
        raw_row = json.loads(raw_message)
        ingestion_time = raw_row.get("_ingested_at_utc")

        if str(raw_row.get("source", "")).strip().lower() == "tomtom":
            raise ValueError("TomTom events must be handled by flink_tomtom_streaming")

        features = build_features(raw_row)
        if features is None:
            raise ValueError("US feature engineering returned no record")

        write_to_gcs_silver(features)
        predicted_severity, risk_score = call_mlflow_model(features)
        if risk_score is None or risk_score < 0:
            risk_score = ML_FALLBACK_RISK_SCORE

        processed_time = datetime.now(timezone.utc).isoformat()
        insert_us_prediction(
            features=features,
            severity=predicted_severity,
            risk_score=risk_score,
            inference_latency_ms=(time.time() - start) * 1000,
            ingestion_time=ingestion_time,
            processed_time=processed_time,
            end_to_end_latency_ms=parse_latency_ms(ingestion_time, processed_time),
        )
        return f"US_OK: {features.get('event_id')}"
    except Exception as exc:
        logger.exception("US message processing failed: %s", str(raw_message)[:200])
        return f"US_FAIL: {exc}"


def build_kafka_source(topic: str, group_id: str, source_name: str):
    """Create a Kafka source for a single raw topic."""
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    return source, source_name


def main() -> None:
    """Build and start the US replay Flink job."""
    logger.info("=" * 80)
    logger.info("Starting Flink US replay inference job")
    logger.info("Kafka bootstrap: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("US topic: %s -> PostgreSQL table: %s", KAFKA_TOPIC_US_RAW, PG_US_TABLE)
    logger.info("US MLflow endpoint: %s", MLFLOW_SERVING_ENDPOINT)
    logger.info("US Silver path: %s", SILVER_FEATURES_PATH)
    logger.info("=" * 80)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(FLINK_PARALLELISM)
    env.enable_checkpointing(FLINK_CHECKPOINT_INTERVAL)

    checkpoint_config = env.get_checkpoint_config()
    if FileSystemCheckpointStorage is not None:
        checkpoint_config.set_checkpoint_storage(
            FileSystemCheckpointStorage(FLINK_CHECKPOINT_DIR)
        )
    else:
        logger.warning("Checkpoint storage configuration is using PyFlink defaults.")

    connector_jars = [
        jar.strip() for jar in FLINK_KAFKA_CONNECTOR_JAR.split(",") if jar.strip()
    ]
    if connector_jars:
        env.add_jars(*connector_jars)

    us_source, us_source_name = build_kafka_source(
        KAFKA_TOPIC_US_RAW,
        FLINK_INFERENCE_GROUP,
        "kafka-us-raw-source",
    )
    us_stream = env.from_source(
        source=us_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name=us_source_name,
    ).map(process_us_message, output_type=Types.STRING())

    us_stream.print()
    env.execute("Flink Traffic Risk Prediction - GCS + PostGIS")


if __name__ == "__main__":
    main()
