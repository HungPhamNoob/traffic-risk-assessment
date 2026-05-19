"""Expose derived Prometheus metrics for pipeline freshness and model state."""

from __future__ import annotations

import logging
import os
from pathlib import Path
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import psycopg2
import requests
from google.cloud import storage
from prometheus_client import Gauge, start_http_server


logger = logging.getLogger("pipeline_exporter")
logging.basicConfig(
    level=os.getenv("PIPELINE_EXPORTER_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


POLL_INTERVAL_SECONDS = int(os.getenv("PIPELINE_EXPORTER_POLL_INTERVAL_SECONDS", "60"))
EXPORTER_PORT = int(os.getenv("PIPELINE_EXPORTER_PORT", "9200"))
POSTGRES_CONNECT_TIMEOUT = int(os.getenv("PIPELINE_EXPORTER_DB_TIMEOUT_SECONDS", "5"))
HTTP_TIMEOUT_SECONDS = int(os.getenv("PIPELINE_EXPORTER_HTTP_TIMEOUT_SECONDS", "5"))
PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/workspace")

POSTGRES_TABLE = os.getenv("POSTGRES_PREDICTION_TABLE", "traffic_risk_predictions")
ML_MODEL_NAME = os.getenv("ML_MODEL_NAME", "traffic-risk-model")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000").rstrip("/")
SILVER_FEATURES_PATH = os.getenv(
    "SILVER_FEATURES_PATH", "gs://big-data-group-4-silver/process/flink_features"
)
GOLD_RETRAIN_PATH = os.getenv(
    "GOLD_RETRAIN_PATH", "gs://big-data-group-4-gold/features/retrain"
)
GOLD_RETRAIN_PARQUET_PATH = os.getenv(
    "GOLD_RETRAIN_PARQUET_PATH",
    "gs://big-data-group-4-gold/features/retrain/parquet",
)
GOLD_RETRAIN_CSV_PATH = os.getenv(
    "GOLD_RETRAIN_CSV_PATH", "gs://big-data-group-4-gold/features/retrain/csv"
)


SOURCE_UP = Gauge(
    "traffic_pipeline_source_up",
    "Whether a pipeline data source is reachable and returned fresh metadata.",
    ["source"],
)
PREDICTION_ROWS = Gauge(
    "traffic_pipeline_prediction_rows",
    "Total rows currently stored in the prediction sink table.",
)
PREDICTION_LATEST_TIMESTAMP = Gauge(
    "traffic_pipeline_prediction_latest_event_timestamp_seconds",
    "Unix timestamp of the newest prediction event_time stored in PostgreSQL.",
)
PREDICTION_FRESHNESS = Gauge(
    "traffic_pipeline_prediction_freshness_seconds",
    "Seconds since the newest prediction event_time stored in PostgreSQL.",
)
SILVER_LATEST_TIMESTAMP = Gauge(
    "traffic_pipeline_silver_latest_object_timestamp_seconds",
    "Unix timestamp of the newest Silver object in GCS.",
)
SILVER_FRESHNESS = Gauge(
    "traffic_pipeline_silver_freshness_seconds",
    "Seconds since the newest Silver object in GCS.",
)
GOLD_LATEST_TIMESTAMP = Gauge(
    "traffic_pipeline_gold_latest_object_timestamp_seconds",
    "Unix timestamp of the newest Gold retrain object in GCS.",
)
GOLD_FRESHNESS = Gauge(
    "traffic_pipeline_gold_freshness_seconds",
    "Seconds since the newest Gold retrain object in GCS.",
)
MODEL_INFO = Gauge(
    "traffic_pipeline_model_info",
    "Latest registered MLflow model version for the pipeline.",
    ["model_name", "version"],
)
MODEL_LATEST_TIMESTAMP = Gauge(
    "traffic_pipeline_model_latest_version_timestamp_seconds",
    "Unix timestamp of the latest registered MLflow model version.",
)
MODEL_FRESHNESS = Gauge(
    "traffic_pipeline_model_freshness_seconds",
    "Seconds since the latest registered MLflow model version was created.",
)
EXPORTER_LAST_SUCCESS = Gauge(
    "traffic_pipeline_exporter_last_success_timestamp_seconds",
    "Unix timestamp when the pipeline exporter last completed a full refresh.",
)


@dataclass
class GcsLocation:
    bucket: str
    prefix: str


def _now_ts() -> float:
    return time.time()


def _freshness_from_timestamp(timestamp_seconds: float) -> float:
    if timestamp_seconds <= 0:
        return 0.0
    return max(0.0, _now_ts() - timestamp_seconds)


def _set_source_down(source: str) -> None:
    SOURCE_UP.labels(source=source).set(0)


def _parse_gcs_uri(uri: str) -> GcsLocation:
    parsed = urlparse(uri)
    if parsed.scheme != "gs" or not parsed.netloc:
        raise ValueError(f"Unsupported GCS URI: {uri}")

    prefix = parsed.path.lstrip("/")
    return GcsLocation(bucket=parsed.netloc, prefix=prefix.rstrip("/"))


def _coerce_to_timestamp_seconds(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return 0.0
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


class PipelineExporter:
    """Refresh derived pipeline metrics without changing pipeline behavior."""

    def __init__(self) -> None:
        self.storage_client: storage.Client | None = None
        self._stop_event = threading.Event()

    def run_forever(self) -> None:
        while not self._stop_event.is_set():
            started_at = _now_ts()
            try:
                self.refresh()
                EXPORTER_LAST_SUCCESS.set(started_at)
            except Exception:
                logger.exception("Pipeline exporter refresh failed.")
            self._stop_event.wait(POLL_INTERVAL_SECONDS)

    def refresh(self) -> None:
        self._refresh_postgres()
        self._refresh_path_source(
            source="silver",
            primary_path=SILVER_FEATURES_PATH,
            fallback_path=None,
            timestamp_metric=SILVER_LATEST_TIMESTAMP,
            freshness_metric=SILVER_FRESHNESS,
        )
        self._refresh_path_source(
            source="gold",
            primary_path=GOLD_RETRAIN_CSV_PATH,
            fallback_path=GOLD_RETRAIN_PARQUET_PATH or GOLD_RETRAIN_PATH,
            timestamp_metric=GOLD_LATEST_TIMESTAMP,
            freshness_metric=GOLD_FRESHNESS,
        )
        self._refresh_mlflow()

    def _refresh_postgres(self) -> None:
        query = f"""
            SELECT
              COUNT(*)::BIGINT AS prediction_rows,
              EXTRACT(EPOCH FROM MAX(event_time)) AS latest_event_time_seconds
            FROM {POSTGRES_TABLE}
        """
        connection = None
        try:
            connection = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                dbname=os.getenv("POSTGRES_DB", "capstone_db"),
                user=os.getenv("POSTGRES_USER", "capstone"),
                password=os.getenv("POSTGRES_PASSWORD", "123"),
                connect_timeout=POSTGRES_CONNECT_TIMEOUT,
            )
            with connection.cursor() as cursor:
                cursor.execute(query)
                prediction_rows, latest_event_time_seconds = cursor.fetchone()

            latest_ts = float(latest_event_time_seconds or 0.0)
            PREDICTION_ROWS.set(float(prediction_rows or 0))
            PREDICTION_LATEST_TIMESTAMP.set(latest_ts)
            PREDICTION_FRESHNESS.set(_freshness_from_timestamp(latest_ts))
            SOURCE_UP.labels(source="postgres").set(1)
        except Exception:
            logger.exception("Failed to refresh PostgreSQL pipeline metrics.")
            _set_source_down("postgres")
            PREDICTION_ROWS.set(0)
            PREDICTION_LATEST_TIMESTAMP.set(0)
            PREDICTION_FRESHNESS.set(0)
        finally:
            if connection is not None:
                connection.close()

    def _refresh_path_source(
        self,
        source: str,
        primary_path: str,
        fallback_path: Optional[str],
        timestamp_metric: Gauge,
        freshness_metric: Gauge,
    ) -> None:
        latest_ts = 0.0
        try:
            latest_ts = self._latest_path_timestamp_seconds(primary_path)
            if latest_ts <= 0 and fallback_path:
                latest_ts = self._latest_path_timestamp_seconds(fallback_path)

            if latest_ts > 0:
                SOURCE_UP.labels(source=source).set(1)
            else:
                _set_source_down(source)

            timestamp_metric.set(latest_ts)
            freshness_metric.set(_freshness_from_timestamp(latest_ts))
        except Exception:
            logger.exception("Failed to refresh %s path metrics.", source)
            _set_source_down(source)
            timestamp_metric.set(0)
            freshness_metric.set(0)

    def _latest_path_timestamp_seconds(self, path_value: str) -> float:
        if path_value.startswith("gs://"):
            return self._latest_gcs_object_timestamp_seconds(path_value)
        return self._latest_local_path_timestamp_seconds(path_value)

    def _latest_local_path_timestamp_seconds(self, path_value: str) -> float:
        local_path = path_value.replace("file://", "", 1)
        path = Path(local_path)
        if not path.is_absolute():
            path = Path(PROJECT_ROOT) / path
        if not path.exists():
            return 0.0

        candidates = [path]
        if path.is_dir():
            candidates = [item for item in path.rglob("*") if item.is_file()]
        latest_mtime = max((item.stat().st_mtime for item in candidates), default=0.0)
        return float(latest_mtime or 0.0)

    def _storage_client(self) -> storage.Client:
        if self.storage_client is None:
            self.storage_client = storage.Client()
        return self.storage_client

    def _latest_gcs_object_timestamp_seconds(self, gcs_uri: str) -> float:
        location = _parse_gcs_uri(gcs_uri)
        latest_updated: Optional[datetime] = None

        for blob in self._storage_client().list_blobs(
            location.bucket, prefix=location.prefix
        ):
            if blob.name.endswith("/"):
                continue
            if latest_updated is None or blob.updated > latest_updated:
                latest_updated = blob.updated

        if latest_updated is None:
            return 0.0
        return _coerce_to_timestamp_seconds(latest_updated)

    def _refresh_mlflow(self) -> None:
        try:
            response = requests.get(
                f"{MLFLOW_TRACKING_URI}/api/2.0/mlflow/registered-models/get",
                params={"name": ML_MODEL_NAME},
                timeout=HTTP_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            payload = response.json().get("registered_model", {})
            latest_versions = payload.get("latest_versions") or []

            latest_version = None
            for version_info in latest_versions:
                if latest_version is None:
                    latest_version = version_info
                    continue
                current_version = int(version_info.get("version", "0"))
                previous_version = int(latest_version.get("version", "0"))
                if current_version >= previous_version:
                    latest_version = version_info

            MODEL_INFO.clear()
            if latest_version is None:
                MODEL_INFO.labels(model_name=ML_MODEL_NAME, version="unknown").set(0)
                MODEL_LATEST_TIMESTAMP.set(0)
                MODEL_FRESHNESS.set(0)
                _set_source_down("mlflow")
                return

            version = str(latest_version.get("version", "unknown"))
            created_at_ms = float(latest_version.get("creation_timestamp") or 0.0)
            created_at_seconds = created_at_ms / 1000.0 if created_at_ms else 0.0

            MODEL_INFO.labels(model_name=ML_MODEL_NAME, version=version).set(1)
            MODEL_LATEST_TIMESTAMP.set(created_at_seconds)
            MODEL_FRESHNESS.set(_freshness_from_timestamp(created_at_seconds))
            SOURCE_UP.labels(source="mlflow").set(1)
        except Exception:
            logger.exception("Failed to refresh MLflow model metrics.")
            MODEL_INFO.clear()
            MODEL_INFO.labels(model_name=ML_MODEL_NAME, version="unknown").set(0)
            MODEL_LATEST_TIMESTAMP.set(0)
            MODEL_FRESHNESS.set(0)
            _set_source_down("mlflow")


def main() -> None:
    exporter = PipelineExporter()
    exporter.refresh()
    start_http_server(EXPORTER_PORT)
    logger.info(
        "Pipeline exporter listening on port %s with refresh interval %ss.",
        EXPORTER_PORT,
        POLL_INTERVAL_SECONDS,
    )
    exporter.run_forever()


if __name__ == "__main__":
    main()
