"""System and model status endpoints."""

from fastapi import APIRouter

from app.core.config import get_settings
from app.services.prediction_service import overview_summary

router = APIRouter()


@router.get("/status")
def get_system_status() -> dict:
    """Return pipeline configuration and lightweight status metadata."""
    settings = get_settings()
    summary = overview_summary()
    return {
        "kafka": {
            "topic": settings.kafka_topic_raw,
            "status": "configured",
        },
        "flink": {
            "job_name": "Flink Traffic Risk Prediction",
            "status": "configured",
            "checkpoint_dir": settings.flink_checkpoint_dir,
            "checkpoint_interval_ms": settings.flink_checkpoint_interval_ms,
        },
        "spark": {
            "last_gold_update": summary.get("latest_event_time"),
            "gold_path": settings.gold_retrain_path,
        },
        "mlflow": {
            "model_name": settings.model_name,
            "serving_endpoint": settings.mlflow_serving_endpoint,
            "latest_version": settings.model_version or "latest",
        },
        "postgres": {
            "prediction_table": settings.prediction_table,
            "row_count": summary.get("total_events", 0),
        },
    }


@router.get("/model/info")
def get_model_info() -> dict:
    """Return model configuration used by the backend."""
    settings = get_settings()
    return {
        "model_name": settings.model_name,
        "model_version": settings.model_version or "latest",
        "tracking_uri": settings.mlflow_tracking_uri,
        "serving_endpoint": settings.mlflow_serving_endpoint,
    }
