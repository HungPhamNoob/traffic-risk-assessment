"""Model metadata endpoints."""

from fastapi import APIRouter

from app.core.config import get_settings

router = APIRouter()


@router.get("/info")
def get_model_info() -> dict:
    """Return model registry and serving configuration used by the backend."""
    settings = get_settings()
    return {
        "model_name": settings.model_name,
        "model_version": settings.model_version or "latest",
        "tracking_uri": settings.mlflow_tracking_uri,
        "serving_endpoint": settings.mlflow_serving_endpoint,
    }
