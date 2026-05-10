"""Overview endpoints."""

from fastapi import APIRouter

from app.services.prediction_service import overview_summary

router = APIRouter()


@router.get("/summary")
def get_overview_summary() -> dict:
    """Return core dashboard counters and the latest model version."""
    return overview_summary()
