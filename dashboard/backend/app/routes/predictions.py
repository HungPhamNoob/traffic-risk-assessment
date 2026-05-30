"""Prediction and map endpoints."""

from fastapi import APIRouter, Query

from app.services.prediction_service import (
    latest_predictions,
    map_points,
    prediction_detail,
)

router = APIRouter()


@router.get("/map")
def get_prediction_map(
    bbox: str | None = None,
    min_risk: float = Query(default=0.0, ge=0.0, le=1.0),
    start_time: str | None = None,
    end_time: str | None = None,
    limit: int = Query(default=5000, ge=1, le=20000),
    mode: str = Query(default="full", pattern="^(replay|live|full)$"),
) -> dict:
    """Return prediction points for the live risk map."""
    return map_points(bbox, min_risk, start_time, end_time, limit, mode)


@router.get("/latest")
def get_latest_predictions(
    limit: int = Query(default=100, ge=1, le=1000),
    mode: str = Query(default="full", pattern="^(replay|live|full)$"),
) -> dict:
    """Return the most recent prediction rows."""
    return latest_predictions(limit, mode)


@router.get("/{event_id}")
def get_prediction_detail(event_id: str) -> dict:
    """Return full prediction details for a single event."""
    return prediction_detail(event_id)
