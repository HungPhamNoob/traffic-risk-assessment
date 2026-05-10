"""Historical analytics endpoints."""

from fastapi import APIRouter

from app.services.analytics_service import (
    risk_by_hour,
    risk_by_weather,
    severity_distribution,
    timeseries,
)

router = APIRouter()


@router.get("/timeseries")
def get_timeseries(
    group_by: str = "day",
    metric: str = "avg_risk",
    start_time: str | None = None,
    end_time: str | None = None,
) -> dict:
    """Return a time series for dashboard charts."""
    return timeseries(group_by, metric, start_time, end_time)


@router.get("/severity-distribution")
def get_severity_distribution() -> dict:
    """Return ground-truth severity class counts."""
    return severity_distribution()


@router.get("/risk-by-hour")
def get_risk_by_hour() -> dict:
    """Return average risk score by hour."""
    return risk_by_hour()


@router.get("/risk-by-weather")
def get_risk_by_weather() -> dict:
    """Return average risk score by weather code."""
    return risk_by_weather()
