"""Historical analytics endpoints."""

from fastapi import APIRouter, Query

from app.services.analytics_service import (
    risk_by_hour,
    risk_by_weather,
    severity_distribution,
    timeseries,
    weather_histogram,
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
def get_severity_distribution(
    mode: str | None = Query(None, description="replay, live, or full"),
) -> dict:
    """Return ground-truth severity class counts."""
    return severity_distribution(mode)


@router.get("/risk-by-hour")
def get_risk_by_hour(
    mode: str | None = Query(None, description="replay, live, or full"),
) -> dict:
    """Return average risk score by hour."""
    return risk_by_hour(mode)


@router.get("/risk-by-weather")
def get_risk_by_weather() -> dict:
    """Return average risk score by weather code."""
    return risk_by_weather()


@router.get("/weather-histogram")
def get_weather_histogram(
    mode: str | None = Query(None, description="replay, live, or full"),
) -> dict:
    """Return temperature, humidity, and wind speed histograms."""
    return weather_histogram(mode)
