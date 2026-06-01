"""Compatibility aliases that map design-document endpoints to implemented services."""

from fastapi import APIRouter, Query

from app.schemas.scenario import ScenarioCompareRequest
from app.routes.scenarios import compare_scenarios
from app.services.hotspot_service import top_hotspots
from app.services.prediction_service import map_points
from app.routes.system import get_system_status

router = APIRouter()


@router.get("/risk/hotspots")
def risk_hotspots_alias(
    limit: int = Query(default=20, ge=1, le=500),
    min_events: int = Query(default=5, ge=1),
    start_time: str | None = None,
    end_time: str | None = None,
    mode: str | None = Query(default="full"),
) -> dict:
    """Return the same ranked hotspots through the design-document risk path."""
    return top_hotspots(limit, min_events, start_time, end_time, mode)


@router.get("/accidents")
def accidents_alias(
    bbox: str | None = None,
    min_risk: float = Query(default=0.0, ge=0.0, le=1.0),
    start_time: str | None = None,
    end_time: str | None = None,
    limit: int = Query(default=5000, ge=1, le=20000),
) -> dict:
    """Return prediction map points through the design-document accident path."""
    return map_points(bbox, min_risk, start_time, end_time, limit)


@router.post("/whatif/simulate")
def whatif_simulate_alias(request: ScenarioCompareRequest) -> dict:
    """Run scenario comparison through the design-document what-if path."""
    return compare_scenarios(request)


@router.get("/system/health")
def system_health_alias() -> dict:
    """Return system status through the design-document health path."""
    return get_system_status()
