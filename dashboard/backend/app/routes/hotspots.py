"""Hotspot endpoints."""

from fastapi import APIRouter, Query

from app.services.hotspot_service import hotspot_detail, nearby_events, top_hotspots

router = APIRouter()


@router.get("")
def get_hotspots(
    limit: int = Query(default=20, ge=1, le=500),
    min_events: int = Query(default=5, ge=1),
    start_time: str | None = None,
    end_time: str | None = None,
) -> dict:
    """Return ranked high-risk locations."""
    return top_hotspots(limit, min_events, start_time, end_time)


@router.get("/nearby")
def get_nearby_events(
    lat: float,
    lon: float,
    radius_m: float = Query(default=5000, gt=0),
    limit: int = Query(default=100, ge=1, le=1000),
) -> dict:
    """Return risky events near a latitude/longitude point."""
    return nearby_events(lat, lon, radius_m, limit)


@router.get("/{hotspot_id}")
def get_hotspot_detail(hotspot_id: int) -> dict:
    """Return detail for a one-based hotspot rank."""
    return hotspot_detail(hotspot_id)
