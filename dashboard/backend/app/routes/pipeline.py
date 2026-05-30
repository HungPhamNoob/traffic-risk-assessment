"""Operational pipeline endpoints for dashboard health views."""

from fastapi import APIRouter, Query

from app.services.pipeline_service import (
    checkpoints,
    latency,
    replay_health,
    throughput,
)

router = APIRouter()


@router.get("/throughput")
def get_throughput(window: str = Query(default="5m")) -> dict:
    """Return recent prediction throughput for a lookback window such as 5m or 1h."""
    return throughput(window)


@router.get("/latency")
def get_latency(metric: str = Query(default="p95")) -> dict:
    """Return latency percentiles from the prediction table."""
    return latency(metric)


@router.get("/checkpoints")
def get_checkpoints() -> dict:
    """Return best-effort checkpoint and Gold dataset freshness metadata."""
    return checkpoints()


@router.get("/replay-health")
def get_replay_health() -> dict:
    """Return replay health and model-status metadata from prediction rows."""
    return replay_health()
