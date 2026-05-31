"""Operational pipeline endpoints for dashboard health views."""

from fastapi import APIRouter, Query

from app.services.pipeline_service import (
    checkpoints,
    latency,
    replay_health,
    reset_job_status,
    trigger_full_realtime_reset,
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


@router.post("/full-realtime-reset")
def post_full_realtime_reset(
    force: bool = Query(default=False),
) -> dict:
    """Launch the full realtime reset script in background and return tracking metadata."""
    return trigger_full_realtime_reset(force=force)


@router.get("/full-realtime-reset")
def get_full_realtime_reset_status() -> dict:
    """Return status for the most recent reset trigger request."""
    return reset_job_status()
