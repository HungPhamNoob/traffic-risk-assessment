"""Best-effort operational metrics for the dashboard pipeline health page."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import os
import re
from typing import Any

from psycopg2 import sql

from app.core.config import get_settings
from app.core.database import fetch_all, fetch_one
from app.services.prediction_service import table_identifier


def _prediction_table_name() -> str:
    return get_settings().prediction_table.split(".")[-1]


def _table_columns() -> set[str]:
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %(table_name)s
    """
    rows = fetch_all(query, {"table_name": _prediction_table_name()})
    return {str(row["column_name"]) for row in rows}


def _parse_window_seconds(window: str) -> int:
    match = re.fullmatch(r"\s*(\d+)\s*([smhd])\s*", window or "5m")
    if not match:
        return 300
    value = int(match.group(1))
    unit = match.group(2)
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    return max(1, value * multipliers[unit])


def _time_column(columns: set[str]) -> str | None:
    if "created_at" in columns:
        return "created_at"
    if "event_time" in columns:
        return "event_time"
    return None


def throughput(window: str) -> dict[str, Any]:
    """Return event throughput over the requested lookback window."""
    columns = _table_columns()
    column = _time_column(columns)
    window_seconds = _parse_window_seconds(window)
    if not column:
        return {
            "status": "unavailable",
            "window": window,
            "event_count": 0,
            "events_per_minute": 0.0,
            "time_column": None,
        }

    query = sql.SQL(
        """
        SELECT COUNT(*)::BIGINT AS event_count
        FROM {table}
        WHERE {time_column} >= NOW() - (%(window_seconds)s * INTERVAL '1 second')
        """
    ).format(table=table_identifier(), time_column=sql.Identifier(column))
    row = fetch_one(query, {"window_seconds": window_seconds}) or {}
    count = int(row.get("event_count") or 0)
    return {
        "status": "ok" if count else "not_enough_data",
        "window": window,
        "window_seconds": window_seconds,
        "event_count": count,
        "events_per_minute": round(count / (window_seconds / 60.0), 4),
        "time_column": column,
    }


def latency(metric: str) -> dict[str, Any]:
    """Return latency percentiles using available latency columns."""
    columns = _table_columns()
    latency_columns = [
        column
        for column in ("end_to_end_latency_ms", "inference_latency_ms")
        if column in columns
    ]
    if not latency_columns:
        return {"status": "unavailable", "metric": metric, "columns": []}

    column = latency_columns[0]
    query = sql.SQL(
        """
        SELECT
            percentile_cont(0.5) WITHIN GROUP (ORDER BY {latency_column})::DOUBLE PRECISION AS p50,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY {latency_column})::DOUBLE PRECISION AS p95,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY {latency_column})::DOUBLE PRECISION AS p99,
            AVG({latency_column})::DOUBLE PRECISION AS avg,
            COUNT({latency_column})::BIGINT AS sample_count
        FROM {table}
        WHERE {latency_column} IS NOT NULL
        """
    ).format(table=table_identifier(), latency_column=sql.Identifier(column))
    row = fetch_one(query) or {}
    allowed = {"p50", "p95", "p99", "avg"}
    metric_key = metric if metric in allowed else "p95"
    return {
        "status": "ok" if row.get("sample_count") else "not_enough_data",
        "metric": metric_key,
        "value_ms": row.get(metric_key),
        "latency_ms": {
            "p50": row.get("p50"),
            "p95": row.get("p95"),
            "p99": row.get("p99"),
            "avg": row.get("avg"),
        },
        "sample_count": row.get("sample_count") or 0,
        "column": column,
        "columns": latency_columns,
    }


def replay_health() -> dict[str, Any]:
    """Return recent replay and model-status metadata from the prediction table."""
    columns = _table_columns()
    if not columns:
        return {
            "status": "unavailable",
            "row_count": 0,
            "latest_event_time": None,
            "latest_created_at": None,
            "model_status": [],
        }

    select_created = (
        sql.SQL("MAX(created_at) AS latest_created_at")
        if "created_at" in columns
        else sql.SQL("NULL AS latest_created_at")
    )
    select_event = (
        sql.SQL("MAX(event_time) AS latest_event_time")
        if "event_time" in columns
        else sql.SQL("NULL AS latest_event_time")
    )
    summary_query = sql.SQL(
        """
        SELECT COUNT(*)::BIGINT AS row_count, {select_event}, {select_created}
        FROM {table}
        """
    ).format(
        table=table_identifier(),
        select_event=select_event,
        select_created=select_created,
    )
    summary = fetch_one(summary_query) or {}

    if "model_status" in columns:
        status_query = sql.SQL(
            """
            SELECT COALESCE(model_status, 'unknown') AS status, COUNT(*)::BIGINT AS count
            FROM {table}
            GROUP BY COALESCE(model_status, 'unknown')
            ORDER BY count DESC
            """
        ).format(table=table_identifier())
        model_status = fetch_all(status_query)
    else:
        model_status = []

    for key in ("latest_event_time", "latest_created_at"):
        if summary.get(key):
            summary[key] = summary[key].isoformat()

    return {
        "status": "ok" if summary.get("row_count") else "not_enough_data",
        "row_count": summary.get("row_count") or 0,
        "latest_event_time": summary.get("latest_event_time"),
        "latest_created_at": summary.get("latest_created_at"),
        "model_status": model_status,
    }


def _path_status(path_value: str | None) -> dict[str, Any]:
    if not path_value:
        return {"path": None, "status": "unavailable", "last_modified": None}
    if path_value.startswith("gs://"):
        return {
            "path": path_value,
            "status": "configured_remote",
            "last_modified": None,
            "note": "GCS timestamp probing is not available in the backend image.",
        }

    local_path = path_value.replace("file://", "", 1)
    path = Path(local_path)
    if not path.exists():
        return {"path": path_value, "status": "missing", "last_modified": None}

    candidates = [path]
    if path.is_dir():
        candidates = [item for item in path.rglob("*") if item.is_file()]
    latest_mtime = max((item.stat().st_mtime for item in candidates), default=None)
    last_modified = (
        datetime.fromtimestamp(latest_mtime, tz=timezone.utc).isoformat()
        if latest_mtime
        else None
    )
    return {
        "path": path_value,
        "status": "ok",
        "last_modified": last_modified,
        "file_count": len(candidates) if path.is_dir() else 1,
    }


def checkpoints() -> dict[str, Any]:
    """Return configured checkpoint and Gold output paths with best-effort timestamps."""
    settings = get_settings()
    return {
        "flink": _path_status(settings.flink_checkpoint_dir),
        "gold": _path_status(settings.gold_retrain_path),
        "environment": settings.environment,
        "cwd": os.getcwd(),
    }


def gold_last_update() -> str | None:
    """Return the best-effort Gold dataset last modified timestamp."""
    return _path_status(get_settings().gold_retrain_path).get("last_modified")
