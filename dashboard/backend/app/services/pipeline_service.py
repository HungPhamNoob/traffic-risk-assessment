"""Best-effort operational metrics for the dashboard pipeline health page."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import os
import re
import shutil
import subprocess
from typing import Any
import uuid

from psycopg2 import sql

from app.core.config import get_settings
from app.core.database import fetch_all, fetch_one
from app.services.prediction_service import table_identifier


def _prediction_table_name() -> str:
    return get_settings().prediction_table.split(".")[-1]


def _prediction_table_names() -> list[str]:
    settings = get_settings()
    names = [
        settings.us_prediction_table.split(".")[-1],
        settings.tomtom_events_table.split(".")[-1],
    ]
    return list(dict.fromkeys(names))


def _table_columns(table_name: str | None = None) -> set[str]:
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %(table_name)s
    """
    rows = fetch_all(query, {"table_name": table_name or _prediction_table_name()})
    return {str(row["column_name"]) for row in rows}


def _columns_for_table(table_name: str) -> set[str]:
    """Return columns for a table while keeping older tests easy to monkeypatch."""
    try:
        return _table_columns(table_name)
    except TypeError:
        return _table_columns()


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
    window_seconds = _parse_window_seconds(window)
    total_count = 0
    sources = []

    for table_name in _prediction_table_names():
        columns = _columns_for_table(table_name)
        column = _time_column(columns)
        if not column:
            sources.append(
                {
                    "table": table_name,
                    "status": "unavailable",
                    "event_count": 0,
                    "time_column": None,
                }
            )
            continue

        query = sql.SQL(
            """
            SELECT COUNT(*)::BIGINT AS event_count
            FROM {table}
            WHERE {time_column} >= NOW() - (%(window_seconds)s * INTERVAL '1 second')
            """
        ).format(
            table=table_identifier(table_name),
            time_column=sql.Identifier(column),
        )
        row = fetch_one(query, {"window_seconds": window_seconds}) or {}
        count = int(row.get("event_count") or 0)
        total_count += count
        sources.append(
            {
                "table": table_name,
                "status": "ok" if count else "not_enough_data",
                "event_count": count,
                "time_column": column,
            }
        )

    if not sources or all(source["status"] == "unavailable" for source in sources):
        return {
            "status": "unavailable",
            "window": window,
            "event_count": 0,
            "events_per_minute": 0.0,
            "events_per_second": 0.0,
            "sources": [],
        }

    return {
        "status": "ok" if total_count else "not_enough_data",
        "window": window,
        "window_seconds": window_seconds,
        "event_count": total_count,
        "events_per_minute": round(total_count / (window_seconds / 60.0), 4),
        "events_per_second": round(total_count / window_seconds, 4),
        "sources": sources,
    }


def latency(metric: str) -> dict[str, Any]:
    """Return latency percentiles using available latency columns."""
    selects: list[sql.Composable] = []
    source_columns: dict[str, list[str]] = {}
    for table_name in _prediction_table_names():
        columns = _columns_for_table(table_name)
        latency_columns = [
            column
            for column in ("end_to_end_latency_ms", "inference_latency_ms")
            if column in columns
        ]
        if not latency_columns:
            continue
        column = latency_columns[0]
        source_columns[table_name] = latency_columns
        selects.append(
            sql.SQL(
                """
                SELECT {latency_column} AS latency_ms
                FROM {table}
                WHERE {latency_column} IS NOT NULL
                """
            ).format(
                table=table_identifier(table_name),
                latency_column=sql.Identifier(column),
            )
        )

    if not selects:
        return {"status": "unavailable", "metric": metric, "columns": []}

    query = sql.SQL(
        """
        SELECT
            percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms)::DOUBLE PRECISION AS p50,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)::DOUBLE PRECISION AS p95,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms)::DOUBLE PRECISION AS p99,
            AVG(latency_ms)::DOUBLE PRECISION AS avg,
            COUNT(latency_ms)::BIGINT AS sample_count
        FROM ({union_query}) AS latency_samples
        """
    ).format(union_query=sql.SQL(" UNION ALL ").join(selects))
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
        "columns": source_columns,
    }


def replay_health() -> dict[str, Any]:
    """Return recent replay and model-status metadata from the prediction table."""
    source_health = []
    total_rows = 0
    for table_name in _prediction_table_names():
        columns = _columns_for_table(table_name)
        if not columns:
            source_health.append(
                {"table": table_name, "status": "unavailable", "row_count": 0}
            )
            continue

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
            table=table_identifier(table_name),
            select_event=select_event,
            select_created=select_created,
        )
        summary = fetch_one(summary_query) or {}
        total_rows += int(summary.get("row_count") or 0)

        if "model_status" in columns:
            status_query = sql.SQL(
                """
                SELECT COALESCE(model_status, 'unknown') AS status, COUNT(*)::BIGINT AS count
                FROM {table}
                GROUP BY COALESCE(model_status, 'unknown')
                ORDER BY count DESC
                """
            ).format(table=table_identifier(table_name))
            model_status = fetch_all(status_query)
        else:
            model_status = []

        for key in ("latest_event_time", "latest_created_at"):
            if summary.get(key):
                summary[key] = summary[key].isoformat()

        source_health.append(
            {
                "table": table_name,
                "status": "ok" if summary.get("row_count") else "not_enough_data",
                "row_count": summary.get("row_count") or 0,
                "latest_event_time": summary.get("latest_event_time"),
                "latest_created_at": summary.get("latest_created_at"),
                "model_status": model_status,
            }
        )

    if not source_health:
        return {
            "status": "unavailable",
            "row_count": 0,
            "latest_event_time": None,
            "latest_created_at": None,
            "model_status": [],
        }

    return {
        "status": "ok" if total_rows else "not_enough_data",
        "row_count": total_rows,
        "sources": source_health,
    }


def _reset_state_file() -> Path:
    settings = get_settings()
    state_dir = Path(settings.pipeline_reset_log_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    return state_dir / "full_realtime_reset_state.json"


def _tail_lines(path: Path, line_count: int = 20) -> list[str]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return lines[-line_count:]


def _pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def reset_job_status() -> dict[str, Any]:
    """Return status metadata for the most recently launched full realtime reset."""
    state_file = _reset_state_file()
    if not state_file.exists():
        return {"status": "not_started"}

    payload = json.loads(state_file.read_text(encoding="utf-8"))
    pid = int(payload.get("pid", 0) or 0)
    log_path = Path(str(payload.get("log_path", "")))
    running = pid > 0 and _pid_running(pid)
    return {
        "status": "running" if running else "finished",
        "pid": pid,
        "run_id": payload.get("run_id"),
        "script": payload.get("script"),
        "started_at": payload.get("started_at"),
        "log_path": str(log_path) if log_path else None,
        "last_log_lines": _tail_lines(log_path, line_count=20) if log_path else [],
    }


def trigger_full_realtime_reset(force: bool = False) -> dict[str, Any]:
    """
    Launch the full realtime reset shell script as a background process.

    This endpoint is designed for operator-triggered restarts from the dashboard.
    """
    current = reset_job_status()
    if current.get("status") == "running" and not force:
        return {
            "status": "already_running",
            "message": "A reset job is already running. Set force=true to start another run.",
            **current,
        }

    settings = get_settings()
    script_path = Path(settings.pipeline_reset_script)
    if not script_path.exists():
        return {
            "status": "script_missing",
            "script": str(script_path),
            "message": "Reset script path does not exist on this runtime host.",
        }

    if not shutil.which("gcloud"):
        return {
            "status": "missing_dependency",
            "script": str(script_path),
            "message": "gcloud CLI is not installed in the backend runtime. Run the reset script on the VM host shell.",
        }

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    log_dir = Path(settings.pipeline_reset_log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"full_realtime_reset_{run_id}.log"

    with log_path.open("a", encoding="utf-8") as log_file:
        process = subprocess.Popen(
            ["bash", str(script_path)],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    state_file = _reset_state_file()
    state_file.write_text(
        json.dumps(
            {
                "pid": process.pid,
                "run_id": run_id,
                "script": str(script_path),
                "log_path": str(log_path),
                "started_at": datetime.now(timezone.utc).isoformat(),
                "force": force,
            },
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )

    return {
        "status": "started",
        "pid": process.pid,
        "run_id": run_id,
        "script": str(script_path),
        "log_path": str(log_path),
    }


def _path_status(path_value: str | None) -> dict[str, Any]:
    if not path_value:
        return {"path": None, "status": "unavailable", "last_modified": None}
    if path_value.startswith("gs://"):
        return _gcs_path_status(path_value)

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


def _gcs_path_status(path_value: str) -> dict[str, Any]:
    """Return best-effort freshness metadata for a GCS prefix."""
    try:
        from google.cloud import storage
    except Exception as exc:
        return {
            "path": path_value,
            "status": "configured_remote",
            "last_modified": None,
            "note": f"GCS client unavailable: {exc}",
        }

    match = re.fullmatch(r"gs://([^/]+)(?:/(.*))?", path_value.rstrip("/"))
    if not match:
        return {
            "path": path_value,
            "status": "invalid",
            "last_modified": None,
            "note": "Invalid GCS path format.",
        }

    bucket_name, prefix = match.group(1), (match.group(2) or "").rstrip("/")
    prefix = f"{prefix}/" if prefix else ""

    try:
        client = storage.Client()
        blobs = list(
            client.list_blobs(bucket_name, prefix=prefix, max_results=2000)  # type: ignore[arg-type]
        )
    except Exception as exc:
        return {
            "path": path_value,
            "status": "configured_remote",
            "last_modified": None,
            "note": f"GCS lookup failed: {exc}",
        }

    if not blobs:
        return {
            "path": path_value,
            "status": "empty",
            "last_modified": None,
            "file_count": 0,
        }

    latest_blob = max(
        (blob for blob in blobs if blob.updated is not None),
        key=lambda blob: blob.updated,  # type: ignore[arg-type]
        default=None,
    )
    result = {
        "path": path_value,
        "status": "ok",
        "last_modified": latest_blob.updated.isoformat() if latest_blob else None,
        "file_count": len(blobs),
        "sample_blob": blobs[0].name,
    }
    if len(blobs) == 2000:
        result["note"] = "Freshness is based on the first 2000 blobs under the prefix."
    return result
