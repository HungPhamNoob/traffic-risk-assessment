"""Hotspot ranking queries for repeated high-risk locations."""

from datetime import datetime, timedelta
from math import ceil, cos, radians
from typing import Any, Literal

from psycopg2 import sql

from app.core.database import fetch_all, fetch_one
from app.core.config import get_settings
from app.services.prediction_service import (
    table_identifier,
    tomtom_table_identifier,
    us_table_identifier,
)
from app.services.risk_sql import effective_us_risk_score_expr
from app.services.risk_sql import effective_tomtom_risk_score_expr

MapMode = Literal["replay", "live", "full"]

DEFAULT_REPLAY_LOOKBACK = timedelta(days=7)
DEFAULT_LIVE_LOOKBACK = timedelta(hours=24)
EXPENSIVE_SCAN_ROW_THRESHOLD = 500_000


def _normalize_mode(mode: str | None) -> MapMode:
    if mode in {"replay", "live", "full"}:
        return mode
    return "full"


def _table_exists(table_name: str) -> bool:
    row = fetch_one(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %(table_name)s
        ) AS exists
        """,
        {"table_name": table_name.split(".")[-1]},
    )
    return bool(row and row.get("exists"))


def _latest_event_time(table: sql.Identifier) -> datetime | None:
    row = fetch_one(
        sql.SQL(
            """
            SELECT event_time
            FROM {table}
            WHERE event_time IS NOT NULL
            ORDER BY event_time DESC NULLS LAST
            LIMIT 1
            """
        ).format(table=table)
    )
    value = row.get("event_time") if row else None
    return value if isinstance(value, datetime) else None


def _table_row_estimate(table_name: str) -> int:
    row = fetch_one(
        """
        SELECT COALESCE(c.reltuples, 0)::BIGINT AS row_estimate
        FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = %(table_name)s
        """,
        {"table_name": table_name},
    )
    return int(row.get("row_estimate") or 0) if row else 0


def _index_valid(index_name: str) -> bool:
    row = fetch_one(
        """
        SELECT COALESCE(
            (
                SELECT i.indisvalid
                FROM pg_index AS i
                JOIN pg_class AS c ON c.oid = i.indexrelid
                WHERE c.relname = %(index_name)s
            ),
            FALSE
        ) AS is_valid
        """,
        {"index_name": index_name},
    )
    return bool(row and row.get("is_valid"))


def _skip_default_replay_scan() -> bool:
    if _index_valid("idx_traffic_risk_predictions_event_time"):
        return False
    return _table_row_estimate("traffic_risk_predictions") >= EXPENSIVE_SCAN_ROW_THRESHOLD


def _default_time_bounds(
    table: sql.Identifier,
    *,
    start_time: str | None,
    end_time: str | None,
    lookback: timedelta | None,
) -> tuple[str | datetime | None, str | datetime | None]:
    if start_time or end_time or lookback is None:
        return start_time, end_time

    latest_time = _latest_event_time(table)
    if latest_time is None:
        return None, None
    return latest_time - lookback, latest_time


def _query_hotspots_for_table(
    *,
    table: sql.Identifier,
    risk_score: sql.Composable,
    severe_expr: sql.Composable,
    limit: int,
    min_events: int,
    start_time: str | datetime | None,
    end_time: str | datetime | None,
) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"limit": limit, "min_events": min_events}
    where_clauses = ["lat IS NOT NULL", "lon IS NOT NULL", "{risk_score} IS NOT NULL"]

    if start_time:
        params["start_time"] = start_time
        where_clauses.append("event_time >= %(start_time)s")
    if end_time:
        params["end_time"] = end_time
        where_clauses.append("event_time <= %(end_time)s")

    query = sql.SQL(
        """
        WITH grouped AS (
            SELECT
                ROUND(lat::numeric, 3)::DOUBLE PRECISION AS center_lat,
                ROUND(lon::numeric, 3)::DOUBLE PRECISION AS center_lon,
                AVG({risk_score})::DOUBLE PRECISION AS avg_risk_score,
                COUNT(*)::BIGINT AS accident_count,
                SUM(CASE WHEN {severe_expr} THEN 1 ELSE 0 END)::BIGINT AS severe_count,
                MODE() WITHIN GROUP (ORDER BY hour) AS peak_hour
            FROM {table}
            WHERE {where_clause}
            GROUP BY ROUND(lat::numeric, 3), ROUND(lon::numeric, 3)
            HAVING COUNT(*) >= %(min_events)s
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY avg_risk_score DESC, accident_count DESC)::INT AS rank,
            center_lat,
            center_lon,
            avg_risk_score,
            accident_count,
            severe_count,
            peak_hour
        FROM grouped
        ORDER BY avg_risk_score DESC, accident_count DESC
        LIMIT %(limit)s
        """
    ).format(
        risk_score=risk_score,
        severe_expr=severe_expr,
        table=table,
        where_clause=sql.SQL(" AND ").join(
            sql.SQL(clause).format(risk_score=risk_score) for clause in where_clauses
        ),
    )
    return fetch_all(query, params)


def top_hotspots(
    limit: int,
    min_events: int,
    start_time: str | None,
    end_time: str | None,
    mode: str | None = None,
) -> dict[str, Any]:
    """Group nearby events by rounded coordinates and return the riskiest cells."""
    settings = get_settings()
    normalized_mode = _normalize_mode(mode)
    sources: list[dict[str, Any]] = []

    if normalized_mode in {"replay", "full"} and _table_exists(
        settings.us_prediction_table
    ):
        if start_time or end_time or not _skip_default_replay_scan():
            sources.append(
                {
                    "label": "us_replay",
                    "table": us_table_identifier(),
                    "risk_score": effective_us_risk_score_expr(),
                    "severe_expr": sql.SQL(
                        "true_severity >= 3 OR predicted_severity >= 3"
                    ),
                    "lookback": DEFAULT_REPLAY_LOOKBACK,
                }
            )
    if normalized_mode in {"live", "full"} and _table_exists(
        settings.tomtom_events_table
    ):
        sources.append(
            {
                "label": "tomtom_live",
                "table": tomtom_table_identifier(),
                "risk_score": effective_tomtom_risk_score_expr(),
                "severe_expr": sql.SQL("severity >= 3"),
                "lookback": DEFAULT_LIVE_LOOKBACK,
            }
        )

    if not sources:
        return {"hotspots": []}

    per_source_limit = limit if len(sources) == 1 else max(limit, ceil(limit / len(sources)) * 3)
    hotspots: list[dict[str, Any]] = []
    for source in sources:
        bounded_start, bounded_end = _default_time_bounds(
            source["table"],
            start_time=start_time,
            end_time=end_time,
            lookback=source["lookback"],
        )
        source_rows = _query_hotspots_for_table(
            table=source["table"],
            risk_score=source["risk_score"],
            severe_expr=source["severe_expr"],
            limit=per_source_limit,
            min_events=min_events,
            start_time=bounded_start,
            end_time=bounded_end,
        )
        for row in source_rows:
            row["data_source"] = source["label"]
        hotspots.extend(source_rows)

    hotspots.sort(
        key=lambda row: (
            float(row.get("avg_risk_score") or 0.0),
            int(row.get("accident_count") or 0),
        ),
        reverse=True,
    )
    ranked = hotspots[:limit]
    for index, row in enumerate(ranked, start=1):
        row["rank"] = index
    return {"hotspots": ranked}


def nearby_events(
    lat: float, lon: float, radius_m: float, limit: int
) -> dict[str, Any]:
    """Return events inside an approximate radius around a map point."""
    postgis_query = sql.SQL(
        """
        SELECT
            event_id,
            lat,
            lon,
            {risk_score} AS risk_score,
            ST_Distance(
                geom::geography,
                ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography
            ) AS distance_m
        FROM {table}
        WHERE geom IS NOT NULL
          AND ST_DWithin(
                geom::geography,
                ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography,
                %(radius_m)s
          )
        ORDER BY distance_m ASC
        LIMIT %(limit)s
        """
    ).format(
        risk_score=effective_us_risk_score_expr(),
        table=table_identifier(),
    )
    postgis_params = {
        "lat": lat,
        "lon": lon,
        "radius_m": radius_m,
        "limit": limit,
    }
    try:
        return {
            "center": {"lat": lat, "lon": lon},
            "radius_m": radius_m,
            "events": fetch_all(postgis_query, postgis_params),
            "method": "postgis",
        }
    except Exception:
        pass

    lat_delta = radius_m / 111_320.0
    lon_delta = radius_m / max(1.0, 111_320.0 * cos(radians(lat)))
    params = {
        "lat": lat,
        "lon": lon,
        "min_lat": lat - lat_delta,
        "max_lat": lat + lat_delta,
        "min_lon": lon - lon_delta,
        "max_lon": lon + lon_delta,
        "limit": limit,
    }
    query = sql.SQL(
        """
        SELECT
            event_id,
            lat,
            lon,
            {risk_score} AS risk_score,
            111320 * SQRT(POWER(lat - %(lat)s, 2) + POWER((lon - %(lon)s) * COS(RADIANS(%(lat)s)), 2)) AS distance_m
        FROM {table}
        WHERE lat BETWEEN %(min_lat)s AND %(max_lat)s
          AND lon BETWEEN %(min_lon)s AND %(max_lon)s
        ORDER BY distance_m ASC
        LIMIT %(limit)s
        """
    ).format(
        risk_score=effective_us_risk_score_expr(),
        table=table_identifier(),
    )
    return {
        "center": {"lat": lat, "lon": lon},
        "radius_m": radius_m,
        "events": fetch_all(query, params),
        "method": "lat_lon_fallback",
    }


def hotspot_detail(hotspot_id: int) -> dict[str, Any]:
    """Return a hotspot by one-based rank from the default ranking."""
    rows = top_hotspots(
        limit=hotspot_id,
        min_events=5,
        start_time=None,
        end_time=None,
        mode="full",
    )["hotspots"]
    if hotspot_id < 1 or hotspot_id > len(rows):
        return {"hotspot": None}
    return {"hotspot": rows[hotspot_id - 1]}
