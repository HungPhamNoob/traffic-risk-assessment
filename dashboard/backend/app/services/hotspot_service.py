"""Hotspot ranking queries for repeated high-risk locations."""

from math import cos, radians
from typing import Any

from psycopg2 import sql

from app.core.database import fetch_all
from app.services.prediction_service import table_identifier


def top_hotspots(
    limit: int, min_events: int, start_time: str | None, end_time: str | None
) -> dict[str, Any]:
    """Group nearby events by rounded coordinates and return the riskiest cells."""
    params: dict[str, Any] = {"limit": limit, "min_events": min_events}
    where_clauses = ["lat IS NOT NULL", "lon IS NOT NULL"]

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
                AVG(risk_score)::DOUBLE PRECISION AS avg_risk_score,
                COUNT(*)::BIGINT AS accident_count,
                SUM(CASE WHEN true_severity >= 3 OR predicted_severity >= 3 THEN 1 ELSE 0 END)::BIGINT AS severe_count,
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
        table=table_identifier(),
        where_clause=sql.SQL(" AND ").join(sql.SQL(clause) for clause in where_clauses),
    )
    return {"hotspots": fetch_all(query, params)}


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
            risk_score,
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
    ).format(table=table_identifier())
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
            risk_score,
            111320 * SQRT(POWER(lat - %(lat)s, 2) + POWER((lon - %(lon)s) * COS(RADIANS(%(lat)s)), 2)) AS distance_m
        FROM {table}
        WHERE lat BETWEEN %(min_lat)s AND %(max_lat)s
          AND lon BETWEEN %(min_lon)s AND %(max_lon)s
        ORDER BY distance_m ASC
        LIMIT %(limit)s
        """
    ).format(table=table_identifier())
    return {
        "center": {"lat": lat, "lon": lon},
        "radius_m": radius_m,
        "events": fetch_all(query, params),
        "method": "lat_lon_fallback",
    }


def hotspot_detail(hotspot_id: int) -> dict[str, Any]:
    """Return a hotspot by one-based rank from the default ranking."""
    rows = top_hotspots(limit=hotspot_id, min_events=5, start_time=None, end_time=None)[
        "hotspots"
    ]
    if hotspot_id < 1 or hotspot_id > len(rows):
        return {"hotspot": None}
    return {"hotspot": rows[hotspot_id - 1]}
