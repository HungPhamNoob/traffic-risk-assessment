"""Prediction query service backed by PostgreSQL/PostGIS."""

from typing import Any

from fastapi import HTTPException
from psycopg2 import sql

from app.core.config import get_settings
from app.core.database import fetch_all, fetch_one


def table_identifier() -> sql.Identifier:
    """Return a safely quoted prediction table identifier."""
    return sql.Identifier(get_settings().prediction_table)


def overview_summary() -> dict[str, Any]:
    """Aggregate high-level prediction metrics for the overview page."""
    query = sql.SQL(
        """
        SELECT
            COUNT(*)::BIGINT AS total_events,
            COALESCE(SUM(CASE WHEN risk_score >= 0.7 THEN 1 ELSE 0 END), 0)::BIGINT AS high_risk_events,
            COALESCE(AVG(risk_score), 0)::DOUBLE PRECISION AS avg_risk_score,
            MAX(event_time) AS latest_event_time
        FROM {table}
        """
    ).format(table=table_identifier())
    row = fetch_one(query)
    settings = get_settings()
    return {
        "total_events": row["total_events"] if row else 0,
        "high_risk_events": row["high_risk_events"] if row else 0,
        "avg_risk_score": round(float(row["avg_risk_score"]), 4) if row else 0,
        "latest_event_time": (
            row["latest_event_time"].isoformat()
            if row and row["latest_event_time"]
            else None
        ),
        "latest_model_version": settings.model_version or "latest",
    }


def map_points(
    bbox: str | None,
    min_risk: float,
    start_time: str | None,
    end_time: str | None,
    limit: int,
) -> dict[str, list[dict[str, Any]]]:
    """Return prediction points for map rendering with optional spatial and time filters."""
    where_clauses = ["risk_score >= %(min_risk)s"]
    params: dict[str, Any] = {"min_risk": min_risk, "limit": limit}

    if bbox:
        parts = [float(value) for value in bbox.split(",")]
        if len(parts) != 4:
            raise HTTPException(
                status_code=400, detail="bbox must be min_lon,min_lat,max_lon,max_lat"
            )
        params.update(
            {
                "min_lon": parts[0],
                "min_lat": parts[1],
                "max_lon": parts[2],
                "max_lat": parts[3],
            }
        )
        where_clauses.append("lon BETWEEN %(min_lon)s AND %(max_lon)s")
        where_clauses.append("lat BETWEEN %(min_lat)s AND %(max_lat)s")
    if start_time:
        params["start_time"] = start_time
        where_clauses.append("event_time >= %(start_time)s")
    if end_time:
        params["end_time"] = end_time
        where_clauses.append("event_time <= %(end_time)s")

    query = sql.SQL(
        """
        SELECT event_id, lat, lon, risk_score, predicted_severity, true_severity, event_time
        FROM {table}
        WHERE {where_clause}
        ORDER BY event_time DESC NULLS LAST
        LIMIT %(limit)s
        """
    ).format(
        table=table_identifier(),
        where_clause=sql.SQL(" AND ").join(sql.SQL(clause) for clause in where_clauses),
    )
    rows = fetch_all(query, params)
    for row in rows:
        if row.get("event_time"):
            row["event_time"] = row["event_time"].isoformat()
    return {"points": rows}


def prediction_detail(event_id: str) -> dict[str, Any]:
    """Return the stored feature and prediction data for one accident event."""
    query = sql.SQL("SELECT * FROM {table} WHERE event_id = %(event_id)s").format(
        table=table_identifier()
    )
    row = fetch_one(query, {"event_id": event_id})
    if not row:
        raise HTTPException(status_code=404, detail="Prediction event not found")
    if row.get("event_time"):
        row["event_time"] = row["event_time"].isoformat()
    if row.get("created_at"):
        row["created_at"] = row["created_at"].isoformat()
    row.pop("geom", None)
    return row


def latest_predictions(limit: int) -> dict[str, list[dict[str, Any]]]:
    """Return the most recent prediction records."""
    query = sql.SQL(
        """
        SELECT event_id, event_time, lat, lon, risk_score, predicted_severity, true_severity
        FROM {table}
        ORDER BY event_time DESC NULLS LAST
        LIMIT %(limit)s
        """
    ).format(table=table_identifier())
    rows = fetch_all(query, {"limit": limit})
    for row in rows:
        if row.get("event_time"):
            row["event_time"] = row["event_time"].isoformat()
    return {"predictions": rows}
