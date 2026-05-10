"""Historical analytics queries for charts."""

from typing import Any

from psycopg2 import sql

from app.core.database import fetch_all
from app.services.prediction_service import table_identifier


def timeseries(
    group_by: str, metric: str, start_time: str | None, end_time: str | None
) -> dict[str, Any]:
    """Return accident count and average risk grouped by day, month, or year."""
    allowed_groups = {"day": "day", "month": "month", "year": "year"}
    date_part = allowed_groups.get(group_by, "day")
    params: dict[str, Any] = {}
    where_clauses = ["event_time IS NOT NULL"]
    if start_time:
        params["start_time"] = start_time
        where_clauses.append("event_time >= %(start_time)s")
    if end_time:
        params["end_time"] = end_time
        where_clauses.append("event_time <= %(end_time)s")

    query = sql.SQL(
        """
        SELECT
            DATE_TRUNC({date_part}, event_time)::DATE AS time,
            AVG(risk_score)::DOUBLE PRECISION AS avg_risk_score,
            COUNT(*)::BIGINT AS accident_count
        FROM {table}
        WHERE {where_clause}
        GROUP BY DATE_TRUNC({date_part}, event_time)
        ORDER BY time
        """
    ).format(
        date_part=sql.Literal(date_part),
        table=table_identifier(),
        where_clause=sql.SQL(" AND ").join(sql.SQL(clause) for clause in where_clauses),
    )
    rows = fetch_all(query, params)
    for row in rows:
        row["time"] = row["time"].isoformat()
    return {"series": rows, "metric": metric}


def severity_distribution() -> dict[str, Any]:
    """Return class imbalance counts using the ground-truth severity label."""
    query = sql.SQL(
        """
        SELECT true_severity AS severity, COUNT(*)::BIGINT AS count
        FROM {table}
        GROUP BY true_severity
        ORDER BY true_severity
        """
    ).format(table=table_identifier())
    return {"distribution": fetch_all(query)}


def risk_by_hour() -> dict[str, Any]:
    """Return average risk and event count by hour of day."""
    query = sql.SQL(
        """
        SELECT hour, AVG(risk_score)::DOUBLE PRECISION AS avg_risk_score, COUNT(*)::BIGINT AS accident_count
        FROM {table}
        GROUP BY hour
        ORDER BY hour
        """
    ).format(table=table_identifier())
    return {"data": fetch_all(query)}


def risk_by_weather() -> dict[str, Any]:
    """Return average risk and event count by normalized weather code."""
    query = sql.SQL(
        """
        SELECT weather_code, AVG(risk_score)::DOUBLE PRECISION AS avg_risk_score, COUNT(*)::BIGINT AS accident_count
        FROM {table}
        GROUP BY weather_code
        ORDER BY weather_code
        """
    ).format(table=table_identifier())
    return {"data": fetch_all(query)}
