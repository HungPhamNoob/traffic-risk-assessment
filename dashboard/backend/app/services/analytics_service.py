"""Historical analytics queries for charts."""

from typing import Any

from psycopg2 import sql

from app.core.database import fetch_all
from app.services.prediction_service import (
    mode_table_identifier,
    table_identifier,
)


def timeseries(
    group_by: str, metric: str, start_time: str | None, end_time: str | None
) -> dict[str, Any]:
    """Return a selected metric grouped by day, month, or year."""
    allowed_groups = {"day": "day", "month": "month", "year": "year"}
    date_part = allowed_groups.get(group_by, "day")
    allowed_metrics = {
        "avg_risk": sql.SQL("AVG(risk_score)::DOUBLE PRECISION"),
        "count": sql.SQL("COUNT(*)::DOUBLE PRECISION"),
        "high_risk_count": sql.SQL(
            "COALESCE(SUM(CASE WHEN risk_score >= 0.7 THEN 1 ELSE 0 END), 0)::DOUBLE PRECISION"
        ),
    }
    metric_key = metric if metric in allowed_metrics else "avg_risk"
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
            {metric_expr} AS value,
            AVG(risk_score)::DOUBLE PRECISION AS avg_risk_score,
            COUNT(*)::BIGINT AS accident_count,
            COALESCE(SUM(CASE WHEN risk_score >= 0.7 THEN 1 ELSE 0 END), 0)::BIGINT AS high_risk_count
        FROM {table}
        WHERE {where_clause}
        GROUP BY DATE_TRUNC({date_part}, event_time)
        ORDER BY time
        """
    ).format(
        date_part=sql.Literal(date_part),
        metric_expr=allowed_metrics[metric_key],
        table=table_identifier(),
        where_clause=sql.SQL(" AND ").join(sql.SQL(clause) for clause in where_clauses),
    )
    rows = fetch_all(query, params)
    for row in rows:
        row["time"] = row["time"].isoformat()
    return {"series": rows, "metric": metric_key, "group_by": date_part}


def severity_distribution(mode: str | None = None) -> dict[str, Any]:
    """Return class imbalance counts using the ground-truth severity label."""
    tbl = mode_table_identifier(mode) if mode else table_identifier()
    query = sql.SQL(
        """
        SELECT true_severity AS severity, COUNT(*)::BIGINT AS count
        FROM {table}
        GROUP BY true_severity
        ORDER BY true_severity
        """
    ).format(table=tbl)
    return {"distribution": fetch_all(query)}


def risk_by_hour(mode: str | None = None) -> dict[str, Any]:
    """Return average risk and event count by hour of day."""
    tbl = mode_table_identifier(mode) if mode else table_identifier()
    query = sql.SQL(
        """
        SELECT hour, AVG(risk_score)::DOUBLE PRECISION AS avg_risk_score, COUNT(*)::BIGINT AS accident_count
        FROM {table}
        GROUP BY hour
        ORDER BY hour
        """
    ).format(table=tbl)
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


def weather_histogram(mode: str | None = None) -> dict[str, Any]:
    """Return temperature, humidity, and wind_speed histograms."""
    tbl = mode_table_identifier(mode) if mode else table_identifier()

    result: dict[str, Any] = {"histogram": {}}
    for col, bins, label in [
        ("temperature_f", 8, "temperature"),
        ("humidity", 8, "humidity"),
        ("wind_speed_mph", 8, "wind_speed"),
    ]:
        col_ident = sql.Identifier(col)
        query = sql.SQL(
            """
            SELECT
                WIDTH_BUCKET({col}, 0, {max_val}, {bins}) AS bin_idx,
                MIN({col})::DOUBLE PRECISION AS bin_min,
                MAX({col})::DOUBLE PRECISION AS bin_max,
                COUNT(*)::BIGINT AS count
            FROM {table}
            WHERE {col} IS NOT NULL
            GROUP BY bin_idx
            ORDER BY bin_idx
            """
        ).format(
            col=col_ident,
            table=tbl,
            bins=sql.Literal(bins),
            max_val=sql.Literal(
                100 if col == "humidity" else 130 if col == "temperature_f" else 100
            ),
        )
        rows = fetch_all(query)
        result["histogram"][label] = [
            {
                "bin": f"{row['bin_min']:.0f}-{row['bin_max']:.0f}",
                "count": row["count"],
            }
            for row in rows
        ]
    return result
