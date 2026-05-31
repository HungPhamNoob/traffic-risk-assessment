"""Historical analytics queries for charts."""

from typing import Any

from psycopg2 import sql

from app.core.config import get_settings
from app.core.database import fetch_all, fetch_one
from app.services.prediction_service import (
    table_identifier,
    tomtom_table_identifier,
    us_table_identifier,
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


def _table_exists(table_name: str) -> bool:
    query = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %(table_name)s
        ) AS exists
    """
    row = fetch_one(query, {"table_name": table_name.split(".")[-1]})
    return bool(row and row.get("exists"))


def _normalize_mode(mode: str | None) -> str:
    if mode in {"replay", "live", "full"}:
        return mode
    return "full"


def _mode_sources(mode: str | None) -> list[dict[str, sql.Composable]]:
    settings = get_settings()
    normalized_mode = _normalize_mode(mode)
    sources: list[dict[str, sql.Composable]] = []

    if normalized_mode in {"replay", "full"} and _table_exists(
        settings.us_prediction_table
    ):
        sources.append(
            {
                "table": us_table_identifier(),
                "severity_column": sql.Identifier("true_severity"),
                "hour_column": sql.Identifier("hour"),
                "temperature_column": sql.Identifier("temperature_f"),
                "humidity_column": sql.Identifier("humidity"),
                "wind_column": sql.Identifier("wind_speed_mph"),
            }
        )

    if normalized_mode in {"live", "full"} and _table_exists(
        settings.tomtom_events_table
    ):
        sources.append(
            {
                "table": tomtom_table_identifier(),
                "severity_column": sql.Identifier("severity"),
                "hour_column": sql.Identifier("hour"),
                "temperature_column": sql.Identifier("temperature_f"),
                "humidity_column": sql.Identifier("humidity"),
                "wind_column": sql.Identifier("wind_speed_mph"),
            }
        )

    return sources


def severity_distribution(mode: str | None = None) -> dict[str, Any]:
    """Return class imbalance counts using the ground-truth severity label."""
    sources = _mode_sources(mode)
    if not sources:
        return {"distribution": []}

    selects = []
    for source in sources:
        selects.append(
            sql.SQL(
                """
                SELECT {severity_column} AS severity
                FROM {table}
                WHERE {severity_column} IS NOT NULL
                """
            ).format(
                severity_column=source["severity_column"],
                table=source["table"],
            )
        )

    query = sql.SQL(
        """
        SELECT severity, COUNT(*)::BIGINT AS count
        FROM ({union_query}) AS severity_events
        GROUP BY severity
        ORDER BY severity
        """
    ).format(union_query=sql.SQL(" UNION ALL ").join(selects))
    return {"distribution": fetch_all(query)}


def risk_by_hour(mode: str | None = None) -> dict[str, Any]:
    """Return average risk and event count by hour of day."""
    sources = _mode_sources(mode)
    if not sources:
        return {"data": []}

    selects = []
    for source in sources:
        selects.append(
            sql.SQL(
                """
                SELECT
                    {hour_column} AS hour,
                    risk_score
                FROM {table}
                WHERE {hour_column} IS NOT NULL AND risk_score IS NOT NULL
                """
            ).format(
                hour_column=source["hour_column"],
                table=source["table"],
            )
        )

    query = sql.SQL(
        """
        SELECT
            hour,
            AVG(risk_score)::DOUBLE PRECISION AS avg_risk_score,
            COUNT(*)::BIGINT AS accident_count
        FROM ({union_query}) AS hourly_events
        GROUP BY hour
        ORDER BY hour
        """
    ).format(union_query=sql.SQL(" UNION ALL ").join(selects))
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
    sources = _mode_sources(mode)
    if not sources:
        return {"histogram": {"temperature": [], "humidity": [], "wind_speed": []}}

    result: dict[str, Any] = {"histogram": {}}
    for key, bins, label in [
        ("temperature_column", 8, "temperature"),
        ("humidity_column", 8, "humidity"),
        ("wind_column", 8, "wind_speed"),
    ]:
        selects = []
        for source in sources:
            selects.append(
                sql.SQL(
                    """
                    SELECT {value_column} AS metric_value
                    FROM {table}
                    WHERE {value_column} IS NOT NULL
                    """
                ).format(
                    value_column=source[key],
                    table=source["table"],
                )
            )

        query = sql.SQL(
            """
            SELECT
                WIDTH_BUCKET(metric_value, 0, {max_val}, {bins}) AS bin_idx,
                MIN(metric_value)::DOUBLE PRECISION AS bin_min,
                MAX(metric_value)::DOUBLE PRECISION AS bin_max,
                COUNT(*)::BIGINT AS count
            FROM ({union_query}) AS weather_values
            GROUP BY bin_idx
            ORDER BY bin_idx
            """
        ).format(
            bins=sql.Literal(bins),
            union_query=sql.SQL(" UNION ALL ").join(selects),
            max_val=sql.Literal(
                100
                if key == "humidity_column"
                else 130
                if key == "temperature_column"
                else 100
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
