"""Historical analytics queries for charts."""

import logging

from typing import Any

from psycopg2 import sql

from app.core.config import get_settings
from app.core.database import fetch_all, fetch_one
from app.core.runtime_cache import cached_result
from app.services.prediction_service import (
    table_identifier,
    tomtom_table_identifier,
    us_table_identifier,
)
from app.services.risk_sql import (
    effective_tomtom_risk_score_expr,
    effective_us_risk_score_expr,
)

logger = logging.getLogger(__name__)
ANALYTICS_CACHE_TTL_SECONDS = 300.0
TIMESERIES_CACHE_TTL_SECONDS = 600.0


def timeseries(
    group_by: str, metric: str, start_time: str | None, end_time: str | None
) -> dict[str, Any]:
    """Return a selected metric grouped by day, month, or year."""
    def load_timeseries() -> dict[str, Any]:
        return _load_timeseries(group_by, metric, start_time, end_time)

    return cached_result(
        "timeseries",
        (group_by, metric, start_time, end_time),
        TIMESERIES_CACHE_TTL_SECONDS,
        load_timeseries,
    )


def _load_timeseries(
    group_by: str, metric: str, start_time: str | None, end_time: str | None
) -> dict[str, Any]:
    """Load a selected metric grouped by day, month, or year."""
    allowed_groups = {"day": "day", "month": "month", "year": "year"}
    date_part = allowed_groups.get(group_by, "day")
    risk_score = effective_us_risk_score_expr()
    allowed_metrics = {
        "avg_risk": sql.SQL("AVG({risk_score})::DOUBLE PRECISION").format(
            risk_score=risk_score
        ),
        "count": sql.SQL("COUNT(*)::DOUBLE PRECISION"),
        "high_risk_count": sql.SQL(
            "COALESCE(SUM(CASE WHEN {risk_score} >= 0.7 THEN 1 ELSE 0 END), 0)::DOUBLE PRECISION"
        ).format(risk_score=risk_score),
    }
    metric_key = metric if metric in allowed_metrics else "avg_risk"
    params: dict[str, Any] = {}
    where_clauses = ["event_time IS NOT NULL", "{risk_score} IS NOT NULL"]
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
            AVG({risk_score})::DOUBLE PRECISION AS avg_risk_score,
            COUNT(*)::BIGINT AS accident_count,
            COALESCE(SUM(CASE WHEN {risk_score} >= 0.7 THEN 1 ELSE 0 END), 0)::BIGINT AS high_risk_count
        FROM {table}
        WHERE {where_clause}
        GROUP BY DATE_TRUNC({date_part}, event_time)
        ORDER BY time
        """
    ).format(
        date_part=sql.Literal(date_part),
        metric_expr=allowed_metrics[metric_key],
        risk_score=risk_score,
        table=table_identifier(),
        where_clause=sql.SQL(" AND ").join(
            sql.SQL(clause).format(risk_score=risk_score) for clause in where_clauses
        ),
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
                "risk_score": effective_us_risk_score_expr(),
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
                "risk_score": effective_tomtom_risk_score_expr(),
                "hour_column": sql.Identifier("hour"),
                "temperature_column": sql.Identifier("temperature_f"),
                "humidity_column": sql.Identifier("humidity"),
                "wind_column": sql.Identifier("wind_speed_mph"),
            }
        )

    return sources


def severity_distribution(mode: str | None = None) -> dict[str, Any]:
    """Return class imbalance counts using the ground-truth severity label."""
    normalized_mode = _normalize_mode(mode)

    def load_distribution() -> dict[str, Any]:
        sources = _mode_sources(normalized_mode)
        if not sources:
            return {"distribution": []}

        selects = []
        for source in sources:
            selects.append(
                sql.SQL(
                    """
                    SELECT
                        {severity_column} AS severity,
                        COUNT(*)::BIGINT AS event_count
                    FROM {table}
                    WHERE {severity_column} IS NOT NULL
                    GROUP BY {severity_column}
                    """
                ).format(
                    severity_column=source["severity_column"],
                    table=source["table"],
                )
            )

        query = sql.SQL(
            """
            SELECT severity, SUM(event_count)::BIGINT AS count
            FROM ({union_query}) AS severity_events
            GROUP BY severity
            ORDER BY severity
            """
        ).format(union_query=sql.SQL(" UNION ALL ").join(selects))
        try:
            return {"distribution": fetch_all(query)}
        except Exception:
            logger.exception(
                "Severity distribution query failed for mode=%s", normalized_mode
            )
            return {"distribution": []}

    return cached_result(
        "severity_distribution",
        (normalized_mode,),
        ANALYTICS_CACHE_TTL_SECONDS,
        load_distribution,
    )


def risk_by_hour(mode: str | None = None) -> dict[str, Any]:
    """Return average risk and event count by hour of day."""
    normalized_mode = _normalize_mode(mode)

    def load_risk_by_hour() -> dict[str, Any]:
        sources = _mode_sources(normalized_mode)
        if not sources:
            return {"data": []}

        selects = []
        for source in sources:
            selects.append(
                sql.SQL(
                    """
                    SELECT
                        {hour_column} AS hour,
                        COALESCE(SUM({risk_score}), 0)::DOUBLE PRECISION AS risk_score_sum,
                        COUNT(*)::BIGINT AS event_count
                    FROM {table}
                    WHERE {hour_column} IS NOT NULL AND {risk_score} IS NOT NULL
                    GROUP BY {hour_column}
                    """
                ).format(
                    hour_column=source["hour_column"],
                    risk_score=source["risk_score"],
                    table=source["table"],
                )
            )

        query = sql.SQL(
            """
            SELECT
                hour,
                CASE
                    WHEN COALESCE(SUM(event_count), 0) = 0 THEN 0::DOUBLE PRECISION
                    ELSE COALESCE(SUM(risk_score_sum), 0)::DOUBLE PRECISION
                        / SUM(event_count)::DOUBLE PRECISION
                END AS avg_risk_score,
                COALESCE(SUM(event_count), 0)::BIGINT AS accident_count
            FROM ({union_query}) AS hourly_events
            GROUP BY hour
            ORDER BY hour
            """
        ).format(union_query=sql.SQL(" UNION ALL ").join(selects))
        try:
            return {"data": fetch_all(query)}
        except Exception:
            logger.exception("Risk-by-hour query failed for mode=%s", normalized_mode)
            return {"data": []}

    return cached_result(
        "risk_by_hour",
        (normalized_mode,),
        ANALYTICS_CACHE_TTL_SECONDS,
        load_risk_by_hour,
    )


def risk_by_weather() -> dict[str, Any]:
    """Return average risk and event count by normalized weather code."""
    risk_score = effective_us_risk_score_expr()
    query = sql.SQL(
        """
        SELECT
            weather_code,
            AVG({risk_score})::DOUBLE PRECISION AS avg_risk_score,
            COUNT(*)::BIGINT AS accident_count
        FROM {table}
        WHERE {risk_score} IS NOT NULL
        GROUP BY weather_code
        ORDER BY weather_code
        """
    ).format(
        risk_score=risk_score,
        table=table_identifier(),
    )
    return {"data": fetch_all(query)}


def weather_histogram(mode: str | None = None) -> dict[str, Any]:
    """Return temperature, humidity, and wind_speed histograms."""
    normalized_mode = _normalize_mode(mode)

    def load_weather_histogram() -> dict[str, Any]:
        sources = _mode_sources(normalized_mode)
        if not sources:
            return {
                "histogram": {"temperature": [], "humidity": [], "wind_speed": []}
            }

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
                        SELECT
                            WIDTH_BUCKET({value_column}, 0, {max_val}, {bins}) AS bin_idx,
                            MIN({value_column})::DOUBLE PRECISION AS bin_min,
                            MAX({value_column})::DOUBLE PRECISION AS bin_max,
                            COUNT(*)::BIGINT AS event_count
                        FROM {table}
                        WHERE {value_column} IS NOT NULL
                        GROUP BY WIDTH_BUCKET({value_column}, 0, {max_val}, {bins})
                        """
                    ).format(
                        bins=sql.Literal(bins),
                        max_val=sql.Literal(
                            100
                            if key == "humidity_column"
                            else 130
                            if key == "temperature_column"
                            else 100
                        ),
                        value_column=source[key],
                        table=source["table"],
                    )
                )

            query = sql.SQL(
                """
                SELECT
                    bin_idx,
                    MIN(bin_min)::DOUBLE PRECISION AS bin_min,
                    MAX(bin_max)::DOUBLE PRECISION AS bin_max,
                    SUM(event_count)::BIGINT AS count
                FROM ({union_query}) AS weather_values
                GROUP BY bin_idx
                ORDER BY bin_idx
                """
            ).format(
                union_query=sql.SQL(" UNION ALL ").join(selects),
            )
            try:
                rows = fetch_all(query)
            except Exception:
                logger.exception(
                    "Weather histogram query failed for mode=%s metric=%s",
                    normalized_mode,
                    label,
                )
                result["histogram"][label] = []
                continue
            result["histogram"][label] = [
                {
                    "bin": f"{row['bin_min']:.0f}-{row['bin_max']:.0f}",
                    "count": row["count"],
                }
                for row in rows
            ]
        return result

    return cached_result(
        "weather_histogram",
        (normalized_mode,),
        ANALYTICS_CACHE_TTL_SECONDS,
        load_weather_histogram,
    )
