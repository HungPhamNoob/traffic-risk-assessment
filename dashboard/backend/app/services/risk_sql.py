"""SQL helpers for normalized risk-score expressions."""

from __future__ import annotations

from psycopg2 import sql


def _identifier_or_expression(
    value: str | sql.Composable | None,
) -> sql.Composable:
    if value is None:
        return sql.SQL("NULL")
    if isinstance(value, str):
        return sql.Identifier(value)
    return value


def _normalized_formula(
    severity_expr: sql.Composable,
    delay_expr: sql.Composable,
    length_expr: sql.Composable,
    is_night_expr: sql.Composable,
    is_weekend_expr: sql.Composable,
    road_type_expr: sql.Composable,
    weather_expr: sql.Composable,
) -> sql.Composable:
    return sql.SQL(
        """
        LEAST(
            1.0,
            GREATEST(
                0.0,
                CASE
                    WHEN {severity} = 1 THEN 0.00
                    WHEN {severity} = 2 THEN 0.25
                    WHEN {severity} = 3 THEN 0.55
                    WHEN {severity} = 4 THEN 0.85
                    ELSE 0.0
                END
                + CASE
                    WHEN COALESCE({delay}, 0) > 0
                    THEN LEAST((COALESCE({delay}, 0)::DOUBLE PRECISION / 60.0) * 0.01, 0.10)
                    ELSE 0.0
                END
                + CASE
                    WHEN COALESCE({length}, 0) > 0
                    THEN LEAST((COALESCE({length}, 0)::DOUBLE PRECISION / 100.0) * 0.005, 0.08)
                    ELSE 0.0
                END
                + CASE WHEN COALESCE({is_night}, 0) = 1 THEN 0.03 ELSE 0.0 END
                + CASE WHEN COALESCE({is_weekend}, 0) = 1 THEN 0.02 ELSE 0.0 END
                + CASE WHEN COALESCE({road_type}, 0) = 1 THEN 0.03 ELSE 0.0 END
                + CASE WHEN COALESCE({weather}, 0) IN (1, 2, 4) THEN 0.03 ELSE 0.0 END
                - CASE
                    WHEN COALESCE({road_type}, 0) IN (3, 4, 6)
                     AND COALESCE({severity}, 0) <= 2
                    THEN 0.02
                    ELSE 0.0
                END
            )
        )::DOUBLE PRECISION
        """
    ).format(
        severity=severity_expr,
        delay=delay_expr,
        length=length_expr,
        is_night=is_night_expr,
        is_weekend=is_weekend_expr,
        road_type=road_type_expr,
        weather=weather_expr,
    )


def effective_risk_score_expr(
    *,
    stored_risk: str | sql.Composable = "risk_score",
    severity: str | sql.Composable,
    delay_seconds: str | sql.Composable | None = None,
    length_meters: str | sql.Composable | None = None,
    is_night: str | sql.Composable = "is_night",
    is_weekend: str | sql.Composable = "is_weekend",
    road_type_code: str | sql.Composable = "road_type_code",
    weather_code: str | sql.Composable = "weather_code",
) -> sql.Composable:
    """Return a SQL expression that fixes invalid stored risk scores on read."""
    stored_risk_expr = _identifier_or_expression(stored_risk)
    severity_expr = _identifier_or_expression(severity)
    formula = _normalized_formula(
        severity_expr=severity_expr,
        delay_expr=_identifier_or_expression(delay_seconds),
        length_expr=_identifier_or_expression(length_meters),
        is_night_expr=_identifier_or_expression(is_night),
        is_weekend_expr=_identifier_or_expression(is_weekend),
        road_type_expr=_identifier_or_expression(road_type_code),
        weather_expr=_identifier_or_expression(weather_code),
    )
    return sql.SQL(
        """
        CASE
            WHEN {stored_risk} BETWEEN 0.0 AND 1.0 THEN {stored_risk}::DOUBLE PRECISION
            WHEN {severity} BETWEEN 1 AND 4 THEN {formula}
            ELSE NULL
        END
        """
    ).format(
        stored_risk=stored_risk_expr,
        severity=severity_expr,
        formula=formula,
    )


def effective_us_risk_score_expr() -> sql.Composable:
    """Return the normalized US replay risk-score expression."""
    severity_expr = sql.SQL("COALESCE({predicted}, {actual})").format(
        predicted=sql.Identifier("predicted_severity"),
        actual=sql.Identifier("true_severity"),
    )
    return effective_risk_score_expr(severity=severity_expr)


def effective_tomtom_risk_score_expr() -> sql.Composable:
    """Return the normalized TomTom live risk-score expression."""
    return effective_risk_score_expr(
        severity="severity",
        delay_seconds="delay_seconds",
        length_meters="length_meters",
    )
