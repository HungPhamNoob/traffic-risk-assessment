"""Unified risk score helpers shared by streaming and dashboard services."""

from __future__ import annotations

from typing import Any


SEVERITY_BASE_SCORES = {1: 0.00, 2: 0.25, 3: 0.55, 4: 0.85}
HIGHWAY_ROAD_TYPE_CODE = 1
LOW_RISK_ROAD_TYPE_CODES = {3, 4, 6}
HIGH_RISK_WEATHER_CODES = {1, 2, 4}


def to_float(value: Any) -> float | None:
    """Convert a loosely typed value to float when possible."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> int | None:
    """Convert a loosely typed value to int when possible."""
    try:
        if value is None:
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def clamp_severity(severity: Any) -> int | None:
    """Clamp severity into the supported 1-4 range."""
    severity_int = to_int(severity)
    if severity_int is None:
        return None
    return max(1, min(4, severity_int))


def infer_severity_from_prediction(prediction: Any) -> int | None:
    """
    Recover a severity class from common MLflow prediction payloads.

    Probabilities are used only to recover the predicted class when the serving
    response omits an explicit severity label. They are never used directly as
    the risk score.
    """
    if not isinstance(prediction, dict):
        return clamp_severity(prediction)

    direct_severity = (
        prediction.get("predicted_severity")
        or prediction.get("prediction")
        or prediction.get("predict")
        or prediction.get("severity")
    )
    severity = clamp_severity(direct_severity)
    if severity is not None:
        return severity

    probabilities: list[tuple[int, float]] = []
    for severity_level in range(1, 5):
        probability = to_float(prediction.get(f"p{severity_level}"))
        if probability is not None:
            probabilities.append((severity_level, probability))

    if not probabilities:
        return None
    return max(probabilities, key=lambda item: item[1])[0]


def compute_unified_risk_score(
    severity: Any,
    delay_seconds: Any = None,
    length_meters: Any = None,
    is_night: Any = 0,
    is_weekend: Any = 0,
    road_type_code: Any = 0,
    weather_code: Any = 0,
) -> float | None:
    """
    Compute the unified continuous risk score for replay and live incidents.

    The score is severity-first, then adjusted with bounded contextual bonuses
    and maluses so the final value stays in the inclusive 0.0-1.0 range.
    """
    severity_int = clamp_severity(severity)
    if severity_int is None:
        return None

    base_score = SEVERITY_BASE_SCORES.get(severity_int, 0.0)
    adjustment = 0.0

    delay_seconds_value = to_float(delay_seconds)
    if delay_seconds_value is not None and delay_seconds_value > 0:
        adjustment += min((delay_seconds_value / 60.0) * 0.01, 0.10)

    length_meters_value = to_float(length_meters)
    if length_meters_value is not None and length_meters_value > 0:
        adjustment += min((length_meters_value / 100.0) * 0.005, 0.08)

    is_night_int = 1 if to_int(is_night) == 1 else 0
    is_weekend_int = 1 if to_int(is_weekend) == 1 else 0
    road_type_int = to_int(road_type_code) or 0
    weather_int = to_int(weather_code) or 0

    if is_night_int == 1:
        adjustment += 0.03
    if is_weekend_int == 1:
        adjustment += 0.02
    if road_type_int == HIGHWAY_ROAD_TYPE_CODE:
        adjustment += 0.03
    if weather_int in HIGH_RISK_WEATHER_CODES:
        adjustment += 0.03
    if road_type_int in LOW_RISK_ROAD_TYPE_CODES and severity_int <= 2:
        adjustment -= 0.02

    return round(max(0.0, min(1.0, base_score + adjustment)), 4)
