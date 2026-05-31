"""MLflow model serving client used by the scenario simulator."""

from typing import Any

import requests

from app.core.config import get_settings
from app.schemas.scenario import ScenarioInput


MODEL_FEATURE_COLUMNS = [
    "lat",
    "lon",
    "hour",
    "day_of_week",
    "is_weekend",
    "is_rush_hour",
    "weather_code",
    "temperature_f",
    "humidity",
    "wind_speed_mph",
    "visibility_mi",
    "road_type_code",
    "is_junction",
    "has_traffic_signal",
    "is_crossing",
    "is_roundabout",
    "is_stop",
    "is_station",
    "is_railway",
    "is_night",
]


def risk_level(score: float) -> str:
    """Map a numeric risk score to the UI risk label."""
    if score >= 0.7:
        return "high"
    if score >= 0.4:
        return "medium"
    return "low"


def compute_risk_score(
    severity: int,
    delay_seconds: float | None = None,
    length_meters: float | None = None,
    is_night: int = 0,
    is_weekend: int = 0,
    road_type_code: int = 0,
    weather_code: int = 0,
) -> float:
    """Compute the unified 0.0-1.0 risk score used across replay and live flows."""
    severity = max(1, min(4, int(severity)))
    severity_map = {1: 0.00, 2: 0.25, 3: 0.55, 4: 0.85}
    base_score = severity_map.get(severity, 0.0)
    adjustment = 0.0

    if delay_seconds is not None and delay_seconds > 0:
        adjustment += min((delay_seconds / 60.0) * 0.01, 0.10)
    if length_meters is not None and length_meters > 0:
        adjustment += min((length_meters / 100.0) * 0.005, 0.08)
    if is_night == 1:
        adjustment += 0.03
    if is_weekend == 1:
        adjustment += 0.02
    if road_type_code == 1:
        adjustment += 0.03
    if weather_code in {1, 2, 4}:
        adjustment += 0.03
    if road_type_code in {3, 4, 6} and severity <= 2:
        adjustment -= 0.02

    return round(max(0.0, min(1.0, base_score + adjustment)), 4)


def normalize_prediction(raw_prediction: Any) -> tuple[int, float]:
    """Normalize MLflow prediction output into severity and risk_score."""
    if isinstance(raw_prediction, dict):
        severity = raw_prediction.get("predicted_severity")
        severity = (
            severity
            or raw_prediction.get("prediction")
            or raw_prediction.get("predict")
        )
        risk = raw_prediction.get("risk_score") or raw_prediction.get("probability")
    else:
        severity = raw_prediction
        risk = None

    predicted_severity = int(float(severity)) if severity is not None else 2

    if isinstance(risk, list) and risk:
        risk_score = max(float(value) for value in risk)
    elif risk is not None:
        risk_score = float(risk)
    else:
        risk_score = compute_risk_score(predicted_severity)

    return predicted_severity, risk_score


def heuristic_prediction(scenario: ScenarioInput) -> tuple[int, float]:
    """
    Produce a deterministic fallback prediction when MLflow serving is unavailable.

    The fallback is intentionally simple and transparent. It is used only for
    local dashboard/API demonstrations when the registered MLflow model server
    has not been started yet. Production cloud mode should use MLflow serving.
    """
    score = 0.18
    score += 0.20 if scenario.is_rush_hour else 0.0
    score += 0.12 if scenario.is_night else 0.0
    score += 0.10 if scenario.is_junction else 0.0
    score += 0.08 if scenario.is_crossing else 0.0
    score += 0.08 if scenario.has_traffic_signal else 0.0
    score += 0.10 if scenario.weather_code in {1, 2, 3, 4, 6} else 0.0
    score += max(0.0, (10.0 - scenario.visibility_mi) / 10.0) * 0.18
    score += min(max(scenario.wind_speed_mph, 0.0), 100.0) / 100.0 * 0.10
    score = max(0.0, min(1.0, score))

    if score >= 0.75:
        severity = 4
    elif score >= 0.50:
        severity = 3
    elif score >= 0.25:
        severity = 2
    else:
        severity = 1
    risk_score = compute_risk_score(
        severity=severity,
        is_night=scenario.is_night,
        is_weekend=scenario.is_weekend,
        road_type_code=scenario.road_type_code,
        weather_code=scenario.weather_code,
    )
    return severity, risk_score


def predict_scenario(scenario: ScenarioInput) -> dict[str, Any]:
    """Send one scenario to MLflow serving and return dashboard-ready prediction data."""
    settings = get_settings()
    scenario_dict = scenario.model_dump()
    row = [scenario_dict[column] for column in MODEL_FEATURE_COLUMNS]

    payload = {
        "dataframe_split": {
            "columns": MODEL_FEATURE_COLUMNS,
            "data": [row],
        }
    }

    model_status = "ok"
    try:
        response = requests.post(
            settings.mlflow_serving_endpoint,
            json=payload,
            timeout=10,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        predictions = response.json().get("predictions", [])
        predicted_severity, risk_score = normalize_prediction(
            predictions[0] if predictions else None
        )
    except requests.RequestException:
        predicted_severity, risk_score = heuristic_prediction(scenario)
        model_status = "heuristic_fallback"

    unified_risk_score = compute_risk_score(
        severity=predicted_severity,
        is_night=scenario.is_night,
        is_weekend=scenario.is_weekend,
        road_type_code=scenario.road_type_code,
        weather_code=scenario.weather_code,
    )
    return {
        "predicted_severity": predicted_severity,
        "risk_score": unified_risk_score,
        "risk_level": risk_level(unified_risk_score),
        "model_name": settings.model_name,
        "model_version": settings.model_version or "latest",
        "model_status": model_status,
    }
