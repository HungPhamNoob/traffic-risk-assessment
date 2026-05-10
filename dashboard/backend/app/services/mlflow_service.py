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
        risk_score = max(0.0, min(1.0, (predicted_severity - 1.0) / 3.0))

    return predicted_severity, risk_score


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

    return {
        "predicted_severity": predicted_severity,
        "risk_score": round(risk_score, 4),
        "risk_level": risk_level(risk_score),
        "model_name": settings.model_name,
        "model_version": settings.model_version or "latest",
    }
