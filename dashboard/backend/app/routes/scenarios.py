"""Scenario simulator endpoints."""

from fastapi import APIRouter

from app.schemas.scenario import ScenarioCompareRequest, ScenarioInput
from app.services.mlflow_service import predict_scenario

router = APIRouter()


@router.post("/predict")
def predict_single_scenario(scenario: ScenarioInput) -> dict:
    """Predict severity and risk for one user-defined what-if scenario."""
    return predict_scenario(scenario)


@router.post("/compare")
def compare_scenarios(request: ScenarioCompareRequest) -> dict:
    """Compare baseline and modified scenarios with the same model."""
    baseline = predict_scenario(request.baseline)
    scenario = predict_scenario(request.scenario)
    baseline_score = float(baseline["risk_score"])
    scenario_score = float(scenario["risk_score"])
    score_change = scenario_score - baseline_score
    percent_change = (score_change / baseline_score * 100.0) if baseline_score else 0.0
    return {
        "baseline": baseline,
        "scenario": scenario,
        "delta": {
            "risk_score_change": round(score_change, 4),
            "risk_percent_change": round(percent_change, 2),
            "severity_change": scenario["predicted_severity"]
            - baseline["predicted_severity"],
        },
    }
