from shared.risk_scoring import (
    compute_unified_risk_score,
    infer_severity_from_prediction,
)


def test_compute_unified_risk_score_for_us_incident():
    score = compute_unified_risk_score(
        severity=4,
        is_night=1,
        is_weekend=0,
        road_type_code=1,
        weather_code=1,
    )

    assert score == 0.94


def test_compute_unified_risk_score_for_tomtom_incident_clips_at_one():
    score = compute_unified_risk_score(
        severity=4,
        delay_seconds=320,
        length_meters=850,
        is_night=1,
        is_weekend=0,
        road_type_code=1,
        weather_code=0,
    )

    assert score == 1.0


def test_infer_severity_from_prediction_prefers_class_label_over_probability_score():
    severity = infer_severity_from_prediction(
        {
            "predict": 3,
            "probability": 0.01,
            "risk_score": -4.0,
            "p1": 0.1,
            "p2": 0.2,
            "p3": 0.6,
            "p4": 0.1,
        }
    )

    assert severity == 3


def test_infer_severity_from_prediction_can_recover_from_probability_columns_only():
    severity = infer_severity_from_prediction(
        {"p1": 0.05, "p2": 0.65, "p3": 0.2, "p4": 0.1}
    )

    assert severity == 2
