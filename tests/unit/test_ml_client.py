"""Unit tests for the shared US feature contract used by Flink, Spark, and H2O."""

from processing.feature_engineering import build_features


def test_build_features_maps_us_severity_to_true_severity():
    """The US raw Severity label must remain available as true_severity for evaluation."""
    raw_row = {
        "ID": "A-1",
        "Severity": "4",
        "Start_Time": "2020-05-08 08:30:00",
        "Start_Lat": "39.865147",
        "Start_Lng": "-84.058723",
        "Weather_Condition": "Heavy Rain",
        "Temperature(F)": "36.9",
        "Humidity(%)": "91",
        "Wind_Speed(mph)": "4.6",
        "Visibility(mi)": "2",
        "Street": "I-75 N",
        "Junction": "False",
        "Traffic_Signal": "True",
        "Crossing": "False",
        "Roundabout": "False",
        "Stop": "False",
        "Station": "False",
        "Railway": "False",
        "Sunrise_Sunset": "Night",
    }

    features = build_features(raw_row)

    assert features is not None
    assert features["event_id"] == "A-1"
    assert features["event_year"] == 2020
    assert features["true_severity"] == 4
    assert features["weather_code"] == 1
    assert features["road_type_code"] == 1
    assert features["has_traffic_signal"] == 1
    assert features["is_night"] == 1
