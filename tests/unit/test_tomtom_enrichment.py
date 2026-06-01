"""Unit tests for TomTom streaming enrichment."""

from processing.feature_engineering import build_features
from processing.streaming_enrichment import (
    enrich_tomtom_event,
    fetch_open_meteo_weather,
    normalize_tomtom_severity,
)


def test_tomtom_delay_and_icon_category_map_to_model_severity():
    """TomTom incident signals should produce the 1-4 label expected by H2O."""
    assert normalize_tomtom_severity(4, 6) == 4
    assert normalize_tomtom_severity(2, 6) == 2
    assert normalize_tomtom_severity(None, 1) == 3
    assert normalize_tomtom_severity(1, 8) == 4


def test_tomtom_enrichment_builds_feature_engineering_input(monkeypatch):
    """TomTom raw events should be enriched before the shared feature builder."""
    monkeypatch.setattr(
        "processing.streaming_enrichment.fetch_open_meteo_weather",
        lambda lat, lon, timestamp=None: {
            "Weather_Condition": "Light Rain",
            "Temperature(F)": 61.0,
            "Humidity(%)": 80,
            "Wind_Speed(mph)": 5.0,
            "weather_observed_at": "2026-05-12T06:00",
        },
    )
    raw_row = {
        "source": "tomtom",
        "event_id": "tomtom-TTI-1",
        "latitude": 40.73,
        "longitude": -74.0,
        "timestamp": "2026-05-12T06:00:00Z",
        "delay_magnitude": 3,
        "icon_category": 6,
        "from_road": "I-95 N",
    }

    enriched = enrich_tomtom_event(raw_row)
    features = build_features(enriched)

    assert enriched["ID"] == "tomtom-TTI-1"
    assert enriched["Severity"] == 3
    assert features is not None
    assert features["event_id"] == "tomtom-TTI-1"
    assert features["true_severity"] == 3
    assert features["weather_code"] == 1
    assert features["road_type_code"] == 1


def test_open_meteo_weather_uses_tomtom_event_time(monkeypatch):
    """Weather lookup should request hourly weather for the TomTom event date."""
    captured = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "hourly": {
                    "time": [
                        "2024-06-15T05:00",
                        "2024-06-15T06:00",
                        "2024-06-15T07:00",
                    ],
                    "temperature_2m": [55.0, 61.0, 62.0],
                    "relative_humidity_2m": [70, 80, 82],
                    "weather_code": [0, 61, 3],
                    "wind_speed_10m": [3.0, 5.0, 6.0],
                }
            }

    def fake_get(url, params, timeout):
        captured["url"] = url
        captured["params"] = params
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr("processing.streaming_enrichment.requests.get", fake_get)

    weather = fetch_open_meteo_weather(40.73, -74.0, "2024-06-15T06:25:00Z")

    assert captured["url"] == "https://archive-api.open-meteo.com/v1/archive"
    assert captured["params"]["latitude"] == 40.73
    assert captured["params"]["longitude"] == -74.0
    assert captured["params"]["start_date"] == "2024-06-15"
    assert captured["params"]["end_date"] == "2024-06-15"
    assert captured["params"]["timezone"] == "UTC"
    assert "hourly" in captured["params"]
    assert "current" not in captured["params"]
    assert weather == {
        "Weather_Condition": "Rain",
        "Temperature(F)": 61.0,
        "Humidity(%)": 80,
        "Wind_Speed(mph)": 5.0,
        "weather_observed_at": "2024-06-15T06:00",
    }


def test_open_meteo_weather_caches_nearby_requests(monkeypatch):
    """Nearby events in the same hour should reuse the cached weather response."""
    import processing.streaming_enrichment as enrichment

    enrichment._WEATHER_CACHE.clear()
    enrichment._OPEN_METEO_BACKOFF_UNTIL = 0.0
    calls = {"count": 0}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "hourly": {
                    "time": ["2024-06-15T06:00"],
                    "temperature_2m": [61.0],
                    "relative_humidity_2m": [80],
                    "weather_code": [61],
                    "wind_speed_10m": [5.0],
                }
            }

    def fake_get(url, params, timeout):
        calls["count"] += 1
        return FakeResponse()

    monkeypatch.setattr("processing.streaming_enrichment.requests.get", fake_get)

    first = fetch_open_meteo_weather(40.731, -74.001, "2024-06-15T06:25:00Z")
    second = fetch_open_meteo_weather(40.732, -74.002, "2024-06-15T06:40:00Z")

    assert calls["count"] == 1
    assert first == second


def test_open_meteo_weather_enters_backoff_after_rate_limit(monkeypatch):
    """HTTP 429 should suppress repeated requests during the cooldown window."""
    import processing.streaming_enrichment as enrichment
    import requests

    enrichment._WEATHER_CACHE.clear()
    enrichment._OPEN_METEO_BACKOFF_UNTIL = 0.0
    calls = {"count": 0}

    class FakeResponse:
        status_code = 429

        def raise_for_status(self):
            raise requests.HTTPError("429 Too Many Requests", response=self)

    def fake_get(url, params, timeout):
        calls["count"] += 1
        return FakeResponse()

    monkeypatch.setattr("processing.streaming_enrichment.requests.get", fake_get)

    first = fetch_open_meteo_weather(40.73, -74.0, "2024-06-15T06:25:00Z")
    second = fetch_open_meteo_weather(40.73, -74.0, "2024-06-15T06:26:00Z")

    assert first == {}
    assert second == {}
    assert calls["count"] == 1
