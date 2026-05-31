from datetime import date
import sys
from pathlib import Path

BACKEND_PATH = Path(__file__).resolve().parents[2] / "dashboard" / "backend"
if str(BACKEND_PATH) not in sys.path:
    sys.path.insert(0, str(BACKEND_PATH))

from app.services import analytics_service, pipeline_service  # noqa: E402


def test_timeseries_metric_count_is_returned(monkeypatch):
    def fake_fetch_all(query, params=None):
        return [
            {
                "time": date(2024, 1, 1),
                "value": 12.0,
                "avg_risk_score": 0.5,
                "accident_count": 12,
                "high_risk_count": 3,
            }
        ]

    monkeypatch.setattr(analytics_service, "fetch_all", fake_fetch_all)

    result = analytics_service.timeseries("day", "count", None, None)

    assert result["metric"] == "count"
    assert result["series"][0]["time"] == "2024-01-01"
    assert result["series"][0]["value"] == 12.0


def test_analytics_empty_sources_return_empty_payload(monkeypatch):
    monkeypatch.setattr(analytics_service, "_mode_sources", lambda mode=None: [])

    severity = analytics_service.severity_distribution("full")
    risk_by_hour = analytics_service.risk_by_hour("full")
    weather = analytics_service.weather_histogram("full")

    assert severity == {"distribution": []}
    assert risk_by_hour == {"data": []}
    assert weather == {
        "histogram": {"temperature": [], "humidity": [], "wind_speed": []}
    }


def test_throughput_handles_missing_prediction_table(monkeypatch):
    monkeypatch.setattr(pipeline_service, "_table_columns", lambda: set())

    result = pipeline_service.throughput("5m")

    assert result["status"] == "unavailable"
    assert result["event_count"] == 0
    assert result["events_per_minute"] == 0.0


def test_latency_handles_missing_latency_columns(monkeypatch):
    monkeypatch.setattr(pipeline_service, "_table_columns", lambda: {"event_id"})

    result = pipeline_service.latency("p95")

    assert result["status"] == "unavailable"
    assert result["columns"] == []
