"""
Unit tests for ML client module.
"""
import json
import unittest
from unittest.mock import MagicMock, patch

import requests

from processing.flink.common.ml_client import MLClient, MLClientConfig


class TestMLClient(unittest.TestCase):
    """Test cases for MLClient class."""

    def setUp(self):
        self.config = MLClientConfig()
        self.config.endpoint = "http://localhost:5000/invocations"
        self.config.timeout_seconds = 5
        self.client = MLClient(config=self.config)

    def test_build_feature_vector(self):
        """Test feature vector construction from enriched event."""
        event = {
            "lat": 40.73,
            "lon": -74.001,
            "hour": 8,
            "day_of_week": 2,
            "is_weekend": 0,
            "is_rush_hour": 1,
            "weather_code": 1,
            "temperature_f": 72.0,
            "humidity": 65.0,
            "wind_speed_mph": 8.0,
            "visibility_mi": 9.5,
            "road_type_code": 1,
            "is_junction": 0,
            "has_traffic_signal": 1,
            "is_crossing": 0,
            "is_roundabout": 0,
            "is_stop": 0,
            "is_station": 0,
            "is_railway": 0,
            "is_night": 0,
        }
        features = self.client._build_feature_vector(event)
        self.assertEqual(features["lat"], 40.73)
        self.assertEqual(features["lon"], -74.001)
        self.assertEqual(features["weather_code"], 1)
        self.assertEqual(features["temperature_f"], 72.0)
        self.assertEqual(features["road_type_code"], 1)
        self.assertEqual(features["has_traffic_signal"], 1)
        self.assertNotIn("true_severity", features)

    @patch("processing.flink.common.ml_client.requests.Session")
    def test_predict_success(self, mock_session_class):
        """Test successful prediction."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [0.75]  # Risk score
        mock_session.post.return_value = mock_response

        client = MLClient(config=self.config)
        event = {
            "event_id": "evt-123",
            "grid_cell_id": "grid_0_0",
            "event_timestamp": "2024-01-15T08:30:00+00:00",
            "speed": 50.0,
        }
        result = client.predict(event)

        self.assertEqual(result["inference_status"], "SUCCESS")
        self.assertEqual(result["risk_score"], 0.75)
        self.assertIsNotNone(result["risk_level"])
        self.assertEqual(result["predicted_severity"], 4)
        self.assertEqual(result["model_status"], "SUCCESS")
        self.assertIn("prediction_timestamp", result)
        self.assertIn("inference_latency_ms", result)
        self.assertIn("scored_at", result)

    @patch("processing.flink.common.ml_client.requests.Session")
    def test_predict_http_error(self, mock_session_class):
        """Test prediction with HTTP error."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_session.post.return_value = mock_response

        client = MLClient(config=self.config)
        event = {"event_id": "evt-456", "grid_cell_id": "grid_1_1"}
        result = client.predict(event)

        self.assertEqual(result["inference_status"], "FAILED")
        self.assertEqual(result["model_status"], "FAILED")
        self.assertEqual(result["risk_score"], -1)
        self.assertEqual(result["predicted_severity"], 0)
        self.assertIsNotNone(result["inference_error"])

    @patch("processing.flink.common.ml_client.requests.Session")
    def test_predict_timeout(self, mock_session_class):
        """Test prediction timeout."""
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_session.post.side_effect = requests.Timeout("Connection timed out")

        client = MLClient(config=self.config)
        event = {"event_id": "evt-789", "grid_cell_id": "grid_2_2"}
        result = client.predict(event)

        self.assertEqual(result["inference_status"], "FAILED")
        self.assertEqual(result["risk_score"], -1)
        self.assertIn("Timeout", result["inference_error"])

    def test_compute_risk_level(self):
        """Test risk level computation from risk score."""
        test_cases = [
            (-1.0, 0),  # Error case
            (0.1, 1),  # Very low
            (0.3, 2),  # Low
            (0.5, 3),  # Medium
            (0.7, 4),  # High
            (0.9, 5),  # Very high
        ]
        for score, expected_level in test_cases:
            level = self.client._compute_risk_level(score)
            self.assertEqual(level, expected_level)

    def test_predict_result_schema(self):
        """Test that prediction result has all required fields."""
        with patch("processing.flink.common.ml_client.requests.Session") as mock_session_class:
            mock_session = MagicMock()
            mock_session_class.return_value = mock_session
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = [0.5]
            mock_session.post.return_value = mock_response

            client = MLClient(config=self.config)
            event = {"event_id": "evt-schema", "grid_cell_id": "grid_schema"}
            result = client.predict(event)

            required_fields = [
                "event_id", "grid_cell_id", "event_timestamp",
                "risk_score", "risk_level", "is_high_risk",
                "model_name", "model_version",
                "inference_status", "model_status", "inference_error",
                "predicted_severity", "prediction_timestamp",
                "inference_latency_ms", "end_to_end_latency_ms", "scored_at"
            ]
            for field in required_fields:
                self.assertIn(field, result)


if __name__ == "__main__":
    unittest.main()
