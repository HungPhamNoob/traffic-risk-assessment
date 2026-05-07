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
            "speed": 50.0,
            "road_type": "highway",
            "speed_limit_kmh": 80,
            "num_lanes": 4,
            "has_traffic_signal": True,
            "temperature_c": 25.0,
            "visibility_km": 10.0,
            "precipitation_mm": 0.0,
            "is_rush_hour": True,
            "hour_of_day": 8,
            "day_of_week": 2,
            "season": "summer",
        }
        features = self.client._build_feature_vector(event)
        self.assertEqual(features["speed"], 50.0)
        self.assertEqual(features["road_type"], "highway")
        self.assertTrue(features["has_traffic_signal"])
        self.assertEqual(features["season"], "summer")

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
        self.assertEqual(result["risk_score"], -1)
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
                "inference_status", "inference_error", "scored_at"
            ]
            for field in required_fields:
                self.assertIn(field, result)


if __name__ == "__main__":
    unittest.main()
