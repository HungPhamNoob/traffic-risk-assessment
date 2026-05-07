"""
Unit tests for Enricher module.
"""
import math
import os
import unittest
from unittest.mock import MagicMock, patch

from processing.flink.common.enricher import Enricher, EnrichmentConfig


class TestEnricher(unittest.TestCase):
    """Test cases for Enricher class."""

    def setUp(self):
        self.config = EnrichmentConfig()
        self.config.min_lat = 20.9
        self.config.max_lat = 21.1
        self.config.min_lon = 105.7
        self.config.max_lon = 106.0
        self.enricher = Enricher(config=self.config)

    def test_compute_grid_cell_id_valid(self):
        """Test grid cell computation for valid coordinates."""
        # Test with coordinates in Hanoi
        grid_id = self.enricher.compute_grid_cell_id(21.0285, 105.8542)
        self.assertIsNotNone(grid_id)
        self.assertTrue(grid_id.startswith("grid_"))

    def test_compute_grid_cell_id_outside_bounds(self):
        """Test grid cell computation for coordinates outside bounds."""
        # Coordinates far from Hanoi
        grid_id = self.enricher.compute_grid_cell_id(10.0, 106.0)
        self.assertIsNone(grid_id)

    def test_compute_grid_cell_id_edge_cases(self):
        """Test grid cell at boundary coordinates."""
        # At minimum bounds
        grid_id = self.enricher.compute_grid_cell_id(20.9, 105.7)
        self.assertIsNotNone(grid_id)

        # At maximum bounds
        grid_id = self.enricher.compute_grid_cell_id(21.1, 106.0)
        self.assertIsNotNone(grid_id)

    def test_enrich_time_features(self):
        """Test time feature extraction."""
        from datetime import datetime, timezone

        # Create a known timestamp (2024-01-15 14:30:00 UTC, Monday=0)
        timestamp = "2024-01-15T14:30:00+00:00"
        features = self.enricher.enrich_time_features(timestamp)

        self.assertEqual(features["hour_of_day"], 14)
        self.assertEqual(features["day_of_week"], 0)  # Monday
        self.assertFalse(features["is_rush_hour"])  # 2:30 PM is not rush hour
        self.assertEqual(features["season"], "winter")

    def test_enrich_time_features_rush_hour(self):
        """Test rush hour detection."""
        # Wednesday 8:30 AM = rush hour
        timestamp = "2024-01-17T08:30:00+00:00"
        features = self.enricher.enrich_time_features(timestamp)
        self.assertTrue(features["is_rush_hour"])

        # Saturday 8:30 AM = not rush hour
        timestamp = "2024-01-20T08:30:00+00:00"
        features = self.enricher.enrich_time_features(timestamp)
        self.assertFalse(features["is_rush_hour"])

    def test_enrich_time_features_season(self):
        """Test season detection."""
        test_cases = [
            ("2024-01-01", "winter"),
            ("2024-04-01", "spring"),
            ("2024-07-01", "summer"),
            ("2024-10-01", "autumn"),
        ]
        for date_str, expected_season in test_cases:
            timestamp = f"{date_str}T12:00:00+00:00"
            features = self.enricher.enrich_time_features(timestamp)
            self.assertEqual(features["season"], expected_season)

    @patch("processing.flink.common.enricher.redis.Redis")
    def test_enrich_road_attributes_redis(self, mock_redis_class):
        """Test road attribute enrichment from Redis."""
        mock_client = MagicMock()
        mock_client.hgetall.return_value = {
            "road_type": "highway",
            "speed_limit_kmh": "80",
            "num_lanes": "4",
            "has_traffic_signal": "true",
        }
        mock_redis_class.return_value = mock_client

        enricher = Enricher(config=self.config)
        attrs = enricher.enrich_road_attributes("seg_test_123")

        self.assertEqual(attrs["road_type"], "highway")
        self.assertEqual(attrs["speed_limit_kmh"], 80)
        self.assertEqual(attrs["num_lanes"], 4)
        self.assertTrue(attrs["has_traffic_signal"])

    def test_enrich_road_attributes_no_redis(self):
        """Test road attribute enrichment without Redis."""
        # Create enricher with no Redis
        self.enricher._redis_client = None
        attrs = self.enricher.enrich_road_attributes("seg_test_123")

        self.assertIsNone(attrs["road_type"])
        self.assertEqual(attrs["speed_limit_kmh"], 0)
        self.assertEqual(attrs["num_lanes"], 0)
        self.assertFalse(attrs["has_traffic_signal"])

    def test_enrich_weather(self):
        """Test weather enrichment (currently placeholder)."""
        weather = self.enricher.enrich_weather(21.0, 105.8)
        self.assertEqual(weather["temperature_c"], 0.0)
        self.assertEqual(weather["visibility_km"], 0.0)
        self.assertEqual(weather["precipitation_mm"], 0.0)
        self.assertIsNone(weather["weather_condition"])

    def test_enrich_valid_event(self):
        """Test full enrichment of a valid event."""
        event = {
            "event_id": "evt-123",
            "flow_segment_id": "seg_001",
            "latitude": 21.0285,
            "longitude": 105.8542,
            "speed": 50.0,
            "timestamp": "2024-01-15T08:30:00+00:00",
        }
        enriched = self.enricher.enrich(event)
        self.assertIsNotNone(enriched)
        self.assertIn("grid_cell_id", enriched)
        self.assertIn("hour_of_day", enriched)
        self.assertIn("is_rush_hour", enriched)
        self.assertIn("season", enriched)
        self.assertIn("processed_at", enriched)

    def test_enrich_invalid_coordinates(self):
        """Test enrichment with invalid coordinates."""
        event = {
            "event_id": "evt-456",
            "flow_segment_id": "seg_002",
            "latitude": 10.0,  # Outside bounds
            "longitude": 106.0,
            "speed": 30.0,
            "timestamp": "2024-01-15T08:30:00+00:00",
        }
        enriched = self.enricher.enrich(event)
        self.assertIsNone(enriched)


if __name__ == "__main__":
    unittest.main()
