"""
Unit tests for Enricher module.
"""
import math
import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

if "redis" not in sys.modules:
    redis_module = types.ModuleType("redis")
    redis_module.Redis = MagicMock
    sys.modules["redis"] = redis_module

from processing.flink.common.enricher import Enricher, EnrichmentConfig


class TestEnricher(unittest.TestCase):
    """Test cases for Enricher class."""

    def setUp(self):
        self.config = EnrichmentConfig()
        self.config.min_lat = 40.4
        self.config.max_lat = 41.0
        self.config.min_lon = -74.3
        self.config.max_lon = -73.6
        self.config.weather_enabled = False
        self.enricher = Enricher(config=self.config)
        self.enricher._redis_client = None

    def test_compute_grid_cell_id_valid(self):
        """Test grid cell computation for valid coordinates."""
        # Test with coordinates in New York City
        grid_id = self.enricher.compute_grid_cell_id(40.73, -74.001)
        self.assertIsNotNone(grid_id)
        self.assertTrue(grid_id.startswith("grid_"))

    def test_compute_grid_cell_id_outside_bounds(self):
        """Test grid cell computation for coordinates outside bounds."""
        # Coordinates far from configured bounds
        grid_id = self.enricher.compute_grid_cell_id(10.0, 106.0)
        self.assertIsNone(grid_id)

    def test_compute_grid_cell_id_edge_cases(self):
        """Test grid cell at boundary coordinates."""
        # At minimum bounds
        grid_id = self.enricher.compute_grid_cell_id(40.4, -74.3)
        self.assertIsNotNone(grid_id)

        # At maximum bounds
        grid_id = self.enricher.compute_grid_cell_id(41.0, -73.6)
        self.assertIsNotNone(grid_id)

    def test_enrich_time_features(self):
        """Test time feature extraction."""
        from datetime import datetime, timezone

        # Create a known timestamp (2024-01-15 14:30:00 UTC, Monday=0)
        timestamp = "2024-01-15T14:30:00+00:00"
        features = self.enricher.enrich_time_features(timestamp)

        self.assertEqual(features["hour_of_day"], 14)
        self.assertEqual(features["day_of_week"], 2)  # Spark-style Monday
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

        # Wednesday 4:30 PM = rush hour per training contract
        timestamp = "2024-01-17T16:30:00+00:00"
        features = self.enricher.enrich_time_features(timestamp)
        self.assertTrue(features["is_rush_hour"])

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
        mock_client.hgetall.side_effect = lambda key: {
            "road:grid:grid_test": {
                "road_type": "route",
                "road_type_code": "2",
                "speed_limit_kmh": "80",
                "num_lanes": "4",
                "has_traffic_signal": "true",
            }
        }.get(key, {})
        mock_redis_class.return_value = mock_client

        enricher = Enricher(config=self.config)
        attrs = enricher.enrich_road_attributes("seg_test_123", grid_cell_id="grid_test")

        self.assertEqual(attrs["road_type"], "route")
        self.assertEqual(attrs["speed_limit_kmh"], 80)
        self.assertEqual(attrs["num_lanes"], 4)
        self.assertEqual(attrs["has_traffic_signal"], 1)
        self.assertEqual(attrs["road_type_code"], 2)

    def test_enrich_road_attributes_fallback_to_flow_segment(self):
        """Test legacy flow_segment_id lookup remains as a fallback."""
        mock_client = MagicMock()
        mock_client.hgetall.side_effect = lambda key: {
            "road:seg_test_123": {
                "road_type": "street",
                "road_type_code": "3",
                "has_traffic_signal": "0",
            }
        }.get(key, {})
        self.enricher._redis_client = mock_client

        attrs = self.enricher.enrich_road_attributes("seg_test_123", grid_cell_id="grid_missing")

        self.assertEqual(attrs["road_type"], "street")
        self.assertEqual(attrs["road_type_code"], 3)

    def test_enrich_road_attributes_no_redis(self):
        """Test road attribute enrichment without Redis."""
        # Create enricher with no Redis
        self.enricher._redis_client = None
        attrs = self.enricher.enrich_road_attributes("seg_test_123")

        self.assertEqual(attrs["road_type"], "unknown")
        self.assertEqual(attrs["road_type_code"], 0)
        self.assertEqual(attrs["speed_limit_kmh"], 0)
        self.assertEqual(attrs["num_lanes"], 0)
        self.assertEqual(attrs["has_traffic_signal"], 0)

    def test_enrich_weather(self):
        """Test weather enrichment fallback."""
        self.enricher.config.weather_enabled = False
        weather = self.enricher.enrich_weather(21.0, 105.8)
        self.assertEqual(weather["temperature_c"], 10.0)
        self.assertEqual(weather["temperature_f"], 50.0)
        self.assertEqual(weather["humidity"], 50.0)
        self.assertEqual(weather["visibility_km"], 16.09344)
        self.assertEqual(weather["visibility_mi"], 10.0)
        self.assertEqual(weather["precipitation_mm"], 0.0)
        self.assertEqual(weather["weather_condition"], "unknown")
        self.assertEqual(weather["weather_code"], 0)

    def test_enrich_valid_event(self):
        """Test full enrichment of a valid event."""
        event = {
            "event_id": "evt-123",
            "flow_segment_id": "seg_001",
            "latitude": 40.73,
            "longitude": -74.001,
            "speed": 50.0,
            "timestamp": "2024-01-15T08:30:00+00:00",
        }
        enriched = self.enricher.enrich(event)
        self.assertIsNotNone(enriched)
        self.assertIn("grid_cell_id", enriched)
        self.assertIn("geom", enriched)
        self.assertIn("event_timestamp", enriched)
        self.assertIn("event_year", enriched)
        self.assertIn("weather_code", enriched)
        self.assertEqual(enriched["has_traffic_signal"], 0)
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
