"""
Unit tests for PostGIS prediction row mapping.
"""
import unittest

from processing.flink.sink_to_postgis import PREDICTION_COLUMNS, build_prediction_row


class TestPostGISSink(unittest.TestCase):
    def test_build_prediction_row_contains_mapping_columns(self):
        event = {
            "event_id": "evt-1",
            "latitude": 40.73,
            "longitude": -74.001,
            "grid_cell_id": "grid_1_2",
            "risk_score": 0.75,
            "risk_level": 4,
            "severity": 3,
            "true_severity": 3,
            "predicted_severity": 4,
            "speed": 0.0,
            "weather_condition": "rain",
            "weather_code": "3",
            "road_type": "unknown",
            "event_timestamp": "2026-05-12T09:34:30Z",
            "prediction_timestamp": "2026-05-12T09:34:31Z",
            "source": "tomtom",
            "lat": 40.73,
            "lng": -74.001,
            "lon": -74.001,
            "model_status": "SUCCESS",
            "hour": 9,
            "event_year": 2026,
            "is_weekend": 0,
            "is_rush_hour": 1,
            "is_night": 0,
        }

        row = build_prediction_row(event)

        self.assertEqual(set(row.keys()), set(PREDICTION_COLUMNS))
        self.assertEqual(row["event_id"], "evt-1")
        self.assertEqual(row["risk_level"], "high")
        self.assertEqual(row["lat"], 40.73)
        self.assertEqual(row["lon"], -74.001)
        self.assertEqual(row["has_traffic_signal"], 0)
        self.assertEqual(row["model_status"], "SUCCESS")


if __name__ == "__main__":
    unittest.main()
