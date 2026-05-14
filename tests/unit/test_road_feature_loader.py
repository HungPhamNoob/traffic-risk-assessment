import unittest

from ingestion.kafka.producers.road_feature_loader import (
    aggregate_elements_by_grid,
    features_from_osm_tags,
    merge_road_features,
    road_type_from_highway,
)
from processing.flink.common.enricher import Enricher, EnrichmentConfig


class TestRoadFeatureLoader(unittest.TestCase):
    def test_road_type_from_highway(self):
        self.assertEqual(road_type_from_highway("motorway"), "interstate")
        self.assertEqual(road_type_from_highway("primary"), "route")
        self.assertEqual(road_type_from_highway("residential"), "street")
        self.assertEqual(road_type_from_highway("service"), "road")
        self.assertEqual(road_type_from_highway("traffic_signals"), "unknown")

    def test_features_from_osm_tags(self):
        features = features_from_osm_tags({
            "highway": "traffic_signals",
            "maxspeed": "35 mph",
            "lanes": "2",
            "crossing": "marked",
        })

        self.assertEqual(features["has_traffic_signal"], 1)
        self.assertEqual(features["is_crossing"], 1)
        self.assertEqual(features["speed_limit_kmh"], 56)
        self.assertEqual(features["num_lanes"], 2)

    def test_merge_road_features(self):
        merged = merge_road_features(
            {
                "road_type": "street",
                "road_type_code": 3,
                "speed_limit_kmh": 30,
                "num_lanes": 1,
                "has_traffic_signal": 0,
                "is_junction": 0,
                "is_crossing": 0,
                "is_roundabout": 0,
                "is_stop": 0,
                "is_station": 0,
                "is_railway": 0,
            },
            {
                "road_type": "route",
                "road_type_code": 2,
                "speed_limit_kmh": 50,
                "num_lanes": 2,
                "has_traffic_signal": 1,
                "is_junction": 0,
                "is_crossing": 1,
                "is_roundabout": 0,
                "is_stop": 0,
                "is_station": 0,
                "is_railway": 0,
            },
        )

        self.assertEqual(merged["road_type"], "street")
        self.assertEqual(merged["road_type_code"], 3)
        self.assertEqual(merged["speed_limit_kmh"], 50)
        self.assertEqual(merged["num_lanes"], 2)
        self.assertEqual(merged["has_traffic_signal"], 1)
        self.assertEqual(merged["is_crossing"], 1)

    def test_aggregate_elements_by_grid(self):
        config = EnrichmentConfig()
        config.min_lat = 40.4
        config.max_lat = 41.0
        config.min_lon = -74.3
        config.max_lon = -73.6
        enricher = Enricher(config)
        enricher._redis_client = None

        elements = [
            {
                "type": "way",
                "center": {"lat": 40.73, "lon": -74.001},
                "tags": {"highway": "primary", "maxspeed": "50", "lanes": "2"},
            },
            {
                "type": "node",
                "lat": 40.73001,
                "lon": -74.00101,
                "tags": {"highway": "traffic_signals"},
            },
        ]

        by_grid = aggregate_elements_by_grid(elements, enricher)

        self.assertEqual(len(by_grid), 1)
        features = next(iter(by_grid.values()))
        self.assertEqual(features["road_type"], "route")
        self.assertEqual(features["road_type_code"], 2)
        self.assertEqual(features["speed_limit_kmh"], 50)
        self.assertEqual(features["has_traffic_signal"], 1)


if __name__ == "__main__":
    unittest.main()
