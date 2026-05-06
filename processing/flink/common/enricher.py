"""
Enrichment module for streaming pipeline.
Provides reusable enrichment logic: grid cell mapping, time features, weather, etc.
"""
import logging
import math
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import redis

logger = logging.getLogger(__name__)


class EnrichmentConfig:
    """Configuration for enrichment parameters."""

    def __init__(self):
        self.grid_cell_size_meters = 100
        # Hanoi bounding box
        self.min_lat = float(os.getenv("ENRICH_MIN_LAT", "20.9"))
        self.max_lat = float(os.getenv("ENRICH_MAX_LAT", "21.1"))
        self.min_lon = float(os.getenv("ENRICH_MIN_LON", "105.7"))
        self.max_lon = float(os.getenv("ENRICH_MAX_LON", "106.0"))
        # Redis config
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("REDIS_PORT", "6379"))
        self.redis_db = int(os.getenv("REDIS_DB", "0"))
        self.redis_timeout = float(os.getenv("REDIS_TIMEOUT_MS", "2000")) / 1000.0
        # PostGIS config
        self.postgis_conn = os.getenv(
            "POSTGIS_CONNECTION_STRING",
            "postgresql://postgres:changeme@localhost:5432/accident_risk"
        )


class Enricher:
    """
    Enriches raw traffic events with additional context:
    - grid_cell_id
    - time-based features (hour_of_day, day_of_week, is_rush_hour, season)
    - road attributes (type, speed limit, lanes, traffic signals)
    - weather conditions (placeholder - would need weather API)
    """

    def __init__(self, config: Optional[EnrichmentConfig] = None):
        self.config = config or EnrichmentConfig()
        self._redis_client: Optional[redis.Redis] = None
        self._init_redis()

    def _init_redis(self):
        """Initialize Redis client if enabled."""
        try:
            self._redis_client = redis.Redis(
                host=self.config.redis_host,
                port=self.config.redis_port,
                db=self.config.redis_db,
                socket_timeout=self.config.redis_timeout,
                decode_responses=True
            )
            self._redis_client.ping()
            logger.info("Redis connection established")
        except Exception as e:
            logger.warning(f"Redis not available: {e}. Using static enrichment.")
            self._redis_client = None

    def compute_grid_cell_id(self, latitude: float, longitude: float) -> Optional[str]:
        """
        Compute grid cell ID from lat/lon using simple grid partitioning.
        Returns None if coordinates are outside configured bounds.
        """
        lat = float(latitude)
        lon = float(longitude)

        if not (self.config.min_lat <= lat <= self.config.max_lat and
                self.config.min_lon <= lon <= self.config.max_lon):
            return None

        # Simple grid: divide lat/lon range into cells of ~cell_size_meters
        # Approximate: 1 deg lat ≈ 111km, 1 deg lon ≈ 111km * cos(lat)
        lat_range = self.config.max_lat - self.config.min_lat
        lon_range = self.config.max_lon - self.config.min_lon

        # Number of cells in each dimension
        lat_cells = max(1, int((lat_range * 111000) / self.config.grid_cell_size_meters))
        lon_cells = max(1, int((lon_range * 111000 * math.cos(math.radians(lat))) /
                                self.config.grid_cell_size_meters))

        lat_idx = int((lat - self.config.min_lat) / lat_range * lat_cells)
        lon_idx = int((lon - self.config.min_lon) / lon_range * lon_cells)

        # Clamp to valid range
        lat_idx = max(0, min(lat_idx, lat_cells - 1))
        lon_idx = max(0, min(lon_idx, lon_cells - 1))

        return f"grid_{lat_idx}_{lon_idx}"

    def enrich_time_features(self, timestamp_str: str) -> Dict[str, Any]:
        """Extract time-based features from ISO-8601 timestamp."""
        try:
            # Parse timestamp
            if timestamp_str.endswith('Z'):
                timestamp_str = timestamp_str[:-1] + '+00:00'
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            dt = dt.astimezone(timezone.utc)
        except Exception:
            dt = datetime.now(timezone.utc)

        hour = dt.hour
        day_of_week = dt.weekday()  # 0=Monday, 6=Sunday

        # Rush hour: 7-9 AM and 5-7 PM on weekdays
        is_rush_hour = (0 <= day_of_week <= 4) and ((7 <= hour <= 9) or (17 <= hour <= 19))

        # Season based on month
        month = dt.month
        if month in (12, 1, 2):
            season = "winter"
        elif month in (3, 4, 5):
            season = "spring"
        elif month in (6, 7, 8):
            season = "summer"
        else:
            season = "autumn"

        return {
            "hour_of_day": hour,
            "day_of_week": day_of_week,
            "is_rush_hour": is_rush_hour,
            "season": season,
        }

    def enrich_road_attributes(self, flow_segment_id: Optional[str]) -> Dict[str, Any]:
        """
        Enrich with road attributes.
        Tries Redis first, falls back to static lookup or defaults.
        """
        default_attrs = {
            "road_type": None,
            "speed_limit_kmh": 0,
            "num_lanes": 0,
            "has_traffic_signal": False,
            "road_condition": None,
        }

        if not flow_segment_id:
            return default_attrs

        # Try Redis lookup
        if self._redis_client:
            try:
                key = f"road:{flow_segment_id}"
                data = self._redis_client.hgetall(key)
                if data:
                    return {
                        "road_type": data.get("road_type"),
                        "speed_limit_kmh": int(data.get("speed_limit_kmh", 0)),
                        "num_lanes": int(data.get("num_lanes", 0)),
                        "has_traffic_signal": data.get("has_traffic_signal", "false").lower() == "true",
                        "road_condition": data.get("road_condition"),
                    }
            except Exception as e:
                logger.warning(f"Redis lookup failed for {flow_segment_id}: {e}")

        # Static fallback (could be extended with a lookup table)
        return default_attrs

    def enrich_weather(self, latitude: float, longitude: float) -> Dict[str, Any]:
        """
        Enrich with weather conditions.
        Placeholder - in production would call a weather API.
        """
        # TODO: Integrate with weather API (OpenWeatherMap, etc.)
        return {
            "temperature_c": 0.0,
            "visibility_km": 0.0,
            "precipitation_mm": 0.0,
            "weather_condition": None,
        }

    def enrich(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Apply all enrichment steps to a validated event.
        Returns enriched event or None if enrichment fails critically.
        """
        try:
            enriched = dict(event)

            # Grid cell
            grid_cell_id = self.compute_grid_cell_id(
                enriched.get("latitude", 0),
                enriched.get("longitude", 0)
            )
            if not grid_cell_id:
                logger.warning(f"Failed to compute grid cell for event {enriched.get('event_id')}")
                return None
            enriched["grid_cell_id"] = grid_cell_id

            # Time features
            time_features = self.enrich_time_features(enriched.get("timestamp", ""))
            enriched.update(time_features)

            # Road attributes
            road_attrs = self.enrich_road_attributes(enriched.get("flow_segment_id"))
            enriched.update(road_attrs)

            # Weather
            weather = self.enrich_weather(
                enriched.get("latitude", 0),
                enriched.get("longitude", 0)
            )
            enriched.update(weather)

            # Processing timestamp
            enriched["processed_at"] = datetime.now(timezone.utc).isoformat()

            return enriched
        except Exception as e:
            logger.error(f"Enrichment failed for event {event.get('event_id')}: {e}")
            return None
