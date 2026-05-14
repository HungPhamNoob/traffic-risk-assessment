"""
Enrichment module for streaming pipeline.
Provides reusable enrichment logic: grid cell mapping, time features, weather, etc.
"""
import logging
import math
import os
from datetime import datetime, timezone
from urllib.parse import urlparse
from typing import Any, Dict, Optional

import requests
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
try:
    import redis
except ImportError:  # local tests can run without the native Redis dependency installed
    redis = None

logger = logging.getLogger(__name__)


class EnrichmentConfig:
    """Configuration for enrichment parameters."""

    def __init__(self):
        self.grid_cell_size_meters = 100
        self.min_lat = float(os.getenv("ENRICH_MIN_LAT", "-90.0"))
        self.max_lat = float(os.getenv("ENRICH_MAX_LAT", "90.0"))
        self.min_lon = float(os.getenv("ENRICH_MIN_LON", "-180.0"))
        self.max_lon = float(os.getenv("ENRICH_MAX_LON", "180.0"))
        # Redis config
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("REDIS_PORT", "6379"))
        self.redis_db = int(os.getenv("REDIS_DB", "0"))
        redis_url = os.getenv("REDIS_URL", "").strip()
        if redis_url:
            parsed = urlparse(redis_url)
            self.redis_host = parsed.hostname or self.redis_host
            self.redis_port = parsed.port or self.redis_port
            if parsed.path and parsed.path.strip("/"):
                self.redis_db = int(parsed.path.strip("/"))
        self.redis_timeout = float(os.getenv("REDIS_TIMEOUT_MS", "2000")) / 1000.0
        # PostGIS config
        self.postgis_conn = os.getenv(
            "POSTGIS_CONNECTION_STRING",
            "postgresql://postgres:changeme@localhost:5432/accident_risk"
        )
        self.weather_enabled = os.getenv("WEATHER_ENRICHMENT_ENABLED", "true").lower() == "true"
        self.weather_timeout = float(os.getenv("WEATHER_TIMEOUT_SECONDS", "3"))
        self.weather_endpoint = os.getenv(
            "OPEN_METEO_ENDPOINT",
            "https://api.open-meteo.com/v1/forecast"
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
        self._redis_client: Optional[Any] = None
        self._init_redis()

    def _init_redis(self):
        """Initialize Redis client if enabled."""
        if redis is None:
            logger.warning("redis package not installed. Using static enrichment.")
            self._redis_client = None
            return
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
        spark_day_of_week = ((day_of_week + 1) % 7) + 1  # 1=Sunday, 7=Saturday

        # Match the training feature contract: weekday 7-9 or 16-18 UTC.
        is_rush_hour = (0 <= day_of_week <= 4) and ((7 <= hour <= 9) or (16 <= hour <= 18))
        is_weekend = 1 if day_of_week >= 5 else 0
        is_night = 1 if hour >= 22 or hour <= 5 else 0

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
            "event_year": dt.year,
            "hour": hour,
            "hour_of_day": hour,
            "day_of_week": spark_day_of_week,
            "is_weekend": is_weekend,
            "is_rush_hour": 1 if is_rush_hour else 0,
            "is_night": is_night,
            "season": season,
        }

    def enrich_road_attributes(
        self,
        flow_segment_id: Optional[str],
        grid_cell_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Enrich with road attributes.
        Tries Redis first, falls back to static lookup or defaults.
        """
        default_attrs = {
            "road_type": "unknown",
            "road_type_code": 0,
            "speed_limit_kmh": 0,
            "num_lanes": 0,
            "has_traffic_signal": 0,
            "is_junction": 0,
            "is_crossing": 0,
            "is_roundabout": 0,
            "is_stop": 0,
            "is_station": 0,
            "is_railway": 0,
            "road_condition": None,
        }

        # Try Redis lookup
        if self._redis_client:
            lookup_keys = []
            if grid_cell_id:
                lookup_keys.append(f"road:grid:{grid_cell_id}")
            if flow_segment_id:
                lookup_keys.append(f"road:{flow_segment_id}")

            for key in lookup_keys:
                try:
                    data = self._redis_client.hgetall(key)
                    if data:
                        return self._road_attrs_from_mapping(data)
                except Exception as e:
                    logger.warning(f"Redis road lookup failed for {key}: {e}")

        # Static fallback (could be extended with a lookup table)
        return default_attrs

    def _road_attrs_from_mapping(self, data: Dict[str, Any]) -> Dict[str, Any]:
        road_type = data.get("road_type") or "unknown"
        return {
            "road_type": road_type,
            "road_type_code": int(data.get("road_type_code") or self._road_type_code(road_type)),
            "speed_limit_kmh": int(float(data.get("speed_limit_kmh", 0) or 0)),
            "num_lanes": int(float(data.get("num_lanes", 0) or 0)),
            "has_traffic_signal": self._bool_int(data.get("has_traffic_signal")),
            "is_junction": self._bool_int(data.get("is_junction")),
            "is_crossing": self._bool_int(data.get("is_crossing")),
            "is_roundabout": self._bool_int(data.get("is_roundabout")),
            "is_stop": self._bool_int(data.get("is_stop")),
            "is_station": self._bool_int(data.get("is_station")),
            "is_railway": self._bool_int(data.get("is_railway")),
            "road_condition": data.get("road_condition"),
        }

    @staticmethod
    def _bool_int(value: Any) -> int:
        if isinstance(value, bool):
            return 1 if value else 0
        if value is None:
            return 0
        return 1 if str(value).strip().lower() in {"1", "true", "yes", "y"} else 0

    @staticmethod
    def _clip_float(value: Any, low: float, high: float, default: float) -> float:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return default
        return max(low, min(high, number))

    @staticmethod
    def _road_type_code(road_type: Optional[str]) -> int:
        mapping = {
            "unknown": 0,
            "interstate": 1,
            "freeway": 1,
            "motorway": 1,
            "highway": 1,
            "route": 2,
            "major": 2,
            "street": 3,
            "minor": 3,
            "avenue": 4,
            "local": 4,
            "boulevard": 5,
            "drive": 6,
            "road": 7,
        }
        return mapping.get((road_type or "unknown").lower(), 0)

    @staticmethod
    def _weather_code(weather_condition: Optional[str]) -> int:
        """Map weather to the model feature contract: 0=clear, 1=rain, 2=snow,
        3=fog, 4=storm, 5=cloudy, 6=windy.
        """
        condition = (weather_condition or "").lower()
        if not condition:
            return 0
        if "wind" in condition:
            return 6
        if "thunder" in condition or "storm" in condition:
            return 4
        if "snow" in condition:
            return 2
        if "ice" in condition or "sleet" in condition or "freezing" in condition:
            return 2
        if "fog" in condition or "mist" in condition or "haze" in condition:
            return 3
        if "rain" in condition or "drizzle" in condition:
            return 1
        if "cloud" in condition or "overcast" in condition:
            return 5
        if "clear" in condition or "fair" in condition:
            return 0
        return 0

    @staticmethod
    def _open_meteo_weather_label(weather_code: Any) -> str:
        labels = {
            0: "clear",
            1: "mostly_clear",
            2: "partly_cloudy",
            3: "overcast",
            45: "fog",
            48: "fog",
            51: "drizzle",
            53: "drizzle",
            55: "drizzle",
            61: "rain",
            63: "rain",
            65: "heavy_rain",
            71: "snow",
            73: "snow",
            75: "heavy_snow",
            80: "rain",
            81: "rain",
            82: "heavy_rain",
            95: "thunderstorm",
        }
        try:
            return labels.get(int(weather_code), "unknown")
        except (TypeError, ValueError):
            return "unknown"

    def enrich_weather(self, latitude: float, longitude: float) -> Dict[str, Any]:
        """
        Enrich with weather conditions.
        Uses Open-Meteo by default and falls back to deterministic neutral values.
        """
        fallback = {
            "temperature_c": 10.0,
            "temperature_f": 50.0,
            "humidity": 50.0,
            "wind_speed_mph": 0.0,
            "visibility_km": 16.09344,
            "visibility_mi": 10.0,
            "precipitation_mm": 0.0,
            "weather_condition": "unknown",
            "weather_code": 0,
        }
        if not self.config.weather_enabled:
            return fallback
        try:
            response = requests.get(
                self.config.weather_endpoint,
                params={
                    "latitude": latitude,
                    "longitude": longitude,
                    "current": (
                        "temperature_2m,relative_humidity_2m,precipitation,"
                        "weather_code,wind_speed_10m,visibility"
                    ),
                    "temperature_unit": "fahrenheit",
                    "wind_speed_unit": "mph",
                    "timezone": "UTC",
                },
                timeout=self.config.weather_timeout,
            )
            if response.status_code != 200:
                logger.warning("Open-Meteo weather lookup failed: HTTP %s", response.status_code)
                return fallback
            current = response.json().get("current") or {}
            condition = self._open_meteo_weather_label(current.get("weather_code"))
            visibility_m = float(current.get("visibility") or 0.0)
            visibility_mi = self._clip_float(visibility_m / 1609.344, 0.0, 10.0, 10.0)
            precipitation_mm = float(current.get("precipitation") or 0.0)
            temperature_f = self._clip_float(current.get("temperature_2m"), -40.0, 130.0, 50.0)
            humidity = self._clip_float(current.get("relative_humidity_2m"), 0.0, 100.0, 50.0)
            wind_speed_mph = self._clip_float(current.get("wind_speed_10m"), 0.0, 100.0, 0.0)
            if wind_speed_mph >= 25.0 and condition in {"clear", "mostly_clear", "partly_cloudy", "unknown"}:
                condition = "windy"
            return {
                "temperature_c": (temperature_f - 32.0) * 5.0 / 9.0,
                "temperature_f": temperature_f,
                "humidity": humidity,
                "wind_speed_mph": wind_speed_mph,
                "visibility_km": visibility_mi * 1.609344,
                "visibility_mi": visibility_mi,
                "precipitation_mm": precipitation_mm,
                "weather_condition": condition,
                "weather_code": self._weather_code(condition),
            }
        except Exception as e:
            logger.warning("Weather enrichment failed: %s", e)
            return fallback

    def enrich(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Apply all enrichment steps to a validated event.
        Returns enriched event or None if enrichment fails critically.
        """
        try:
            enriched = dict(event)
            event_timestamp = enriched.get("event_timestamp") or enriched.get("timestamp", "")
            enriched["event_timestamp"] = event_timestamp
            enriched["event_time"] = event_timestamp
            enriched.setdefault("ingestion_time", datetime.now(timezone.utc).isoformat())

            # Grid cell
            grid_cell_id = self.compute_grid_cell_id(
                enriched.get("latitude", 0),
                enriched.get("longitude", 0)
            )
            if not grid_cell_id:
                logger.warning(f"Failed to compute grid cell for event {enriched.get('event_id')}")
                return None
            enriched["grid_cell_id"] = grid_cell_id
            enriched["lat"] = float(enriched.get("latitude", 0))
            enriched["lng"] = float(enriched.get("longitude", 0))
            enriched["lon"] = float(enriched.get("longitude", 0))
            enriched["geom"] = f"POINT ({enriched['lon']} {enriched['lat']})"

            # Time features
            time_features = self.enrich_time_features(event_timestamp)
            enriched.update(time_features)

            # Road attributes
            road_attrs = self.enrich_road_attributes(
                enriched.get("flow_segment_id"),
                grid_cell_id=enriched.get("grid_cell_id"),
            )
            enriched.update(road_attrs)

            # Weather
            weather = self.enrich_weather(
                enriched.get("latitude", 0),
                enriched.get("longitude", 0)
            )
            enriched.update(weather)

            # Processing timestamp
            processed_at = datetime.now(timezone.utc).isoformat()
            enriched["processed_at"] = processed_at
            enriched["processed_time"] = processed_at
            enriched.setdefault("severity", 1)
            enriched.setdefault("true_severity", enriched.get("severity"))
            enriched.setdefault("risk_score", None)
            enriched.setdefault("risk_level", None)

            return enriched
        except Exception as e:
            logger.error(f"Enrichment failed for event {event.get('event_id')}: {e}")
            return None
