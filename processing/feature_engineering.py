# ============================================================
# processing/feature_engineering.py
# Shared Feature Engineering for Flink & Spark
# ============================================================
# Purpose:
#   Both Flink (streaming) and Spark (batch) consume raw CSV rows
#   from Kafka and call build_features() to produce an identical
#   feature vector that the H2O model expects.
#
# Usage:
#   from processing.feature_engineering import build_features
#   features = build_features(raw_row_dict)
# ============================================================

from datetime import datetime
from typing import Any, Optional, Dict


# ============================================================
# Safe type conversion helpers
# ============================================================


def _safe_string(value: Any, default: str = "") -> str:
    """Convert any value to a clean string. Never returns None."""
    if value is None:
        return default

    text = str(value).strip()
    return text if text else default


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    """Convert a value to float, returning default if conversion fails."""
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    """Convert a value to int, returning default if conversion fails."""
    try:
        if value is None or str(value).strip() == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _safe_bool_as_int(value: Any) -> int:
    """Convert boolean-like CSV field to 0 or 1."""
    text = _safe_string(value).lower()
    if text in {"true", "1", "yes", "y"}:
        return 1
    return 0


# ============================================================
# Timestamp parsing
# ============================================================


def parse_datetime(value: Any) -> Optional[datetime]:
    """Parse US Accidents timestamp string into datetime object."""
    text = _safe_string(value)
    if not text:
        return None

    # Common formats in US Accidents dataset
    for fmt in [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]:
        try:
            return datetime.strptime(text.replace("Z", ""), fmt)
        except ValueError:
            continue

    # ISO format as last resort
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


# ============================================================
# Time feature helpers
# ============================================================


def spark_day_of_week(event_time: datetime) -> int:
    """
    Return day of week matching Spark's dayofweek() convention.
    Spark: 1=Sunday, 2=Monday, ..., 7=Saturday
    Python weekday: 0=Monday, 6=Sunday
    """
    return (event_time.isoweekday() % 7) + 1


def is_rush_hour(hour: int) -> int:
    """Return 1 during commute hours (7-9 AM, 4-6 PM)."""
    if 7 <= hour <= 9:
        return 1
    if 16 <= hour <= 18:
        return 1
    return 0


# ============================================================
# Feature encoding functions
# ============================================================


def encode_weather_condition(weather_text: Any) -> int:
    """
    Map raw US Accidents weather description to numeric code.

    Codes:
        0 = unknown / clear / normal
        1 = rain / drizzle / shower
        2 = snow / ice / sleet
        3 = fog / haze / mist
        4 = storm / thunder
        5 = cloudy / overcast
        6 = windy
    """
    weather = _safe_string(weather_text).lower()

    if not weather:
        return 0

    if "thunder" in weather or "storm" in weather:
        return 4
    if "snow" in weather or "ice" in weather or "sleet" in weather:
        return 2
    if "rain" in weather or "drizzle" in weather or "shower" in weather:
        return 1
    if "fog" in weather or "haze" in weather or "mist" in weather:
        return 3
    if "cloud" in weather or "overcast" in weather:
        return 5
    if "wind" in weather:
        return 6

    return 0


def encode_road_type(street_name: Any) -> int:
    """
    Infer road type code from US Accidents Street field.

    Codes:
        0 = unknown / local road
        1 = interstate / freeway / highway
        2 = route / state route / US route
        3 = street
        4 = avenue
        5 = boulevard
        6 = drive
        7 = road
    """
    street = _safe_string(street_name).lower()

    if not street:
        return 0

    if any(
        t in street for t in ["interstate", "i-", "freeway", "fwy", "highway", "hwy"]
    ):
        return 1
    if any(t in street for t in ["route", "state route", "us-", "sr-"]):
        return 2
    if any(t in street for t in ["street", " st", "st."]):
        return 3
    if any(t in street for t in ["avenue", " ave", "ave."]):
        return 4
    if any(t in street for t in ["boulevard", " blvd", "blvd."]):
        return 5
    if any(t in street for t in ["drive", " dr", "dr."]):
        return 6
    if any(t in street for t in ["road", " rd", "rd."]):
        return 7

    return 0


# ============================================================
# Main feature builder - called by both Flink and Spark
# ============================================================


def build_features(raw_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Convert a raw US Accidents CSV row into the standard feature vector.

    Parameters
    ----------
    raw_row : dict
        A single row from the US Accidents CSV with all 46 original columns.
        Keys match the CSV header: ID, Severity, Start_Time, Start_Lat,
        Start_Lng, Weather_Condition, Temperature(F), ...

    Returns
    -------
    dict or None
        Feature dict with keys:
            event_id, event_year, event_time, true_severity,
            lat, lon,
            hour, day_of_week, is_weekend, is_rush_hour,
            weather_code, temperature_f, humidity,
            wind_speed_mph, visibility_mi,
            road_type_code, is_junction, has_traffic_signal,
            is_crossing, is_roundabout, is_stop,
            is_station, is_railway, is_night
        Returns None if any critical field is missing.
    """
    # ---- Parse timestamp ----
    event_time = parse_datetime(raw_row.get("Start_Time"))
    if event_time is None:
        return None

    # ---- Critical fields ----
    lat = _safe_float(raw_row.get("Start_Lat"))
    lon = _safe_float(raw_row.get("Start_Lng"))
    severity = _safe_int(raw_row.get("Severity"))
    event_id = _safe_string(raw_row.get("ID"))

    if lat is None or lon is None or severity is None or not event_id:
        return None

    # ---- Time features ----
    event_year = event_time.year
    hour = event_time.hour
    day_of_week = spark_day_of_week(event_time)
    is_weekend = 1 if day_of_week in {1, 7} else 0
    is_rush = is_rush_hour(hour)

    # ---- Weather features ----
    weather_code = encode_weather_condition(raw_row.get("Weather_Condition"))
    temperature_f = _safe_float(raw_row.get("Temperature(F)"), default=50.0)
    humidity = _safe_float(raw_row.get("Humidity(%)"), default=50.0)
    wind_speed_mph = _safe_float(raw_row.get("Wind_Speed(mph)"), default=0.0)
    visibility_mi = _safe_float(raw_row.get("Visibility(mi)"), default=10.0)

    # ---- Road type ----
    road_type_code = encode_road_type(raw_row.get("Street"))

    # ---- Boolean flags ----
    is_junction = _safe_bool_as_int(raw_row.get("Junction"))
    has_traffic_signal = _safe_bool_as_int(raw_row.get("Traffic_Signal"))
    is_crossing = _safe_bool_as_int(raw_row.get("Crossing"))
    is_roundabout = _safe_bool_as_int(raw_row.get("Roundabout"))
    is_stop = _safe_bool_as_int(raw_row.get("Stop"))
    is_station = _safe_bool_as_int(raw_row.get("Station"))
    is_railway = _safe_bool_as_int(raw_row.get("Railway"))

    # ---- Light ----
    sunrise_sunset = _safe_string(raw_row.get("Sunrise_Sunset")).lower()
    is_night = 1 if sunrise_sunset == "night" else 0

    return {
        # Metadata
        "event_id": event_id,
        "event_year": event_year,
        "event_time": event_time.isoformat(),
        # Label (NOT an input feature - used for evaluation)
        "true_severity": severity,
        # Geospatial
        "lat": lat,
        "lon": lon,
        # Time
        "hour": hour,
        "day_of_week": day_of_week,
        "is_weekend": is_weekend,
        "is_rush_hour": is_rush,
        # Weather
        "weather_code": weather_code,
        "temperature_f": temperature_f,
        "humidity": humidity,
        "wind_speed_mph": wind_speed_mph,
        "visibility_mi": visibility_mi,
        # Road / context
        "road_type_code": road_type_code,
        "is_junction": is_junction,
        "has_traffic_signal": has_traffic_signal,
        "is_crossing": is_crossing,
        "is_roundabout": is_roundabout,
        "is_stop": is_stop,
        "is_station": is_station,
        "is_railway": is_railway,
        # Light
        "is_night": is_night,
    }
