"""
processing/streaming_enrichment.py
Real-time enrichment for TomTom incident events before shared feature engineering.

US replay rows already conform to the model training schema. TomTom incident
events require a small projection step – normalizing coordinates, timestamps,
and road-infrastructure flags – plus optional live weather enrichment from
Open-Meteo before processing.feature_engineering.build_features() can consume
them.
"""

import logging
import os
from datetime import datetime, timezone
from time import monotonic
from typing import Any, Dict, List, Optional

import requests

from processing.feature_engineering import _safe_float, _safe_int, _safe_string

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Weather enrichment configuration
# ---------------------------------------------------------------------------

OPEN_METEO_ENDPOINT = os.getenv(
    "OPEN_METEO_ENDPOINT", "https://api.open-meteo.com/v1/forecast"
)
OPEN_METEO_ARCHIVE_ENDPOINT = os.getenv(
    "OPEN_METEO_ARCHIVE_ENDPOINT", "https://archive-api.open-meteo.com/v1/archive"
)
OPEN_METEO_TIMEOUT_SECONDS = float(os.getenv("OPEN_METEO_TIMEOUT_SECONDS", "5"))
WEATHER_ENRICHMENT_ENABLED = os.getenv(
    "STREAMING_WEATHER_ENRICHMENT_ENABLED", "true"
).lower() in {"1", "true", "yes"}
OPEN_METEO_CACHE_TTL_SECONDS = float(
    os.getenv("OPEN_METEO_CACHE_TTL_SECONDS", "900")
)
OPEN_METEO_BACKOFF_SECONDS = float(
    os.getenv("OPEN_METEO_BACKOFF_SECONDS", "300")
)
OPEN_METEO_COORDINATE_PRECISION = int(
    os.getenv("OPEN_METEO_COORDINATE_PRECISION", "2")
)
_WEATHER_CACHE: Dict[tuple[Any, ...], tuple[float, Dict[str, Any]]] = {}
_OPEN_METEO_BACKOFF_UNTIL = 0.0


# ---------------------------------------------------------------------------
# TomTom severity normalisation
# ---------------------------------------------------------------------------


def normalize_tomtom_severity(delay_magnitude: Any, icon_category: Any) -> int:
    """
    Map TomTom magnitudeOfDelay and iconCategory to a 1–4 severity scale.

    The rule-based mapping mirrors the US Accidents severity convention so
    that both data sources share the same feature schema. No H2O model is
    needed for TomTom – the label is derived directly from TomTom signals.

    Rules:
        - magnitudeOfDelay 0-1  → severity 1 (minor)
        - magnitudeOfDelay 2    → severity 2 (moderate)
        - magnitudeOfDelay 3    → severity 3 (serious)
        - magnitudeOfDelay >= 4 → severity 4 (major)
        - iconCategory 8 (road_closed) elevates to at least 4
        - iconCategory 1 (accident)    elevates to at least 3
        - iconCategory 9 (congestion)  elevates to at least 2
    """
    severity = 1
    delay = _safe_int(delay_magnitude)
    icon = _safe_int(icon_category)

    if delay is not None:
        if delay >= 4:
            severity = 4
        elif delay == 3:
            severity = 3
        elif delay == 2:
            severity = 2

    if icon == 8:
        severity = max(severity, 4)
    elif icon == 1:
        severity = max(severity, 3)
    elif icon == 9:
        severity = max(severity, 2)

    return severity


# ---------------------------------------------------------------------------
# Open-Meteo weather label helper
# ---------------------------------------------------------------------------


def _weather_label_from_code(code: Any) -> str:
    """Return a human-readable weather condition label from a WMO weather code."""
    weather_code = _safe_int(code, default=0) or 0
    if weather_code in {95, 96, 99}:
        return "Thunderstorm"
    if weather_code in {71, 73, 75, 77, 85, 86}:
        return "Snow"
    if weather_code in {51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82}:
        return "Rain"
    if weather_code in {45, 48}:
        return "Fog"
    if weather_code in {1, 2, 3}:
        return "Cloudy"
    return "Clear"


# ---------------------------------------------------------------------------
# Timestamp parsing helpers
# ---------------------------------------------------------------------------


def _parse_event_time(timestamp: Any) -> Optional[datetime]:
    """Parse an ISO 8601 timestamp string into a UTC datetime, rounded to the hour."""
    text = _safe_string(timestamp)
    if not text:
        return None
    try:
        event_time = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=timezone.utc)
    return event_time.astimezone(timezone.utc).replace(
        minute=0, second=0, microsecond=0
    )


def _parse_open_meteo_time(value: Any) -> Optional[datetime]:
    """Parse an Open-Meteo hourly time string into a UTC datetime."""
    text = _safe_string(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _nearest_hour_index(times: List[Any], event_time: datetime) -> Optional[int]:
    """Return the index of the closest hourly time slot to event_time."""
    best_index: Optional[int] = None
    best_delta: Optional[float] = None
    for index, value in enumerate(times):
        candidate = _parse_open_meteo_time(value)
        if candidate is None:
            continue
        delta = abs((candidate - event_time).total_seconds())
        if best_delta is None or delta < best_delta:
            best_index = index
            best_delta = delta
    return best_index


def _value_at(values: Any, index: Optional[int]) -> Any:
    """Safe list index accessor. Returns None if index is out of range."""
    if index is None or not isinstance(values, list) or index >= len(values):
        return None
    return values[index]


def _cache_key(lat: float, lon: float, event_time: Optional[datetime]) -> tuple[Any, ...]:
    """Bucket nearby events together so repeated weather lookups reuse responses."""
    rounded_lat = round(lat, OPEN_METEO_COORDINATE_PRECISION)
    rounded_lon = round(lon, OPEN_METEO_COORDINATE_PRECISION)
    if event_time is None:
        return ("current", rounded_lat, rounded_lon)
    return (
        event_time.date().isoformat(),
        event_time.hour,
        rounded_lat,
        rounded_lon,
    )


def _get_cached_weather(cache_key: tuple[Any, ...]) -> Dict[str, Any]:
    """Return a cached weather payload if it is still fresh."""
    cached = _WEATHER_CACHE.get(cache_key)
    if not cached:
        return {}
    cached_at, payload = cached
    if monotonic() - cached_at > OPEN_METEO_CACHE_TTL_SECONDS:
        _WEATHER_CACHE.pop(cache_key, None)
        return {}
    return dict(payload)


def _set_cached_weather(cache_key: tuple[Any, ...], payload: Dict[str, Any]) -> None:
    """Store a successful weather payload for future nearby events."""
    if payload:
        _WEATHER_CACHE[cache_key] = (monotonic(), dict(payload))


# ---------------------------------------------------------------------------
# Open-Meteo weather fetch
# ---------------------------------------------------------------------------


def _fetch_current_open_meteo_weather(lat: float, lon: float) -> Dict[str, Any]:
    """Fetch the current weather conditions from Open-Meteo for a coordinate."""
    response = requests.get(
        OPEN_METEO_ENDPOINT,
        params={
            "latitude": lat,
            "longitude": lon,
            "current": ",".join(
                [
                    "temperature_2m",
                    "relative_humidity_2m",
                    "weather_code",
                    "wind_speed_10m",
                ]
            ),
            "temperature_unit": "fahrenheit",
            "wind_speed_unit": "mph",
            "timezone": "UTC",
        },
        timeout=OPEN_METEO_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    current = response.json().get("current") or {}
    return {
        "Weather_Condition": _weather_label_from_code(current.get("weather_code")),
        "Temperature(F)": current.get("temperature_2m"),
        "Humidity(%)": current.get("relative_humidity_2m"),
        "Wind_Speed(mph)": current.get("wind_speed_10m"),
    }


def fetch_open_meteo_weather(
    lat: float,
    lon: float,
    timestamp: Any = None,
) -> Dict[str, Any]:
    """
    Fetch weather for the given coordinate and event timestamp from Open-Meteo.

    For past timestamps the archive endpoint is used. For the current hour the
    forecast endpoint is used. Returns an empty dict if enrichment is disabled
    or if the API call fails.
    """
    if not WEATHER_ENRICHMENT_ENABLED:
        return {}

    global _OPEN_METEO_BACKOFF_UNTIL
    try:
        event_time = _parse_event_time(timestamp)
        cache_key = _cache_key(lat, lon, event_time)
        cached = _get_cached_weather(cache_key)
        if cached:
            return cached

        if monotonic() < _OPEN_METEO_BACKOFF_UNTIL:
            return {}

        if event_time is None:
            payload = _fetch_current_open_meteo_weather(lat, lon)
            _set_cached_weather(cache_key, payload)
            return payload

        today = datetime.now(timezone.utc).date()
        endpoint = (
            OPEN_METEO_ARCHIVE_ENDPOINT
            if event_time.date() < today
            else OPEN_METEO_ENDPOINT
        )
        response = requests.get(
            endpoint,
            params={
                "latitude": lat,
                "longitude": lon,
                "hourly": ",".join(
                    [
                        "temperature_2m",
                        "relative_humidity_2m",
                        "weather_code",
                        "wind_speed_10m",
                    ]
                ),
                "temperature_unit": "fahrenheit",
                "wind_speed_unit": "mph",
                "timezone": "UTC",
                "start_date": event_time.date().isoformat(),
                "end_date": event_time.date().isoformat(),
            },
            timeout=OPEN_METEO_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        hourly = response.json().get("hourly") or {}
        index = _nearest_hour_index(hourly.get("time") or [], event_time)
        weather_code = _value_at(hourly.get("weather_code"), index)
        payload = {
            "Weather_Condition": _weather_label_from_code(weather_code),
            "Temperature(F)": _value_at(hourly.get("temperature_2m"), index),
            "Humidity(%)": _value_at(hourly.get("relative_humidity_2m"), index),
            "Wind_Speed(mph)": _value_at(hourly.get("wind_speed_10m"), index),
            "weather_observed_at": _value_at(hourly.get("time"), index),
        }
        _set_cached_weather(cache_key, payload)
        return payload
    except requests.HTTPError as exc:
        response = exc.response
        if response is not None and response.status_code == 429:
            _OPEN_METEO_BACKOFF_UNTIL = monotonic() + OPEN_METEO_BACKOFF_SECONDS
            logger.warning(
                "Open-Meteo rate limit hit. Skipping weather enrichment for %.0fs.",
                OPEN_METEO_BACKOFF_SECONDS,
            )
            return {}
        logger.warning("Open-Meteo weather enrichment failed: %s", exc)
        return {}
    except requests.RequestException as exc:
        logger.warning("Open-Meteo weather enrichment failed: %s", exc)
        return {}
    except Exception:
        logger.exception("Open-Meteo weather enrichment failed")
        return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _first_non_empty(*values: Any, default: str = "") -> str:
    """Return the first non-empty string from the provided values."""
    for value in values:
        text = _safe_string(value)
        if text:
            return text
    return default


def _infer_light(timestamp: Any) -> str:
    """Return 'Night' for hours 22:00-05:59 UTC, otherwise 'Day'."""
    text = _safe_string(timestamp)
    try:
        event_time = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return "Day"
    return "Night" if event_time.hour >= 22 or event_time.hour < 6 else "Day"


# ---------------------------------------------------------------------------
# TomTom event enrichment – main entry point
# ---------------------------------------------------------------------------


def enrich_tomtom_event(raw_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Project a raw TomTom incident event into the US Accidents feature schema.

    The output is consumed by processing.feature_engineering.build_features()
    to produce the final feature vector stored in traffic_tomtom_incidents.

    Returns None when the mandatory fields (lat, lon, timestamp, event_id)
    are missing, so the Flink sink can skip the record without crashing.
    """
    lat = _safe_float(raw_row.get("latitude", raw_row.get("lat")))
    lon = _safe_float(raw_row.get("longitude", raw_row.get("lon", raw_row.get("lng"))))
    timestamp = _first_non_empty(
        raw_row.get("timestamp"),
        raw_row.get("event_timestamp"),
        raw_row.get("last_report_time"),
    )
    event_id = _first_non_empty(raw_row.get("event_id"), raw_row.get("incident_id"))

    if lat is None or lon is None or not timestamp or not event_id:
        return None

    weather = fetch_open_meteo_weather(lat, lon, timestamp)
    severity = normalize_tomtom_severity(
        raw_row.get("delay_magnitude"), raw_row.get("icon_category")
    )
    street = _first_non_empty(
        raw_row.get("from_road"),
        raw_row.get("to_road"),
        ", ".join(raw_row.get("road_numbers") or []),
        default="",
    )

    enriched = dict(raw_row)
    enriched.update(
        {
            # Map to the US Accidents schema keys expected by build_features().
            "ID": event_id,
            "Severity": severity,
            "Start_Time": timestamp,
            "Start_Lat": lat,
            "Start_Lng": lon,
            "Weather_Condition": weather.get("Weather_Condition", "Clear"),
            "Temperature(F)": weather.get("Temperature(F)", 50.0),
            "Humidity(%)": weather.get("Humidity(%)", 50.0),
            "Wind_Speed(mph)": weather.get("Wind_Speed(mph)", 0.0),
            "weather_observed_at": weather.get("weather_observed_at"),
            "Visibility(mi)": raw_row.get("visibility_mi", 10.0),
            "Street": street,
            # Infrastructure flags default to 0 for TomTom incidents.
            "Junction": raw_row.get("is_junction", 0),
            "Traffic_Signal": raw_row.get("has_traffic_signal", 0),
            "Crossing": raw_row.get("is_crossing", 0),
            "Roundabout": raw_row.get("is_roundabout", 0),
            "Stop": raw_row.get("is_stop", 0),
            "Station": raw_row.get("is_station", 0),
            "Railway": raw_row.get("is_railway", 0),
            "Sunrise_Sunset": _infer_light(timestamp),
        }
    )
    return enriched


def enrich_stream_event(raw_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Route a raw stream event to the appropriate enrichment function.

    TomTom events are enriched via enrich_tomtom_event(). US replay events
    already conform to the schema and are returned unchanged.
    """
    source = _safe_string(raw_row.get("source")).lower()
    if source == "tomtom":
        return enrich_tomtom_event(raw_row)
    return raw_row
