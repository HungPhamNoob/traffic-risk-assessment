"""
TomTom Producer for streaming pipeline.
Fetches TomTom Traffic Incident Details and publishes normalized events to Kafka.
"""
import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Add project root to path for runtime imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

logger = logging.getLogger(__name__)

# Configuration
TOPIC = os.getenv("KAFKA_TOPIC_RAW", "tomtom.traffic.raw")
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY", "").strip()
TOMTOM_ENDPOINT = (
    (os.getenv("TOMTOM_ENDPOINT") or "").strip()
    or "https://api.tomtom.com/traffic/services/5/incidentDetails"
)
DEFAULT_TOMTOM_BBOXES = (
    "US:New York:-74.25909,40.477399,-73.700181,40.917577;"
    "UK:London:-0.510375,51.28676,0.334015,51.691874"
)
TOMTOM_BBOX = os.getenv("TOMTOM_BBOX", "")
TOMTOM_BBOXES = os.getenv("TOMTOM_BBOXES", DEFAULT_TOMTOM_BBOXES)
TOMTOM_LANGUAGE = os.getenv("TOMTOM_LANGUAGE") or "en-US"
TOMTOM_TIME_VALIDITY = os.getenv("TOMTOM_TIME_VALIDITY") or "present"
TOMTOM_FIELDS = (
    os.getenv("TOMTOM_FIELDS")
    or "{incidents{type,geometry{type,coordinates},properties{id,iconCategory,"
    "magnitudeOfDelay,events{description,code,iconCategory},startTime,endTime,"
    "from,to,length,delay,roadNumbers,timeValidity,probabilityOfOccurrence,"
    "numberOfReports,lastReportTime}}}"
)
POLL_INTERVAL_SECONDS = float(os.getenv("PRODUCER_POLL_INTERVAL", "60"))
THROUGHPUT_PER_SECOND = float(os.getenv("PRODUCER_THROUGHPUT", "10"))


def get_tomtom_regions() -> List[Dict[str, str]]:
    """
    Return configured TomTom bbox regions.

    TOMTOM_BBOXES format:
      country:city:minLon,minLat,maxLon,maxLat;country:city:minLon,minLat,maxLon,maxLat

    Legacy region_name:minLon,minLat,maxLon,maxLat is also accepted.

    TOMTOM_BBOX is still supported as a single-region override for local testing.
    """
    if TOMTOM_BBOX:
        region = os.getenv("TOMTOM_REGION", "custom")
        country = os.getenv("TOMTOM_COUNTRY", region)
        city = os.getenv("TOMTOM_CITY", region)
        return [{"region": region, "country": country, "city": city, "bbox": TOMTOM_BBOX}]

    regions: List[Dict[str, str]] = []
    for item in TOMTOM_BBOXES.split(";"):
        item = item.strip()
        if not item:
            continue
        parts = item.split(":")
        if len(parts) >= 3:
            country = parts[0].strip()
            city = parts[1].strip()
            bbox = ":".join(parts[2:]).strip()
            region = f"{country.lower()}_{city.lower().replace(' ', '_')}"
        elif len(parts) == 2:
            region, bbox = parts[0].strip(), parts[1].strip()
            country, city = _region_to_location(region)
        else:
            region, bbox = "custom", item
            country, city = "custom", "custom"
        regions.append({"region": region, "country": country, "city": city, "bbox": bbox})
    return regions


def _region_to_location(region: str, country: Optional[str] = None, city: Optional[str] = None) -> Tuple[str, str]:
    if country and city:
        return country, city
    if region.startswith("us_"):
        return "US", region.removeprefix("us_").replace("_", " ").title()
    if region.startswith("uk_"):
        return "UK", region.removeprefix("uk_").replace("_", " ").title()
    return region, region


def fetch_incident_details(region: Optional[Dict[str, str]] = None) -> Optional[Dict[str, Any]]:
    """Fetch traffic incidents from TomTom Incident Details API."""
    if not TOMTOM_API_KEY:
        logger.warning("No TomTom API key configured, using simulated data")
        return generate_simulated_data(region)

    try:
        import requests
        region = region or get_tomtom_regions()[0]
        params = {
            "key": TOMTOM_API_KEY,
            "bbox": region["bbox"],
            "fields": TOMTOM_FIELDS,
            "language": TOMTOM_LANGUAGE,
            "timeValidityFilter": TOMTOM_TIME_VALIDITY,
        }
        response = requests.get(TOMTOM_ENDPOINT, params=params, timeout=10)
        if response.status_code == 200:
            return response.json()
        logger.error(f"TomTom API error {response.status_code}: {response.text[:200]}")
    except Exception as e:
        logger.error(f"Failed to fetch from TomTom API: {e}")
    return None


def generate_simulated_data(region: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Generate a TomTom-like incident response for local testing/demo."""
    region = region or {
        "region": "us_new_york",
        "country": "US",
        "city": "New York",
        "bbox": "-74.25909,40.477399,-73.700181,40.917577",
    }
    min_lon, min_lat, max_lon, max_lat = [float(part) for part in region["bbox"].split(",")]
    lon = round((min_lon + max_lon) / 2, 7)
    lat = round((min_lat + max_lat) / 2, 7)
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "incidents": [
            {
                "type": "Feature",
                "properties": {
                    "id": f"sim_{region['region']}_{int(time.time())}",
                    "iconCategory": 6,
                    "magnitudeOfDelay": 2,
                    "startTime": now,
                    "endTime": None,
                    "from": region["region"],
                    "to": region["region"],
                    "length": 850.0,
                    "delay": 180,
                    "roadNumbers": [],
                    "timeValidity": "present",
                    "probabilityOfOccurrence": "certain",
                    "numberOfReports": 1,
                    "lastReportTime": now,
                    "events": [
                        {"code": 101, "description": "Slow traffic", "iconCategory": 6}
                    ],
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[lon, lat], [round(lon + 0.001, 7), round(lat + 0.001, 7)]],
                },
            }
        ]
    }


def _flatten_coordinates(coordinates: Any) -> List[Tuple[float, float]]:
    """Return GeoJSON coordinates as [(lon, lat), ...]."""
    points: List[Tuple[float, float]] = []

    def walk(value: Any) -> None:
        if not isinstance(value, list) or not value:
            return
        if len(value) >= 2 and all(isinstance(v, (int, float)) for v in value[:2]):
            points.append((float(value[0]), float(value[1])))
            return
        for item in value:
            walk(item)

    walk(coordinates)
    return points


def _geometry_to_wkt(geometry: Dict[str, Any]) -> str:
    """Convert simple GeoJSON geometry from TomTom to WKT."""
    geom_type = geometry.get("type")
    points = _flatten_coordinates(geometry.get("coordinates"))
    if not points:
        return "GEOMETRYCOLLECTION EMPTY"
    if geom_type == "Point" or len(points) == 1:
        lon, lat = points[0]
        return f"POINT ({lon} {lat})"
    coords = ", ".join(f"{lon} {lat}" for lon, lat in points)
    return f"LINESTRING ({coords})"


def _representative_lat_lon(geometry: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """Pick a stable representative point for validation/enrichment."""
    points = _flatten_coordinates(geometry.get("coordinates"))
    if not points:
        return None, None
    lon, lat = points[0]
    return lat, lon


def _incident_event_id(properties: Dict[str, Any], geometry: Dict[str, Any]) -> str:
    incident_id = properties.get("id")
    if incident_id:
        return f"tomtom-{incident_id}"
    digest = hashlib.sha1(
        json.dumps({"properties": properties, "geometry": geometry}, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]
    return f"tomtom-{digest}"


def map_tomtom_severity(icon_category: Optional[int], delay_magnitude: Optional[int]) -> int:
    """Map TomTom incident category/delay signals to the shared 1-4 severity scale."""
    try:
        magnitude = int(delay_magnitude or 0)
    except (TypeError, ValueError):
        magnitude = 0
    try:
        icon = int(icon_category) if icon_category is not None else None
    except (TypeError, ValueError):
        icon = None

    if magnitude >= 4:
        severity = 4
    elif magnitude == 3:
        severity = 3
    elif magnitude == 2:
        severity = 2
    else:
        severity = 1

    if icon == 8:  # road closure
        return max(severity, 4)
    if icon == 1:  # accident
        return max(severity, 3)
    if icon == 9:  # roadworks
        return max(severity, 2)
    return severity


def create_traffic_event(
    incident: Dict[str, Any],
    fetched_at: Optional[str] = None,
    region: Optional[str] = None,
    country: Optional[str] = None,
    city: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Create a normalized raw traffic event from a TomTom incident feature."""
    properties = incident.get("properties") or {}
    geometry = incident.get("geometry") or {}
    lat, lon = _representative_lat_lon(geometry)
    if lat is None or lon is None:
        logger.warning("Skipping TomTom incident without geometry coordinates")
        return None

    fetched_at = fetched_at or datetime.now(timezone.utc).isoformat()
    incident_id = properties.get("id") or _incident_event_id(properties, geometry).replace("tomtom-", "")
    events = properties.get("events") or []
    first_event = events[0] if events else {}
    state_or_region, city_name = _region_to_location(region or "custom", country, city)
    event_timestamp = properties.get("startTime") or properties.get("lastReportTime") or fetched_at
    severity = map_tomtom_severity(properties.get("iconCategory"), properties.get("magnitudeOfDelay"))

    return {
        "event_id": _incident_event_id(properties, geometry),
        "source": "tomtom",
        "flow_segment_id": incident_id,
        "latitude": lat,
        "longitude": lon,
        "speed": 0.0,
        "timestamp": event_timestamp,
        "event_timestamp": event_timestamp,
        "ingestion_time": fetched_at,
        "severity": severity,
        "true_severity": severity,
        "road_type": "unknown",
        "incident_id": incident_id,
        "icon_category": properties.get("iconCategory"),
        "delay_magnitude": properties.get("magnitudeOfDelay"),
        "delay_seconds": properties.get("delay"),
        "length_meters": properties.get("length"),
        "geometry_wkt": _geometry_to_wkt(geometry),
        "incident_description": first_event.get("description"),
        "incident_code": first_event.get("code"),
        "from_road": properties.get("from"),
        "to_road": properties.get("to"),
        "state_or_region": state_or_region,
        "city": city_name,
        "tomtom_region": region,
        "road_numbers": properties.get("roadNumbers"),
        "time_validity": properties.get("timeValidity"),
        "probability_of_occurrence": properties.get("probabilityOfOccurrence"),
        "number_of_reports": properties.get("numberOfReports"),
        "last_report_time": properties.get("lastReportTime"),
        "raw_payload": incident,
    }


def create_traffic_events(
    api_response: Dict[str, Any],
    region: Optional[str] = None,
    country: Optional[str] = None,
    city: Optional[str] = None,
) -> Iterable[Dict[str, Any]]:
    """Yield normalized events from a TomTom Incident Details API response."""
    fetched_at = datetime.now(timezone.utc).isoformat()
    for incident in api_response.get("incidents") or []:
        event = create_traffic_event(
            incident,
            fetched_at=fetched_at,
            region=region,
            country=country,
            city=city,
        )
        if event:
            yield event


def main() -> None:
    """Main entry point for TomTom producer."""
    from processing.flink.common.kafka_client import create_producer, flush_producer, send_message

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    logger.info("=" * 60)
    logger.info("TomTom Producer")
    logger.info("=" * 60)
    logger.info(f"Topic: {TOPIC}")
    logger.info(f"API enabled: {bool(TOMTOM_API_KEY)}")
    logger.info(f"Endpoint: {TOMTOM_ENDPOINT}")
    regions = get_tomtom_regions()
    logger.info(f"Regions: {regions}")
    logger.info(f"Poll interval: {POLL_INTERVAL_SECONDS}s")
    logger.info(f"Throughput: {THROUGHPUT_PER_SECOND} msg/s")

    producer = create_producer()
    msg_count = 0
    interval = 1.0 / max(THROUGHPUT_PER_SECOND, 0.1)  # Sleep between messages

    try:
        while True:
            produced_this_cycle = 0
            for region in regions:
                traffic_data = fetch_incident_details(region)
                if not traffic_data:
                    logger.warning(f"No TomTom incident data returned for {region['region']}")
                    continue

                for event in create_traffic_events(
                    traffic_data,
                    region=region["region"],
                    country=region.get("country"),
                    city=region.get("city"),
                ):
                    # Send to Kafka
                    success = send_message(
                        producer=producer,
                        topic=TOPIC,
                        key=event["flow_segment_id"],
                        value=event
                    )
                    if success:
                        msg_count += 1
                        produced_this_cycle += 1
                        if msg_count % 10 == 0:
                            logger.info(f"Produced {msg_count} messages")
                    else:
                        logger.error(f"Failed to send event {event['event_id']}")

                    # Rate limiting
                    time.sleep(interval)

            logger.info(
                f"Completed cycle with {produced_this_cycle} incidents, "
                f"sleeping {POLL_INTERVAL_SECONDS}s..."
            )
            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        flush_producer(producer)
        logger.info(f"Total messages sent: {msg_count}")


if __name__ == "__main__":
    main()
