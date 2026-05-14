"""
Load static road/context features from OpenStreetMap into Redis.

This is a bootstrap step for streaming enrichment. It fetches road-related OSM
elements for configured TomTom bboxes, aggregates them by the same grid id used
by the streaming Enricher, and writes Redis hashes under:

    road:grid:{grid_cell_id}
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import redis
except ImportError:
    redis = None

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from ingestion.kafka.producers.tomtom_producer import get_tomtom_regions
from processing.flink.common.enricher import Enricher, EnrichmentConfig

logger = logging.getLogger(__name__)

OVERPASS_ENDPOINT = os.getenv("OVERPASS_ENDPOINT", "https://overpass-api.de/api/interpreter")
OVERPASS_TIMEOUT_SECONDS = int(os.getenv("OVERPASS_TIMEOUT_SECONDS", "60"))
USER_AGENT = os.getenv("ROAD_LOADER_USER_AGENT", "policy-streaming-road-loader/1.0")


def _redis_config() -> Dict[str, Any]:
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6379"))
    db = int(os.getenv("REDIS_DB", "0"))
    redis_url = os.getenv("REDIS_URL", "").strip()
    if redis_url:
        parsed = urlparse(redis_url)
        host = parsed.hostname or host
        port = parsed.port or port
        if parsed.path and parsed.path.strip("/"):
            db = int(parsed.path.strip("/"))
    return {
        "host": host,
        "port": port,
        "db": db,
        "socket_timeout": float(os.getenv("REDIS_TIMEOUT_MS", "2000")) / 1000.0,
        "decode_responses": True,
    }


def create_redis_client() -> Any:
    if redis is None:
        raise RuntimeError("redis package is not installed")
    client = redis.Redis(**_redis_config())
    client.ping()
    return client


def _bbox_for_overpass(bbox: str) -> str:
    min_lon, min_lat, max_lon, max_lat = [float(part.strip()) for part in bbox.split(",")]
    return f"{min_lat},{min_lon},{max_lat},{max_lon}"


def build_overpass_query(bbox: str) -> str:
    osm_bbox = _bbox_for_overpass(bbox)
    return f"""
    [out:json][timeout:{OVERPASS_TIMEOUT_SECONDS}];
    (
      way["highway"]({osm_bbox});
      node["highway"~"^(traffic_signals|crossing|stop)$"]({osm_bbox});
      node["railway"]({osm_bbox});
      node["public_transport"="station"]({osm_bbox});
      node["railway"="station"]({osm_bbox});
    );
    out tags center;
    """


def fetch_osm_elements(bbox: str) -> List[Dict[str, Any]]:
    response = requests.post(
        OVERPASS_ENDPOINT,
        data={"data": build_overpass_query(bbox)},
        headers={"User-Agent": USER_AGENT},
        timeout=OVERPASS_TIMEOUT_SECONDS + 10,
    )
    response.raise_for_status()
    payload = response.json()
    return payload.get("elements") or []


def _element_lat_lon(element: Dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    if "lat" in element and "lon" in element:
        return float(element["lat"]), float(element["lon"])
    center = element.get("center") or {}
    if "lat" in center and "lon" in center:
        return float(center["lat"]), float(center["lon"])
    return None, None


def _parse_int_tag(value: Any) -> int:
    if value is None:
        return 0
    match = re.search(r"\d+", str(value))
    return int(match.group(0)) if match else 0


def _parse_speed_kmh(value: Any) -> int:
    if value is None:
        return 0
    text = str(value).lower()
    match = re.search(r"\d+", text)
    if not match:
        return 0
    speed = int(match.group(0))
    if "mph" in text:
        return int(round(speed * 1.609344))
    return speed


def road_type_from_highway(highway: Optional[str]) -> str:
    highway = (highway or "").lower()
    if highway in {"motorway", "trunk", "motorway_link", "trunk_link"}:
        return "interstate"
    if highway in {"primary", "secondary", "primary_link", "secondary_link"}:
        return "route"
    if highway in {"residential", "living_street", "pedestrian"}:
        return "street"
    if highway in {"tertiary", "unclassified", "service", "tertiary_link"}:
        return "road"
    return "unknown"


def features_from_osm_tags(tags: Dict[str, Any]) -> Dict[str, Any]:
    highway = tags.get("highway")
    road_type = road_type_from_highway(highway)
    return {
        "road_type": road_type,
        "road_type_code": Enricher._road_type_code(road_type),
        "speed_limit_kmh": _parse_speed_kmh(tags.get("maxspeed")),
        "num_lanes": _parse_int_tag(tags.get("lanes")),
        "has_traffic_signal": 1 if highway == "traffic_signals" else 0,
        "is_junction": 1 if tags.get("junction") else 0,
        "is_crossing": 1 if highway == "crossing" or tags.get("crossing") else 0,
        "is_roundabout": 1 if tags.get("junction") == "roundabout" else 0,
        "is_stop": 1 if highway == "stop" else 0,
        "is_station": 1 if tags.get("public_transport") == "station" or tags.get("railway") == "station" else 0,
        "is_railway": 1 if tags.get("railway") else 0,
    }


def merge_road_features(current: Optional[Dict[str, Any]], incoming: Dict[str, Any]) -> Dict[str, Any]:
    if current is None:
        return dict(incoming)
    merged = dict(current)
    if int(incoming.get("road_type_code", 0)) > int(merged.get("road_type_code", 0)):
        merged["road_type"] = incoming.get("road_type", "unknown")
        merged["road_type_code"] = incoming.get("road_type_code", 0)
    merged["speed_limit_kmh"] = max(int(merged.get("speed_limit_kmh", 0)), int(incoming.get("speed_limit_kmh", 0)))
    merged["num_lanes"] = max(int(merged.get("num_lanes", 0)), int(incoming.get("num_lanes", 0)))
    for key in [
        "has_traffic_signal",
        "is_junction",
        "is_crossing",
        "is_roundabout",
        "is_stop",
        "is_station",
        "is_railway",
    ]:
        merged[key] = 1 if int(merged.get(key, 0)) or int(incoming.get(key, 0)) else 0
    return merged


def aggregate_elements_by_grid(
    elements: Iterable[Dict[str, Any]],
    enricher: Enricher,
) -> Dict[str, Dict[str, Any]]:
    by_grid: Dict[str, Dict[str, Any]] = {}
    for element in elements:
        lat, lon = _element_lat_lon(element)
        if lat is None or lon is None:
            continue
        grid_cell_id = enricher.compute_grid_cell_id(lat, lon)
        if not grid_cell_id:
            continue
        features = features_from_osm_tags(element.get("tags") or {})
        by_grid[grid_cell_id] = merge_road_features(by_grid.get(grid_cell_id), features)
    return by_grid


def write_features_to_redis(client: Any, features_by_grid: Dict[str, Dict[str, Any]], region: str) -> int:
    count = 0
    for grid_cell_id, features in features_by_grid.items():
        payload = {
            **features,
            "region": region,
            "source": "osm",
        }
        key = f"road:grid:{grid_cell_id}"
        client.hset(key, mapping={name: str(value) for name, value in payload.items()})
        count += 1
    return count


def load_region(region: Dict[str, str], client: Any, dry_run: bool = False) -> Dict[str, int]:
    logger.info("Fetching OSM road features for %s (%s)", region["region"], region["bbox"])
    elements = fetch_osm_elements(region["bbox"])
    enricher = Enricher(EnrichmentConfig())
    enricher._redis_client = None
    features_by_grid = aggregate_elements_by_grid(elements, enricher)
    written = 0 if dry_run else write_features_to_redis(client, features_by_grid, region["region"])
    logger.info(
        "Region %s: osm_elements=%d grid_cells=%d redis_written=%d",
        region["region"],
        len(elements),
        len(features_by_grid),
        written,
    )
    return {
        "osm_elements": len(elements),
        "grid_cells": len(features_by_grid),
        "redis_written": written,
    }


def select_regions(country: Optional[str]) -> List[Dict[str, str]]:
    regions = get_tomtom_regions()
    if country:
        regions = [region for region in regions if region.get("country", "").lower() == country.lower()]
    return regions


def main() -> None:
    parser = argparse.ArgumentParser(description="Load OSM road features into Redis for streaming enrichment")
    parser.add_argument("--country", default=os.getenv("ROAD_LOAD_COUNTRY", "US"), help="TomTom region country filter")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and aggregate without writing Redis")
    args = parser.parse_args()

    logging.basicConfig(level=os.getenv("STREAMING_LOG_LEVEL", "INFO"), format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    regions = select_regions(args.country)
    if not regions:
        raise SystemExit(f"No TomTom regions found for country={args.country!r}")

    client = None if args.dry_run else create_redis_client()
    totals = {"osm_elements": 0, "grid_cells": 0, "redis_written": 0}
    for region in regions:
        result = load_region(region, client, dry_run=args.dry_run)
        for key, value in result.items():
            totals[key] += value
    logger.info("Road feature load complete: %s", totals)


if __name__ == "__main__":
    main()
