"""
TomTom Hanoi Producer for streaming pipeline.
Fetches traffic data from TomTom API and publishes to Kafka topic 'tomtom.traffic.raw'.
"""
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# Add project root to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from processing.flink.common.kafka_client import create_producer, send_message

logger = logging.getLogger(__name__)

# Configuration
TOPIC = os.getenv("KAFKA_TOPIC_RAW", "tomtom.traffic.raw")
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY", "")
TOMTOM_ENDPOINT = os.getenv(
    "TOMTOM_ENDPOINT",
    "https://api.tomtom.com/traffic/services/4/flowSegmentData.json"
)
POLL_INTERVAL_SECONDS = float(os.getenv("PRODUCER_POLL_INTERVAL", "60"))
THROUGHPUT_PER_SECOND = float(os.getenv("PRODUCER_THROUGHPUT", "10"))

# Hanoi flow segments (sample - extend as needed)
HANOI_SEGMENTS = [
    {"segment_id": "seg_hn_001", "lat": 21.0285, "lon": 105.8542},  # Hoan Kiem
    {"segment_id": "seg_hn_002", "lat": 21.0373, "lon": 105.8497},  # Ba Dinh
    {"segment_id": "seg_hn_003", "lat": 21.0156, "lon": 105.8356},  # Dong Da
    {"segment_id": "seg_hn_004", "lat": 21.0467, "lon": 105.8272},  # Tay Ho
    {"segment_id": "seg_hn_005", "lat": 20.9865, "lon": 105.8607},  # Thanh Xuan
]


def fetch_traffic_data(segment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Fetch traffic data from TomTom API for a given segment."""
    if not TOMTOM_API_KEY:
        logger.warning("No TomTom API key configured, using simulated data")
        return generate_simulated_data(segment)

    try:
        import requests
        params = {
            "key": TOMTOM_API_KEY,
            "point": f"{segment['lat']},{segment['lon']}",
            "unit": "KMPH",
        }
        response = requests.get(TOMTOM_ENDPOINT, params=params, timeout=10)
        if response.status_code == 200:
            return response.json()
        logger.error(f"TomTom API error {response.status_code}: {response.text[:200]}")
    except Exception as e:
        logger.error(f"Failed to fetch from TomTom API: {e}")
    return None


def generate_simulated_data(segment: Dict[str, Any]) -> Dict[str, Any]:
    """Generate simulated traffic data for testing/demo."""
    import random
    return {
        "flowSegmentData": {
            "currentSpeed": random.randint(20, 60),
            "freeFlowSpeed": 50,
            "currentTravelTime": random.randint(60, 300),
            "freeFlowTravelTime": 120,
            "confidence": random.uniform(0.7, 1.0),
            "roadClosure": False,
            "coordinates": {
                "coordinate": [{"latitude": segment["lat"], "longitude": segment["lon"]}]
            }
        }
    }


def create_traffic_event(segment: Dict[str, Any], traffic_data: Dict[str, Any]) -> Dict[str, Any]:
    """Create a normalized traffic event from API response."""
    flow_data = traffic_data.get("flowSegmentData", {})
    coord = flow_data.get("coordinates", {}).get("coordinate", [{}])[0]

    return {
        "event_id": f"evt_{segment['segment_id']}_{int(time.time())}",
        "source": "tomtom",
        "flow_segment_id": segment["segment_id"],
        "latitude": coord.get("latitude", segment["lat"]),
        "longitude": coord.get("longitude", segment["lon"]),
        "speed": flow_data.get("currentSpeed", 0.0),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "raw_payload": traffic_data,
    }


def main() -> None:
    """Main entry point for TomTom Hanoi producer."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    logger.info("=" * 60)
    logger.info("TomTom Hanoi Producer")
    logger.info("=" * 60)
    logger.info(f"Topic: {TOPIC}")
    logger.info(f"API enabled: {bool(TOMTOM_API_KEY)}")
    logger.info(f"Poll interval: {POLL_INTERVAL_SECONDS}s")
    logger.info(f"Throughput: {THROUGHPUT_PER_SECOND} msg/s")

    producer = create_producer()
    msg_count = 0
    interval = 1.0 / max(THROUGHPUT_PER_SECOND, 0.1)  # Sleep between messages

    try:
        while True:
            for segment in HANOI_SEGMENTS:
                # Fetch traffic data
                traffic_data = fetch_traffic_data(segment)
                if not traffic_data:
                    logger.warning(f"No data for segment {segment['segment_id']}")
                    continue

                # Create event
                event = create_traffic_event(segment, traffic_data)

                # Send to Kafka
                success = send_message(
                    producer=producer,
                    topic=TOPIC,
                    key=event["flow_segment_id"],
                    value=event
                )
                if success:
                    msg_count += 1
                    if msg_count % 10 == 0:
                        logger.info(f"Produced {msg_count} messages")
                else:
                    logger.error(f"Failed to send event {event['event_id']}")

                # Rate limiting
                time.sleep(interval)

            logger.info(f"Completed cycle, sleeping {POLL_INTERVAL_SECONDS}s...")
            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        from processing.flink.common.kafka_client import flush_producer
        flush_producer(producer)
        logger.info(f"Total messages sent: {msg_count}")


if __name__ == "__main__":
    main()
