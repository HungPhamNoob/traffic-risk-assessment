"""
Simulated Producer for streaming pipeline.
Replays historical data or generates synthetic events for testing/demo purposes.
"""
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Add project root to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from processing.flink.common.kafka_client import create_producer, send_message

logger = logging.getLogger(__name__)

# Configuration
TOPIC = os.getenv("KAFKA_TOPIC_RAW", "tomtom.traffic.raw")
THROUGHPUT_PER_SECOND = float(os.getenv("SIMULATED_THROUGHPUT", "10"))
REPLAY_PATH = os.getenv("SIMULATED_REPLAY_PATH", "data/sample/")
SOURCE = os.getenv("SIMULATED_SOURCE", "simulated")


def load_sample_data(file_path: str) -> List[Dict[str, Any]]:
    """Load sample events from JSON file."""
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
            return data if isinstance(data, list) else [data]
    except FileNotFoundError:
        logger.warning(f"Sample data file not found: {file_path}")
        return []
    except Exception as e:
        logger.error(f"Failed to load sample data: {e}")
        return []


def generate_synthetic_event(event_id: int) -> Dict[str, Any]:
    """Generate a synthetic traffic event in a US/UK demo area."""
    import random
    if event_id % 2 == 0:
        # New York City
        lat = round(random.uniform(40.60, 40.85), 6)
        lon = round(random.uniform(-74.05, -73.80), 6)
    else:
        # London
        lat = round(random.uniform(51.40, 51.60), 6)
        lon = round(random.uniform(-0.25, 0.10), 6)
    speed = round(random.uniform(10.0, 80.0), 2)
    return {
        "event_id": f"sim_{event_id}_{int(time.time())}",
        "source": SOURCE,
        "flow_segment_id": f"seg_sim_{event_id % 100}",
        "latitude": lat,
        "longitude": lon,
        "speed": speed,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "raw_payload": {"synthetic": True},
    }


def main() -> None:
    """Main entry point for simulated producer."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    logger.info("=" * 60)
    logger.info("Simulated Producer")
    logger.info("=" * 60)
    logger.info(f"Topic: {TOPIC}")
    logger.info(f"Throughput: {THROUGHPUT_PER_SECOND} msg/s")
    logger.info(f"Replay path: {REPLAY_PATH}")

    producer = create_producer()
    msg_count = 0
    interval = 1.0 / max(THROUGHPUT_PER_SECOND, 0.1)

    # Try to load sample data
    sample_events: List[Dict[str, Any]] = []
    if os.path.isdir(REPLAY_PATH):
        for filename in os.listdir(REPLAY_PATH):
            if filename.endswith(".json"):
                events = load_sample_data(os.path.join(REPLAY_PATH, filename))
                sample_events.extend(events)
                logger.info(f"Loaded {len(events)} events from {filename}")

    if sample_events:
        logger.info(f"Total sample events loaded: {len(sample_events)}")
    else:
        logger.info("No sample data found, using synthetic event generation")

    event_idx = 0
    try:
        while True:
            if sample_events:
                # Replay mode
                event = sample_events[event_idx % len(sample_events)].copy()
                event["timestamp"] = datetime.now(timezone.utc).isoformat()
                event["event_id"] = f"replay_{event_idx}_{int(time.time())}"
                event_idx += 1
            else:
                # Synthetic mode
                event = generate_synthetic_event(event_idx)
                event_idx += 1

            # Send to Kafka using send_message (with delivery tracking)
            success = send_message(
                producer=producer,
                topic=TOPIC,
                key=event.get("flow_segment_id", "unknown"),
                value=event
            )
            if success:
                msg_count += 1
                if msg_count % 10 == 0:
                    logger.info(f"Produced {msg_count} messages")
            else:
                logger.error(f"Failed to send event {event.get('event_id')}")

            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        from processing.flink.common.kafka_client import flush_producer
        flush_producer(producer)
        logger.info(f"Total messages sent: {msg_count}")


if __name__ == "__main__":
    main()
