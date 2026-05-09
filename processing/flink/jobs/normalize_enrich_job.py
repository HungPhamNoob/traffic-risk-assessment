"""
Flink Job 1: Normalize, Validate, and Enrich traffic events.
Reads from 'tomtom.traffic.raw', validates, enriches, and writes to 'traffic.events.enriched'.
Invalid events are routed to 'streaming.dlq'.
"""
import json
import logging
import os
import sys
from datetime import datetime, timezone

# PyFlink imports
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration

# Add project root to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from processing.flink.common.kafka_client import create_consumer, create_producer, ensure_topic
from processing.flink.common.dlq_handler import DLQHandler
from processing.flink.common.enricher import Enricher, EnrichmentConfig
from processing.flink.common.metrics import StreamMetrics, setup_logging

logger = logging.getLogger(__name__)

# Kafka topics
RAW_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "tomtom.traffic.raw")
ENRICHED_TOPIC = os.getenv("KAFKA_TOPIC_ENRICHED", "traffic.events.enriched")
DLQ_TOPIC = os.getenv("KAFKA_TOPIC_DLQ", "streaming.dlq")
CONSUMER_GROUP = os.getenv("FLINK_CONSUMER_GROUP", "flink-normalize-enrich")


def validate_raw_event(event: dict) -> tuple[bool, str, str]:
    """
    Validate a raw traffic event.
    Returns: (is_valid, error_type, error_reason)
    """
    # Check required fields
    required_fields = ["event_id", "timestamp", "latitude", "longitude"]
    for field in required_fields:
        if field not in event or event[field] is None:
            return False, "MISSING_FIELD", f"Missing or null field: {field}"

    # Validate JSON structure (already parsed, so check types)
    try:
        # Validate coordinates
        lat = float(event.get("latitude", 0))
        lon = float(event.get("longitude", 0))
        if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
            return False, "INVALID_COORDINATE", f"Invalid coordinates: lat={lat}, lon={lon}"
    except (ValueError, TypeError):
        return False, "INVALID_COORDINATE", "Coordinates are not valid numbers"

    try:
        speed = event.get("speed")
        if speed is not None:
            float(speed)
    except (ValueError, TypeError):
        return False, "SCHEMA_INVALID", "Speed is not a valid number"

    return True, "", ""


def normalize_and_validate(stream_env, metrics: StreamMetrics, dlq_handler: DLQHandler):
    """
    Main Flink job logic:
    1. Read from Kafka raw topic
    2. Parse and validate each event
    3. Route invalid events to DLQ
    4. Enrich valid events
    5. Write enriched events to Kafka enriched topic
    """
    # Note: This is a simplified version for local/development.
    # In production, use PyFlink connectors properly.
    logger.info("Starting Flink Job 1: Normalize & Enrich")
    logger.info(f"Reading from: {RAW_TOPIC}")
    logger.info(f"Writing to: {ENRICHED_TOPIC}")
    logger.info(f"DLQ: {DLQ_TOPIC}")

    # Ensure topics exist
    ensure_topic(RAW_TOPIC)
    ensure_topic(ENRICHED_TOPIC)
    ensure_topic(DLQ_TOPIC)

    # Initialize enricher
    enricher = Enricher()

    # Create Kafka consumer and producer (using confluent_kafka for simplicity)
    consumer = create_consumer(CONSUMER_GROUP)
    consumer.subscribe([RAW_TOPIC])
    producer = create_producer()
    dlq = dlq_handler

    logger.info("Starting event processing loop...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            metrics.log_event_received("tomtom.traffic.raw")
            raw_value = msg.value()
            event_id = None

            # Parse JSON
            try:
                event = json.loads(raw_value)
                event_id = event.get("event_id")
            except json.JSONDecodeError as e:
                dlq.malformed_json(RAW_TOPIC, raw_value)
                metrics.log_dlq_event("MALFORMED_JSON")
                continue

            # Validate
            is_valid, error_type, error_reason = validate_raw_event(event)
            if not is_valid:
                dlq.schema_invalid(event_id, RAW_TOPIC, error_reason, event)
                metrics.log_dlq_event(error_type)
                continue

            # Enrich
            enriched = enricher.enrich(event)
            if enriched is None:
                dlq.mapping_failed(event_id, RAW_TOPIC, "Enrichment failed", event)
                metrics.log_dlq_event("MAPPING_FAILED")
                continue

            # Write to enriched topic
            success = producer.produce(
                topic=ENRICHED_TOPIC,
                key=enriched.get("grid_cell_id"),
                value=json.dumps(enriched).encode("utf-8")
            )
            if success:
                metrics.log_event_enriched("traffic.events.enriched")
                logger.debug(f"Enriched event {event_id} -> {enriched.get('grid_cell_id')}")
            else:
                logger.error(f"Failed to write enriched event {event_id}")

            producer.flush()

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        consumer.close()
        producer.flush()


def main():
    """Entry point for Flink Job 1."""
    setup_logging()
    logger.info("=" * 60)
    logger.info("Flink Job 1: Normalize, Validate & Enrich")
    logger.info("=" * 60)

    # Setup metrics
    enable_prometheus = os.getenv("PROMETHEUS_ENABLED", "false").lower() == "true"
    metrics = StreamMetrics(enable_prometheus=enable_prometheus)

    # Setup DLQ handler
    dlq_handler = DLQHandler()

    # Setup Flink environment (for future PyFlink connector usage)
    try:
        env = StreamExecutionEnvironment.get_execution_environment()
        checkpoint_interval = int(os.getenv("FLINK_CHECKPOINT_INTERVAL", "30000"))
        env.enable_checkpointing(checkpoint_interval, CheckpointingMode.EXACTLY_ONCE)
        logger.info(f"Flink checkpointing enabled: {checkpoint_interval}ms")
    except Exception as e:
        logger.warning(f"PyFlink environment not available: {e}. Using simple loop.")

    # Run processing
    normalize_and_validate(env, metrics, dlq_handler)
    logger.info("Flink Job 1 completed")


if __name__ == "__main__":
    main()
