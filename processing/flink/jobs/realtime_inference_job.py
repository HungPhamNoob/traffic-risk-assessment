"""
Flink Job 2: Real-time ML Inference.
Reads from 'traffic.events.enriched', runs ML inference, writes to 'traffic.risk.predictions'.
"""
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

# Add project root to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))

from processing.flink.common.kafka_client import create_consumer, create_producer, ensure_topic, send_message
from processing.flink.common.ml_client import MLClient, MLClientConfig
from processing.flink.common.metrics import StreamMetrics, setup_logging
from processing.flink.sink_to_postgis import write_prediction

logger = logging.getLogger(__name__)

# Kafka topics
ENRICHED_TOPIC = os.getenv("KAFKA_TOPIC_ENRICHED", "traffic.events.enriched")
PREDICTIONS_TOPIC = os.getenv("KAFKA_TOPIC_PREDICTIONS", "traffic.risk.predictions")
CONSUMER_GROUP = os.getenv("FLINK_INFERENCE_GROUP", "flink-inference")
POSTGIS_SINK_ENABLED = os.getenv("POSTGIS_SINK_ENABLED", "false").lower() == "true"


def run_inference_job():
    """Main inference job logic."""
    logger.info("Starting Flink Job 2: Real-time ML Inference")
    logger.info(f"Reading from: {ENRICHED_TOPIC}")
    logger.info(f"Writing to: {PREDICTIONS_TOPIC}")

    # Ensure topics exist
    ensure_topic(ENRICHED_TOPIC)
    ensure_topic(PREDICTIONS_TOPIC)

    # Initialize ML client and metrics
    ml_config = MLClientConfig()
    ml_client = MLClient(ml_config)
    metrics = StreamMetrics(
        enable_prometheus=os.getenv("PROMETHEUS_ENABLED", "false").lower() == "true"
    )

    # Create Kafka consumer and producer
    consumer = create_consumer(CONSUMER_GROUP)
    consumer.subscribe([ENRICHED_TOPIC])
    producer = create_producer()

    logger.info("Starting inference processing loop...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            metrics.log_event_received("traffic.events.enriched")
            start_time = time.time()

            # Parse enriched event
            try:
                event = json.loads(msg.value())
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse enriched event: {e}")
                continue

            event_id = event.get("event_id", "unknown")
            grid_cell_id = event.get("grid_cell_id", "unknown")

            # Run inference
            prediction = ml_client.predict(event)

            # Record metrics
            latency_ms = (time.time() - start_time) * 1000
            metrics.record_inference_latency(latency_ms)

            if prediction["inference_status"] == "SUCCESS":
                metrics.log_prediction_success()
                logger.debug(f"Inference success for {event_id}: risk_score={prediction['risk_score']}")
            else:
                metrics.log_prediction_failed()
                logger.warning(f"Inference failed for {event_id}: {prediction.get('inference_error')}")

            # Write prediction to output topic using send_message (with delivery tracking)
            success = send_message(
                producer=producer,
                topic=PREDICTIONS_TOPIC,
                key=grid_cell_id,
                value=prediction
            )
            if not success:
                logger.error(f"Failed to write prediction for {event_id}")

            if POSTGIS_SINK_ENABLED and not write_prediction(prediction):
                logger.error(f"Failed to upsert prediction to PostGIS for {event_id}")

            # Update e2e latency if event has timestamp
            event_timestamp = event.get("event_timestamp") or event.get("timestamp")
            if event_timestamp:
                try:
                    dt = datetime.fromisoformat(event_timestamp.replace("Z", "+00:00"))
                    e2e_ms = (datetime.now(timezone.utc) - dt).total_seconds() * 1000
                    metrics.record_e2e_latency(e2e_ms)
                except Exception:
                    pass

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        consumer.close()
        producer.flush()


def main():
    """Entry point for Flink Job 2."""
    setup_logging()
    logger.info("=" * 60)
    logger.info("Flink Job 2: Real-time ML Inference")
    logger.info("=" * 60)

    # Setup Flink environment (for future PyFlink connector usage)
    try:
        from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
        env = StreamExecutionEnvironment.get_execution_environment()
        checkpoint_interval = int(os.getenv("FLINK_CHECKPOINT_INTERVAL", "30000"))
        env.enable_checkpointing(checkpoint_interval, CheckpointingMode.EXACTLY_ONCE)
        logger.info(f"Flink checkpointing enabled: {checkpoint_interval}ms")
    except Exception as e:
        logger.warning(f"PyFlink environment not available: {e}. Using simple loop.")

    # Run inference job
    run_inference_job()
    logger.info("Flink Job 2 completed")


if __name__ == "__main__":
    main()
