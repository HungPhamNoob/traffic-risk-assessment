"""Dedicated TomTom Kafka consumer for near-realtime PostgreSQL updates.

This sidecar reuses the existing TomTom enrichment + upsert logic without
restarting the unified US replay Flink job. It force-flushes buffered TomTom
rows on a short interval so the dashboard stays live even when TomTom volume is
much lower than the US replay throughput.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time

from confluent_kafka import Consumer, KafkaException

from processing.flink_streaming import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_TOMTOM_RAW,
    flush_tomtom_batch,
    initialize_schemas,
    process_tomtom_message,
)


logging.basicConfig(
    level=getattr(logging, os.getenv("STREAMING_LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("tomtom-realtime-consumer")

CONSUMER_GROUP = os.getenv(
    "TOMTOM_CONSUMER_GROUP",
    os.getenv("FLINK_TOMTOM_GROUP", "tomtom-live-postgres-consumer"),
)
AUTO_OFFSET_RESET = os.getenv("TOMTOM_CONSUMER_AUTO_OFFSET_RESET", "earliest")
POLL_TIMEOUT_SECONDS = float(os.getenv("TOMTOM_CONSUMER_POLL_TIMEOUT_SECONDS", "1.0"))
FLUSH_INTERVAL_SECONDS = float(os.getenv("TOMTOM_FLUSH_INTERVAL_SECONDS", "15"))

RUNNING = True


def _handle_signal(signum, _frame) -> None:
    global RUNNING
    logger.info("Received signal %s, flushing TomTom buffer before shutdown.", signum)
    RUNNING = False


def build_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": AUTO_OFFSET_RESET,
            "enable.auto.commit": True,
            "client.id": "tomtom-realtime-consumer",
        }
    )


def main() -> int:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    logger.info("Starting dedicated TomTom realtime consumer")
    logger.info("Kafka bootstrap: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Topic: %s", KAFKA_TOPIC_TOMTOM_RAW)
    logger.info("Group: %s", CONSUMER_GROUP)
    logger.info("Flush interval: %.1fs", FLUSH_INTERVAL_SECONDS)

    initialize_schemas()
    consumer = build_consumer()
    consumer.subscribe([KAFKA_TOPIC_TOMTOM_RAW])

    last_flush_at = time.monotonic()
    processed_since_flush = 0

    try:
        while RUNNING:
            message = consumer.poll(POLL_TIMEOUT_SECONDS)
            now = time.monotonic()

            if message is None:
                if now - last_flush_at >= FLUSH_INTERVAL_SECONDS:
                    flush_tomtom_batch()
                    last_flush_at = now
                    processed_since_flush = 0
                continue

            if message.error():
                raise KafkaException(message.error())

            raw_value = message.value()
            if raw_value is None:
                continue

            if isinstance(raw_value, bytes):
                raw_message = raw_value.decode("utf-8", errors="replace")
            else:
                raw_message = str(raw_value)

            result = process_tomtom_message(raw_message)
            processed_since_flush += 1

            if result.startswith("TOMTOM_FAIL"):
                logger.warning("TomTom processing failure: %s", result)

            if now - last_flush_at >= FLUSH_INTERVAL_SECONDS:
                flush_tomtom_batch()
                logger.info("Flushed TomTom live buffer after %d messages.", processed_since_flush)
                last_flush_at = now
                processed_since_flush = 0
    except KeyboardInterrupt:
        logger.info("Interrupted by keyboard signal.")
    finally:
        try:
            flush_tomtom_batch()
        finally:
            consumer.close()
        logger.info("TomTom realtime consumer stopped cleanly.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
