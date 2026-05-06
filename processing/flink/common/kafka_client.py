"""
Kafka client module for Flink streaming pipeline.
Provides reusable producer and consumer factories with DLQ support.
"""
import json
import logging
import os
from typing import Any, Dict, Optional

from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)


def _get_config() -> Dict[str, Any]:
    """Load Kafka config from streaming.yaml or environment variables."""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return {
        "bootstrap.servers": bootstrap_servers,
    }


def create_producer(config: Optional[Dict] = None) -> Producer:
    """Create a Kafka producer with sensible defaults."""
    base = _get_config()
    if config:
        base.update(config)
    # Producer-specific defaults
    base.setdefault("acks", "all")
    base.setdefault("retries", 3)
    base.setdefault("batch.size", 16384)
    base.setdefault("linger.ms", 10)
    return Producer(base)


def create_consumer(group_id: str, config: Optional[Dict] = None) -> Consumer:
    """Create a Kafka consumer with sensible defaults."""
    base = _get_config()
    base["group.id"] = group_id
    if config:
        base.update(config)
    # Consumer-specific defaults
    base.setdefault("auto.offset.reset", "latest")
    base.setdefault("enable.auto.commit", False)
    return Consumer(base)


def ensure_topic(topic_name: str, num_partitions: int = 1, replication_factor: int = 1) -> bool:
    """Create a Kafka topic if it doesn't exist."""
    try:
        admin = AdminClient(_get_config())
        metadata = admin.list_topics(timeout=10)
        if topic_name in metadata.topics:
            logger.info(f"Topic '{topic_name}' already exists")
            return True
        # Create topic
        new_topics = [NewTopic(topic_name, num_partitions=num_partitions,
                               replication_factor=replication_factor)]
        fs = admin.create_topics(new_topics)
        for topic, f in fs.items():
            f.result()  # Wait for operation
            logger.info(f"Created topic: {topic}")
        return True
    except Exception as e:
        logger.error(f"Failed to ensure topic '{topic_name}': {e}")
        return False


def send_message(producer: Producer, topic: str, key: Optional[str],
                 value: Dict, timeout: float = 10.0) -> bool:
    """Send a message to Kafka topic with JSON serialization."""
    try:
        def delivery_callback(err: Optional[KafkaError], msg):
            if err:
                logger.error(f"Message delivery failed: {err}")
            else:
                logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

        producer.produce(
            topic=topic,
            key=key,
            value=json.dumps(value).encode("utf-8"),
            callback=delivery_callback
        )
        producer.poll(timeout)
        return True
    except Exception as e:
        logger.error(f"Failed to send message to '{topic}': {e}")
        return False


def flush_producer(producer: Producer, timeout: float = 10.0):
    """Flush the producer to ensure all messages are sent."""
    producer.flush(timeout)
