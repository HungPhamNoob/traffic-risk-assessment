"""
Unit tests for Kafka client module.
"""
import json
import os
import unittest
from unittest.mock import MagicMock, patch

from processing.flink.common.kafka_client import (
    create_producer, create_consumer, ensure_topic, send_message
)


class TestKafkaClient(unittest.TestCase):
    """Test cases for Kafka client functions."""

    @patch("processing.flink.common.kafka_client.Producer")
    def test_create_producer(self, mock_producer_class):
        """Test producer creation with defaults."""
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        producer = create_producer()
        self.assertIsNotNone(producer)
        mock_producer_class.assert_called_once()

    @patch("processing.flink.common.kafka_client.Consumer")
    def test_create_consumer(self, mock_consumer_class):
        """Test consumer creation with group ID."""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        consumer = create_consumer("test-group")
        self.assertIsNotNone(consumer)
        mock_consumer_class.assert_called_once()

    @patch("processing.flink.common.kafka_client.AdminClient")
    def test_ensure_topic_exists(self, mock_admin_class):
        """Test topic creation when topic doesn't exist."""
        mock_admin = MagicMock()
        mock_admin_class.return_value = mock_admin

        # Mock metadata to show topic doesn't exist
        mock_metadata = MagicMock()
        mock_metadata.topics = {}
        mock_admin.list_topics.return_value = mock_metadata

        # Mock create_topics future
        mock_future = MagicMock()
        mock_future.result.return_value = None
        mock_admin.create_topics.return_value = {"test-topic": mock_future}

        result = ensure_topic("test-topic")
        self.assertTrue(result)
        mock_admin.create_topics.assert_called_once()

    @patch("processing.flink.common.kafka_client.Producer")
    def test_send_message_success(self, mock_producer_class):
        """Test successful message sending."""
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        test_value = {"event_id": "test-123", "speed": 50.0}
        result = send_message(
            producer=mock_producer,
            topic="test-topic",
            key="test-123",
            value=test_value
        )
        self.assertTrue(result)
        mock_producer.produce.assert_called_once()
        mock_producer.poll.assert_called_once()

    @patch("processing.flink.common.kafka_client.Producer")
    def test_send_message_failure(self, mock_producer_class):
        """Test message sending failure."""
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        mock_producer.produce.side_effect = Exception("Kafka error")

        test_value = {"event_id": "test-123"}
        result = send_message(
            producer=mock_producer,
            topic="test-topic",
            key="test-123",
            value=test_value
        )
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
