"""
Unit tests for DLQ handler module.
"""
import json
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

if "confluent_kafka" not in sys.modules:
    confluent_kafka = types.ModuleType("confluent_kafka")
    confluent_kafka.Producer = MagicMock
    confluent_kafka.Consumer = MagicMock
    confluent_kafka.KafkaError = Exception
    confluent_kafka_admin = types.ModuleType("confluent_kafka.admin")
    confluent_kafka_admin.AdminClient = MagicMock
    confluent_kafka_admin.NewTopic = MagicMock
    sys.modules["confluent_kafka"] = confluent_kafka
    sys.modules["confluent_kafka.admin"] = confluent_kafka_admin

from processing.flink.common.dlq_handler import DLQHandler


def _delivered_msg(topic="streaming.dlq"):
    msg = MagicMock()
    msg.topic.return_value = topic
    msg.partition.return_value = 0
    return msg


class TestDLQHandler(unittest.TestCase):
    """Test cases for DLQ handler."""

    def setUp(self):
        self.mock_producer = MagicMock()
        self.mock_producer.produce.side_effect = (
            lambda topic, key, value, callback: callback(None, _delivered_msg(topic))
        )
        self.dlq_handler = DLQHandler(producer=self.mock_producer)

    def test_send_to_dlq(self):
        """Test sending event to DLQ."""
        result = self.dlq_handler.send_to_dlq(
            event_id="evt-123",
            source_topic="test.raw",
            error_type="SCHEMA_INVALID",
            error_reason="Missing required field: speed",
            raw_payload={"event_id": "evt-123"}
        )
        self.assertTrue(result)
        self.mock_producer.produce.assert_called_once()

        # Verify payload structure
        call_args = self.mock_producer.produce.call_args
        self.assertEqual(call_args[1]["topic"], "streaming.dlq")
        self.assertEqual(call_args[1]["key"], "evt-123")

    def test_malformed_json(self):
        """Test handling malformed JSON."""
        raw_bytes = b"{invalid json}"
        result = self.dlq_handler.malformed_json("test.raw", raw_bytes)
        self.assertTrue(result)
        self.mock_producer.produce.assert_called()

    def test_missing_field(self):
        """Test handling missing field error."""
        payload = {"event_id": "evt-456", "timestamp": "2024-01-01T00:00:00Z"}
        result = self.dlq_handler.missing_field(
            event_id="evt-456",
            source_topic="test.raw",
            field_name="latitude",
            payload=payload
        )
        self.assertTrue(result)

    def test_invalid_coordinate(self):
        """Test handling invalid coordinate error."""
        payload = {"event_id": "evt-789", "latitude": 999, "longitude": 999}
        result = self.dlq_handler.invalid_coordinate(
            event_id="evt-789",
            source_topic="test.raw",
            lat=999,
            lon=999,
            payload=payload
        )
        self.assertTrue(result)

    def test_mapping_failed(self):
        """Test handling mapping failure."""
        payload = {"event_id": "evt-999"}
        result = self.dlq_handler.mapping_failed(
            event_id="evt-999",
            source_topic="test.raw",
            reason="Could not compute grid cell",
            payload=payload
        )
        self.assertTrue(result)

    def test_inference_failed(self):
        """Test handling inference failure."""
        payload = {"event_id": "evt-000"}
        result = self.dlq_handler.inference_failed(
            event_id="evt-000",
            source_topic="test.enriched",
            reason="Model serving timeout",
            payload=payload
        )
        self.assertTrue(result)

    def test_dlq_payload_structure(self):
        """Test DLQ payload has required fields."""
        payload = {"event_id": "evt-test"}
        self.dlq_handler.send_to_dlq(
            event_id="evt-test",
            source_topic="test.raw",
            error_type="TEST_ERROR",
            error_reason="Test reason",
            raw_payload=payload
        )

        call_args = self.mock_producer.produce.call_args
        sent_value = json.loads(call_args[1]["value"].decode("utf-8"))

        self.assertIn("event_id", sent_value)
        self.assertIn("source_topic", sent_value)
        self.assertIn("error_type", sent_value)
        self.assertIn("error_reason", sent_value)
        self.assertIn("raw_payload", sent_value)
        self.assertIn("failed_at", sent_value)


if __name__ == "__main__":
    unittest.main()
