"""
Dead Letter Queue (DLQ) handler for streaming pipeline.
Routes invalid/failed events to the DLQ topic with error context.
"""
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .kafka_client import create_producer, send_message

logger = logging.getLogger(__name__)

DLQ_TOPIC = os.getenv("KAFKA_TOPIC_DLQ", "streaming.dlq")


class DLQHandler:
    """Handles routing of failed events to Dead Letter Queue."""

    def __init__(self, producer: Optional[Any] = None):
        self.producer = producer or create_producer()
        self.dlq_topic = DLQ_TOPIC

    def send_to_dlq(self, event_id: Optional[str], source_topic: str,
                    error_type: str, error_reason: str,
                    raw_payload: Any) -> bool:
        """
        Send a failed event to the DLQ topic.

        Args:
            event_id: Original event ID (may be None if missing/invalid)
            source_topic: Topic where the event originated
            error_type: Error classification (MALFORMED_JSON, SCHEMA_INVALID, etc.)
            error_reason: Human-readable error description
            raw_payload: Original event payload (dict or raw bytes)

        Returns:
            True if successfully queued for delivery
        """
        dlq_payload = {
            "event_id": event_id,
            "source_topic": source_topic,
            "error_type": error_type,
            "error_reason": error_reason,
            "raw_payload": raw_payload if isinstance(raw_payload, dict)
                          else str(raw_payload),
            "failed_at": datetime.now(timezone.utc).isoformat()
        }
        logger.warning(f"Sending to DLQ [{error_type}]: {error_reason}")
        return send_message(
            producer=self.producer,
            topic=self.dlq_topic,
            key=event_id or error_type,
            value=dlq_payload
        )

    def malformed_json(self, source_topic: str, raw_bytes: bytes) -> bool:
        """Handle malformed JSON error."""
        return self.send_to_dlq(
            event_id=None,
            source_topic=source_topic,
            error_type="MALFORMED_JSON",
            error_reason="Failed to parse JSON payload",
            raw_payload=raw_bytes.decode("utf-8", errors="replace")
        )

    def schema_invalid(self, event_id: Optional[str], source_topic: str,
                       reason: str, payload: Dict) -> bool:
        """Handle schema validation failure."""
        return self.send_to_dlq(
            event_id=event_id,
            source_topic=source_topic,
            error_type="SCHEMA_INVALID",
            error_reason=reason,
            raw_payload=payload
        )

    def missing_field(self, event_id: Optional[str], source_topic: str,
                      field_name: str, payload: Dict) -> bool:
        """Handle missing required field."""
        return self.send_to_dlq(
            event_id=event_id,
            source_topic=source_topic,
            error_type="MISSING_FIELD",
            error_reason=f"Missing required field: {field_name}",
            raw_payload=payload
        )

    def invalid_coordinate(self, event_id: Optional[str], source_topic: str,
                           lat: Any, lon: Any, payload: Dict) -> bool:
        """Handle invalid coordinate values."""
        return self.send_to_dlq(
            event_id=event_id,
            source_topic=source_topic,
            error_type="INVALID_COORDINATE",
            error_reason=f"Invalid coordinates: lat={lat}, lon={lon}",
            raw_payload=payload
        )

    def mapping_failed(self, event_id: Optional[str], source_topic: str,
                       reason: str, payload: Dict) -> bool:
        """Handle enrichment mapping failure."""
        return self.send_to_dlq(
            event_id=event_id,
            source_topic=source_topic,
            error_type="MAPPING_FAILED",
            error_reason=reason,
            raw_payload=payload
        )

    def inference_failed(self, event_id: Optional[str], source_topic: str,
                         reason: str, payload: Dict) -> bool:
        """Handle ML inference failure."""
        return self.send_to_dlq(
            event_id=event_id,
            source_topic=source_topic,
            error_type="INFERENCE_FAILED",
            error_reason=reason,
            raw_payload=payload
        )
