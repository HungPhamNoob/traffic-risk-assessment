"""
Metrics and logging utilities for streaming pipeline.
Provides structured logging and Prometheus metrics (optional).
"""
import logging
import os
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Optional Prometheus metrics
try:
    from prometheus_client import Counter, Histogram, Gauge, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    logger.info("prometheus_client not available, metrics disabled")


class StreamMetrics:
    """
    Metrics collector for streaming pipeline.
    Supports both logging and optional Prometheus metrics.
    """

    def __init__(self, enable_prometheus: bool = False, prometheus_port: int = 8001):
        self.enable_prometheus = enable_prometheus and PROMETHEUS_AVAILABLE
        self.prometheus_port = prometheus_port

        # Initialize counters
        if self.enable_prometheus:
            self.events_in = Counter(
                "stream_events_in_total",
                "Total events read from Kafka",
                ["source"]
            )
            self.events_enriched = Counter(
                "stream_events_enriched_total",
                "Total events successfully enriched",
                ["source"]
            )
            self.events_dlq = Counter(
                "stream_events_dlq_total",
                "Total events sent to DLQ",
                ["error_type"]
            )
            self.predictions_success = Counter(
                "stream_predictions_success_total",
                "Total successful predictions"
            )
            self.predictions_failed = Counter(
                "stream_predictions_failed_total",
                "Total failed predictions"
            )
            self.inference_latency = Histogram(
                "stream_inference_latency_ms",
                "ML inference latency in milliseconds",
                buckets=[10, 50, 100, 200, 500, 1000, 5000]
            )
            self.e2e_latency = Histogram(
                "stream_e2e_latency_ms",
                "End-to-end latency in milliseconds",
                buckets=[100, 500, 1000, 5000, 10000, 30000]
            )
            self.checkpoint_failures = Counter(
                "flink_checkpoint_failed_count",
                "Total checkpoint failures"
            )
            start_http_server(prometheus_port)
            logger.info(f"Prometheus metrics server started on port {prometheus_port}")
        else:
            self.events_in = self._noop_counter
            self.events_enriched = self._noop_counter
            self.events_dlq = self._noop_counter
            self.predictions_success = self._noop_counter
            self.predictions_failed = self._noop_counter
            self.inference_latency = self._noop_histogram
            self.e2e_latency = self._noop_histogram
            self.checkpoint_failures = self._noop_counter

    def _noop_counter(self, *args, **kwargs):
        """No-op counter for when Prometheus is disabled."""
        pass

    def _noop_histogram(self, *args, **kwargs):
        """No-op histogram for when Prometheus is disabled."""
        pass

    def log_event_received(self, source: str = "unknown"):
        """Log and count received event."""
        logger.debug(f"Event received from {source}")
        if self.enable_prometheus:
            self.events_in.labels(source=source).inc()

    def log_event_enriched(self, source: str = "unknown"):
        """Log and count enriched event."""
        logger.debug(f"Event enriched from {source}")
        if self.enable_prometheus:
            self.events_enriched.labels(source=source).inc()

    def log_dlq_event(self, error_type: str):
        """Log and count DLQ event."""
        logger.warning(f"DLQ event: {error_type}")
        if self.enable_prometheus:
            self.events_dlq.labels(error_type=error_type).inc()

    def log_prediction_success(self):
        """Log and count successful prediction."""
        if self.enable_prometheus:
            self.predictions_success.inc()

    def log_prediction_failed(self):
        """Log and count failed prediction."""
        if self.enable_prometheus:
            self.predictions_failed.inc()

    def record_inference_latency(self, latency_ms: float):
        """Record inference latency."""
        if self.enable_prometheus:
            self.inference_latency.observe(latency_ms)

    def record_e2e_latency(self, latency_ms: float):
        """Record end-to-end latency."""
        if self.enable_prometheus:
            self.e2e_latency.observe(latency_ms)

    def log_checkpoint_failure(self):
        """Log and count checkpoint failure."""
        logger.error("Checkpoint failed")
        if self.enable_prometheus:
            self.checkpoint_failures.inc()


def setup_logging(log_level: Optional[str] = None):
    """Configure structured logging for the streaming pipeline."""
    level = getattr(logging, (log_level or os.getenv("STREAMING_LOG_LEVEL", "INFO")).upper())
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    # Set noisy loggers to higher level
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
