"""
ML inference client for streaming pipeline.
Supports async HTTP calls to MLflow REST API for real-time predictions.
"""
import json
import logging
import os
import time
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)


class MLClientConfig:
    """Configuration for ML inference client."""

    def __init__(self):
        self.endpoint = os.getenv(
            "MLFLOW_SERVING_ENDPOINT",
            "http://localhost:5000/invocations"
        )
        self.timeout_seconds = float(os.getenv("ML_TIMEOUT_SECONDS", "5"))
        self.fallback_risk_score = float(os.getenv("ML_FALLBACK_RISK_SCORE", "-1"))
        self.fallback_risk_level = None  # null in JSON
        self.fallback_status = "FAILED"
        # Model metadata
        self.model_name = os.getenv("ML_MODEL_NAME", "risk-predictor")
        self.model_version = os.getenv("ML_MODEL_VERSION", None)


class MLClient:
    """
    Client for ML model inference.
    Uses REST API calls to MLflow serving endpoint.
    """

    def __init__(self, config: Optional[MLClientConfig] = None):
        self.config = config or MLClientConfig()
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json"
        })

    def _build_feature_vector(self, enriched_event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build feature vector from enriched event for model inference.
        Adjust based on your model's expected input schema.
        """
        return {
            "speed": enriched_event.get("speed", 0.0),
            "road_type": enriched_event.get("road_type"),
            "speed_limit_kmh": enriched_event.get("speed_limit_kmh", 0),
            "num_lanes": enriched_event.get("num_lanes", 0),
            "has_traffic_signal": enriched_event.get("has_traffic_signal", False),
            "temperature_c": enriched_event.get("temperature_c", 0.0),
            "visibility_km": enriched_event.get("visibility_km", 0.0),
            "precipitation_mm": enriched_event.get("precipitation_mm", 0.0),
            "is_rush_hour": enriched_event.get("is_rush_hour", False),
            "hour_of_day": enriched_event.get("hour_of_day", 0),
            "day_of_week": enriched_event.get("day_of_week", 0),
            "season": enriched_event.get("season", "unknown"),
        }

    def predict(self, enriched_event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run inference on an enriched event.
        Returns prediction result with metadata.

        Output schema:
        {
            "event_id": str,
            "grid_cell_id": str,
            "event_timestamp": str,
            "risk_score": float,
            "risk_level": int or null,
            "is_high_risk": bool,
            "model_name": str,
            "model_version": str or null,
            "inference_status": "SUCCESS|FAILED|UNKNOWN",
            "inference_error": str or null,
            "scored_at": str (ISO-8601)
        }
        """
        event_id = enriched_event.get("event_id", "unknown")
        grid_cell_id = enriched_event.get("grid_cell_id", "unknown")
        event_timestamp = enriched_event.get("event_timestamp") or enriched_event.get("timestamp", "")

        result = {
            "event_id": event_id,
            "grid_cell_id": grid_cell_id,
            "event_timestamp": event_timestamp,
            "risk_score": self.config.fallback_risk_score,
            "risk_level": self.config.fallback_risk_level,
            "is_high_risk": False,
            "model_name": self.config.model_name,
            "model_version": self.config.model_version,
            "inference_status": self.config.fallback_status,
            "inference_error": None,
            "scored_at": "",
        }

        try:
            features = self._build_feature_vector(enriched_event)
            payload = {
                "dataframe_split": {
                    "columns": list(features.keys()),
                    "data": [list(features.values())]
                }
            }

            start_time = time.time()
            response = self.session.post(
                self.config.endpoint,
                json=payload,
                timeout=self.config.timeout_seconds
            )
            latency_ms = (time.time() - start_time) * 1000

            if response.status_code == 200:
                prediction = response.json()
                # Parse prediction - format depends on MLflow model output
                # Assuming output is a list with risk_score
                if isinstance(prediction, list) and len(prediction) > 0:
                    risk_score = float(prediction[0])
                elif isinstance(prediction, dict):
                    risk_score = float(prediction.get("prediction", prediction.get("risk_score", -1)))
                else:
                    risk_score = float(prediction)

                result["risk_score"] = risk_score
                result["risk_level"] = self._compute_risk_level(risk_score)
                result["is_high_risk"] = risk_score >= 0.7
                result["inference_status"] = "SUCCESS"
                logger.debug(f"Inference success for {event_id}: risk_score={risk_score} ({latency_ms:.1f}ms)")
            else:
                error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
                result["inference_error"] = error_msg
                logger.warning(f"Inference failed for {event_id}: {error_msg}")

        except requests.Timeout:
            result["inference_error"] = f"Timeout after {self.config.timeout_seconds}s"
            logger.warning(f"Inference timeout for {event_id}")
        except Exception as e:
            result["inference_error"] = str(e)[:200]
            logger.error(f"Inference error for {event_id}: {e}")

        result["scored_at"] = self._now_iso()
        return result

    def _compute_risk_level(self, risk_score: float) -> int:
        """Convert risk score to discrete risk level (1-5)."""
        if risk_score < 0:
            return 0  # Unknown/error
        if risk_score < 0.2:
            return 1
        if risk_score < 0.4:
            return 2
        if risk_score < 0.6:
            return 3
        if risk_score < 0.8:
            return 4
        return 5

    @staticmethod
    def _now_iso() -> str:
        """Get current UTC time in ISO-8601 format."""
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()
