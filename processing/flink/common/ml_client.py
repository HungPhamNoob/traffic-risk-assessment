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
        Build the realtime feature vector from the enriched event.
        This matches docs/streaming/feature/feature_output.md and excludes
        metadata/label fields such as event_id, event_time, and true_severity.
        """
        return {
            "lat": enriched_event.get("lat", enriched_event.get("latitude", 0.0)),
            "lon": enriched_event.get("lon", enriched_event.get("longitude", 0.0)),
            "hour": enriched_event.get("hour", enriched_event.get("hour_of_day", 0)),
            "day_of_week": enriched_event.get("day_of_week", 0),
            "is_weekend": enriched_event.get("is_weekend", 0),
            "is_rush_hour": enriched_event.get("is_rush_hour", 0),
            "weather_code": int(enriched_event.get("weather_code", 0) or 0),
            "temperature_f": enriched_event.get("temperature_f", 50.0),
            "humidity": enriched_event.get("humidity", 50.0),
            "wind_speed_mph": enriched_event.get("wind_speed_mph", 0.0),
            "visibility_mi": enriched_event.get("visibility_mi", 10.0),
            "road_type_code": enriched_event.get("road_type_code", 0),
            "is_junction": enriched_event.get("is_junction", 0),
            "has_traffic_signal": enriched_event.get("has_traffic_signal", 0),
            "is_crossing": enriched_event.get("is_crossing", 0),
            "is_roundabout": enriched_event.get("is_roundabout", 0),
            "is_stop": enriched_event.get("is_stop", 0),
            "is_station": enriched_event.get("is_station", 0),
            "is_railway": enriched_event.get("is_railway", 0),
            "is_night": enriched_event.get("is_night", 0),
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

        result = dict(enriched_event)
        result.update({
            "event_id": event_id,
            "grid_cell_id": grid_cell_id,
            "event_timestamp": event_timestamp,
            "risk_score": self.config.fallback_risk_score,
            "risk_level": self.config.fallback_risk_level,
            "predicted_severity": 0,
            "is_high_risk": False,
            "model_name": self.config.model_name,
            "model_version": self.config.model_version,
            "inference_status": self.config.fallback_status,
            "model_status": self.config.fallback_status,
            "inference_error": None,
            "inference_latency_ms": 0.0,
            "scored_at": "",
            "prediction_timestamp": "",
        })

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
            result["inference_latency_ms"] = latency_ms

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
                result["predicted_severity"] = self._compute_predicted_severity(risk_score)
                result["is_high_risk"] = risk_score >= 0.7
                result["inference_status"] = "SUCCESS"
                result["model_status"] = "SUCCESS"
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

        scored_at = self._now_iso()
        result["scored_at"] = scored_at
        result["prediction_timestamp"] = scored_at
        result["end_to_end_latency_ms"] = self._compute_e2e_latency_ms(
            event_timestamp,
            scored_at,
        )
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

    def _compute_predicted_severity(self, risk_score: float) -> int:
        """Convert model risk score to the shared 1-4 severity scale."""
        if risk_score < 0:
            return 0
        if risk_score < 0.25:
            return 1
        if risk_score < 0.5:
            return 2
        if risk_score < 0.75:
            return 3
        return 4

    @staticmethod
    def _compute_e2e_latency_ms(event_timestamp: str, scored_at: str) -> float:
        if not event_timestamp:
            return 0.0
        try:
            from datetime import datetime
            start = datetime.fromisoformat(event_timestamp.replace("Z", "+00:00"))
            end = datetime.fromisoformat(scored_at.replace("Z", "+00:00"))
            return max(0.0, (end - start).total_seconds() * 1000)
        except Exception:
            return 0.0

    @staticmethod
    def _now_iso() -> str:
        """Get current UTC time in ISO-8601 format."""
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()
