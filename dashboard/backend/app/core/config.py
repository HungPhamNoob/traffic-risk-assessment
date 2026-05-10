"""Environment-backed settings for the dashboard backend."""

from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings loaded from `.env`, `.env.cloud`, or process environment."""

    postgres_user: str = Field(default="capstone", alias="POSTGRES_USER")
    postgres_password: str = Field(default="123", alias="POSTGRES_PASSWORD")
    postgres_db: str = Field(default="capstone_db", alias="POSTGRES_DB")
    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    prediction_table: str = Field(
        default="traffic_risk_predictions",
        alias="POSTGRES_PREDICTION_TABLE",
    )

    kafka_topic_raw: str = Field(default="traffic.us.raw", alias="KAFKA_TOPIC_RAW")
    flink_checkpoint_dir: str = Field(
        default="gs://big-data-group-4-backups/checkpoints/flink",
        alias="FLINK_CHECKPOINT_DIR",
    )
    flink_checkpoint_interval_ms: int = Field(
        default=30000, alias="FLINK_CHECKPOINT_INTERVAL"
    )
    gold_retrain_path: str = Field(
        default="gs://big-data-group-4-gold/features/retrain",
        alias="GOLD_RETRAIN_PATH",
    )
    mlflow_tracking_uri: str = Field(
        default="http://localhost:5000", alias="MLFLOW_TRACKING_URI"
    )
    mlflow_serving_endpoint: str = Field(
        default="http://localhost:5001/invocations",
        alias="MLFLOW_SERVING_ENDPOINT",
    )
    model_name: str = Field(default="traffic-risk-model", alias="ML_MODEL_NAME")
    model_version: str = Field(default="latest", alias="ML_MODEL_VERSION")

    model_config = SettingsConfigDict(
        env_file=(".env.cloud", ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )


@lru_cache
def get_settings() -> Settings:
    """Return cached settings so each request does not reread environment files."""
    return Settings()
