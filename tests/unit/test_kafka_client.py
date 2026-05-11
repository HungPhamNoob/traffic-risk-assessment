"""Configuration tests for the US Kafka replay producer."""

import importlib
import sys
import types
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))


class DummyProducer:
    """Small stand-in so producer configuration can be imported without librdkafka."""

    def __init__(self, *args, **kwargs):
        pass


sys.modules["confluent_kafka"] = types.SimpleNamespace(Producer=DummyProducer)
sys.modules.setdefault("dotenv", types.SimpleNamespace(load_dotenv=lambda: None))


def test_us_producer_uses_replay_path_and_three_producer_split(monkeypatch):
    """Producer should replay the 2020+ US split and support three parallel producers."""
    monkeypatch.setenv(
        "US_PIPELINE_REPLAY_PATH", "gs://bucket/process/us_pipeline_from_2020.csv"
    )
    monkeypatch.setenv("KAFKA_TOPIC_RAW", "traffic.us.raw")
    monkeypatch.setenv("TOTAL_PRODUCERS", "3")
    monkeypatch.setenv("PRODUCER_INDEX", "2")

    module = importlib.import_module("ingestion.kafka.us_producer")
    module = importlib.reload(module)

    assert module.DATA_FILE_PATH == "gs://bucket/process/us_pipeline_from_2020.csv"
    assert module.KAFKA_TOPIC == "traffic.us.raw"
    assert module.TOTAL_PRODUCERS == 3
    assert module.PRODUCER_INDEX == 2
    assert module.should_send_row(2) is True
    assert module.should_send_row(1) is False
