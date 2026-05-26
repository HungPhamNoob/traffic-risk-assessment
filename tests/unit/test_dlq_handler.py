"""Cloud environment consistency tests."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def parse_env_file(path: Path) -> dict[str, str]:
    """Parse simple KEY=VALUE lines from an env file."""
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def test_cloud_env_uses_traffic_project_root_and_us_dataset_only():
    """Cloud deployment should use /opt/traffic and the US replay/training split."""
    config = parse_env_file(PROJECT_ROOT / ".env.cloud")

    assert config["PROJECT_ROOT"] == "/opt/traffic"
    assert config["US_TRAIN_OFFLINE_PATH"].endswith("us_train_offline_before_2020.csv")
    assert config["US_PIPELINE_REPLAY_PATH"].endswith("us_pipeline_from_2020.csv")
    assert config["SPLIT_YEAR"] == "2020"


def test_cloud_env_matches_pipeline_table_and_topic_names():
    """Shared table/topic env values must match the running pipeline contracts."""
    config = parse_env_file(PROJECT_ROOT / ".env.cloud")

    assert config["POSTGRES_PREDICTION_TABLE"] == "traffic_risk_predictions"
    assert config["KAFKA_TOPIC_RAW"] == "traffic.us.raw"
    assert config["KAFKA_TOPIC_TOMTOM_RAW"] == "traffic.tomtom.raw"
    assert {key for key in config if key.startswith("KAFKA_TOPIC_")} == {
        "KAFKA_TOPIC_RAW",
        "KAFKA_TOPIC_TOMTOM_RAW",
    }
    assert config["TOTAL_PRODUCERS"] == "3"
