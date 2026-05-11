#!/usr/bin/env python3
"""
Offline feature preparation for H2O training.

This module converts the before-2020 US Accidents split into the same feature
schema used by streaming inference and Spark retraining. Keeping offline
training on the shared `build_features()` contract prevents schema drift
between pretraining data and realtime replay data.

Input:
    data/split/us_train_offline_before_2020.csv

Output:
    data/process/us_train_offline_before_2020.csv
"""

import csv
import logging
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from processing.feature_engineering import build_features  # noqa: E402


load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("offline-feature-engineering")


INPUT_CSV_PATH = Path(
    os.getenv("OFFLINE_TRAIN_INPUT", "data/split/us_train_offline_before_2020.csv")
)
OUTPUT_CSV_PATH = Path(
    os.getenv("OFFLINE_TRAIN_OUTPUT", "data/process/us_train_offline_before_2020.csv")
)
CHUNK_LOG_INTERVAL = int(os.getenv("OFFLINE_FEATURE_LOG_INTERVAL", "250000"))


FEATURE_COLUMNS = [
    "event_id",
    "event_year",
    "event_time",
    "true_severity",
    "lat",
    "lon",
    "hour",
    "day_of_week",
    "is_weekend",
    "is_rush_hour",
    "weather_code",
    "temperature_f",
    "humidity",
    "wind_speed_mph",
    "visibility_mi",
    "road_type_code",
    "is_junction",
    "has_traffic_signal",
    "is_crossing",
    "is_roundabout",
    "is_stop",
    "is_station",
    "is_railway",
    "is_night",
]


def ordered_feature_row(feature_row: dict[str, Any]) -> dict[str, Any]:
    """Return one feature row with stable column order for CSV output."""
    return {column: feature_row.get(column) for column in FEATURE_COLUMNS}


def prepare_training_features(input_path: Path, output_path: Path) -> int:
    """
    Stream raw CSV rows, build unified features, and write H2O-ready CSV.

    The implementation avoids loading the multi-gigabyte raw split fully into
    memory. Invalid rows are skipped only when shared feature engineering
    rejects missing timestamps, coordinates, identifiers, or severity labels.
    """
    logger.info("=" * 80)
    logger.info("Offline Feature Engineering for H2O Training")
    logger.info("=" * 80)
    logger.info("Input CSV:  %s", input_path)
    logger.info("Output CSV: %s", output_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    processed_count = 0
    written_count = 0
    skipped_count = 0

    with input_path.open("r", encoding="utf-8", newline="") as input_file:
        reader = csv.DictReader(input_file)
        with output_path.open("w", encoding="utf-8", newline="") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=FEATURE_COLUMNS)
            writer.writeheader()

            for raw_row in reader:
                processed_count += 1
                feature_row = build_features(raw_row)
                if feature_row is None:
                    skipped_count += 1
                else:
                    writer.writerow(ordered_feature_row(feature_row))
                    written_count += 1

                if processed_count % CHUNK_LOG_INTERVAL == 0:
                    logger.info(
                        "Processed %s rows, wrote %s rows, skipped %s rows",
                        f"{processed_count:,}",
                        f"{written_count:,}",
                        f"{skipped_count:,}",
                    )

    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    logger.info(
        "Completed offline feature engineering: processed=%s, written=%s, skipped=%s, output_size=%.2f MB",
        f"{processed_count:,}",
        f"{written_count:,}",
        f"{skipped_count:,}",
        file_size_mb,
    )
    logger.info("=" * 80)
    return written_count


def main() -> None:
    """Run the full offline feature preparation pipeline."""
    if not INPUT_CSV_PATH.exists():
        logger.error(
            "Input file not found: %s. Run data splitting before offline feature engineering.",
            INPUT_CSV_PATH,
        )
        raise SystemExit(1)

    written_count = prepare_training_features(INPUT_CSV_PATH, OUTPUT_CSV_PATH)
    if written_count == 0:
        logger.error("No valid training rows were written.")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
