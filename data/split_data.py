#!/usr/bin/env python3
"""
Split the raw US accident dataset into:
1. offline training data: records before the split year
2. pipeline replay data: records from the split year onward

Default split year:
    2020

Input:
    data/raw/US.csv

Output:
    data/process/us_train_offline_before_2020.csv
    data/process/us_pipeline_from_2020.csv

Important:
    The input CSV files do NOT need to be sorted by year.
    This script reads data in chunks and splits each row based on its year.
"""

from pathlib import Path
import logging
import os

import pandas as pd


LOG_LEVEL = os.getenv("SPLIT_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ============================================================
# Configuration
# ============================================================

SPLIT_YEAR = 2020
CHUNK_SIZE = 25_000

RAW_DIR = Path("data/raw")
PROCESS_DIR = Path("data/process")

US_INPUT_PATH = RAW_DIR / "US.csv"

US_TRAIN_OUTPUT_PATH = PROCESS_DIR / f"us_train_offline_before_{SPLIT_YEAR}.csv"
US_PIPELINE_OUTPUT_PATH = PROCESS_DIR / f"us_pipeline_from_{SPLIT_YEAR}.csv"


# ============================================================
# Helper functions
# ============================================================


def ensure_output_folder_exists() -> None:
    """Create the output folder if it does not already exist."""
    PROCESS_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Output folder ready: %s", PROCESS_DIR)


def remove_old_output_files() -> None:
    """
    Remove old split files before writing new files.

    This avoids accidentally appending new data to old output files.
    """
    output_paths = [
        US_TRAIN_OUTPUT_PATH,
        US_PIPELINE_OUTPUT_PATH,
    ]

    for output_path in output_paths:
        if output_path.exists():
            output_path.unlink()
            logger.info("Removed old output file: %s", output_path)


def validate_input_file_exists(input_path: Path) -> None:
    """Stop the script early if an input file is missing."""
    if not input_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {input_path}. "
            "Please make sure your raw CSV files are placed inside data/raw/."
        )


def append_chunk_to_csv(
    chunk: pd.DataFrame, output_path: Path, write_header: bool
) -> None:
    """
    Append one chunk to a CSV file.

    Parameters:
        chunk:
            The DataFrame chunk to write.

        output_path:
            Destination CSV path.

        write_header:
            True only when writing the first chunk to that output file.
    """
    chunk.to_csv(
        output_path,
        mode="a",
        index=False,
        header=write_header,
    )


# ============================================================
# US splitting logic
# ============================================================


def split_us_dataset() -> None:
    """
    Split US.csv by year.

    US year column is extracted from:
        Start_Time

    Rule:
        Start_Time year < 2020  -> offline training data
        Start_Time year >= 2020 -> pipeline replay data
    """
    logger.info("=" * 80)
    logger.info("Starting US dataset split")
    logger.info("=" * 80)

    validate_input_file_exists(US_INPUT_PATH)

    total_rows = 0
    train_rows = 0
    pipeline_rows = 0
    invalid_time_rows = 0

    has_written_train_header = False
    has_written_pipeline_header = False

    for chunk_index, chunk in enumerate(
        pd.read_csv(US_INPUT_PATH, chunksize=CHUNK_SIZE, low_memory=False),
        start=1,
    ):
        logger.info(
            "Processing US chunk %s with %s rows",
            chunk_index,
            f"{len(chunk):,}",
        )

        # Convert Start_Time to datetime.
        # Invalid values become NaT, so they can be safely detected and removed.
        parsed_start_time = pd.to_datetime(
            chunk["Start_Time"],
            errors="coerce",
        )

        year_series = parsed_start_time.dt.year

        valid_time_mask = year_series.notna()
        invalid_count = int((~valid_time_mask).sum())

        if invalid_count > 0:
            invalid_time_rows += invalid_count
            logger.warning(
                "US chunk %s: %s rows have invalid Start_Time and will be skipped",
                chunk_index,
                f"{invalid_count:,}",
            )

        valid_chunk = chunk.loc[valid_time_mask].copy()
        valid_years = year_series.loc[valid_time_mask]

        train_chunk = valid_chunk.loc[valid_years < SPLIT_YEAR]
        pipeline_chunk = valid_chunk.loc[valid_years >= SPLIT_YEAR]

        if not train_chunk.empty:
            append_chunk_to_csv(
                train_chunk,
                US_TRAIN_OUTPUT_PATH,
                write_header=not has_written_train_header,
            )
            has_written_train_header = True

        if not pipeline_chunk.empty:
            append_chunk_to_csv(
                pipeline_chunk,
                US_PIPELINE_OUTPUT_PATH,
                write_header=not has_written_pipeline_header,
            )
            has_written_pipeline_header = True

        total_rows += len(chunk)
        train_rows += len(train_chunk)
        pipeline_rows += len(pipeline_chunk)

        logger.info(
            "US progress: total=%s, train_before_%s=%s, pipeline_from_%s=%s, invalid_time=%s",
            f"{total_rows:,}",
            SPLIT_YEAR,
            f"{train_rows:,}",
            SPLIT_YEAR,
            f"{pipeline_rows:,}",
            f"{invalid_time_rows:,}",
        )

    logger.info("US split completed")
    logger.info("US total rows read: %s", f"{total_rows:,}")
    logger.info("US offline training rows: %s", f"{train_rows:,}")
    logger.info("US pipeline replay rows: %s", f"{pipeline_rows:,}")
    logger.info("US invalid Start_Time rows skipped: %s", f"{invalid_time_rows:,}")
    logger.info("US training output: %s", US_TRAIN_OUTPUT_PATH)
    logger.info("US pipeline output: %s", US_PIPELINE_OUTPUT_PATH)


# ============================================================
# Main entry point
# ============================================================


def main() -> None:
    logger.info("=" * 80)
    logger.info("Accident dataset split job started")
    logger.info("Split year: %s", SPLIT_YEAR)
    logger.info("Rows before %s will be used for offline training", SPLIT_YEAR)
    logger.info("Rows from %s onward will be used for pipeline replay", SPLIT_YEAR)
    logger.info("Input files do not need to be sorted by year")
    logger.info("=" * 80)

    ensure_output_folder_exists()
    remove_old_output_files()

    split_us_dataset()

    logger.info("=" * 80)
    logger.info("Dataset split job completed successfully")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
