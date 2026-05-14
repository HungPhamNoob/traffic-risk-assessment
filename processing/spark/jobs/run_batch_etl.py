"""
run_batch_etl.py
================
Job 1: Bronze → Silver ETL
Airflow gọi: SparkSubmitOperator hoặc PythonOperator

Usage:
    spark-submit processing/spark/jobs/run_batch_etl.py \
        --date 2024-01-15 \
        --source us,uk

    Hoặc từ Python:
        from processing.spark.jobs.run_batch_etl import run
        run(run_date=date(2024, 1, 15), sources=["us", "uk"])
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime
from typing import Optional

# Thêm project root vào sys.path khi chạy spark-submit
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))

from dotenv import load_dotenv
load_dotenv()

from processing.spark.utils.spark_session import get_spark, stop_spark
from processing.spark.utils.gcs_utils import get_bronze_path, write_parquet_silver
from processing.spark.bronze_to_silver.us_cleaner import clean_us_accidents
from processing.spark.bronze_to_silver.uk_cleaner import clean_uk_accidents
from processing.spark.bronze_to_silver.tomtom_cleaner import clean_tomtom_incidents
from processing.spark.bronze_to_silver.schema_enforcer import enforce_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def run(
    run_date: Optional[date] = None,
    sources: Optional[list[str]] = None,
    log_mlflow: bool = False,
) -> dict[str, int]:
    """
    Chạy Bronze → Silver ETL cho tất cả sources.

    Parameters
    ----------
    run_date    : Ngày xử lý. None = lấy ngày hôm nay.
    sources     : ["us", "uk"] mặc định. Có thể chạy thêm "tomtom".
    log_mlflow  : Log metrics lên MLflow không

    Returns
    -------
    dict: {"us": row_count, "uk": row_count}
    """
    run_date = run_date or date.today()
    sources  = sources or ["us", "uk"]

    logger.info(
        "=== Batch ETL Job START | date=%s | sources=%s ===",
        run_date, sources,
    )

    spark = get_spark("BatchETL_BronzeToSilver")
    results: dict[str, int] = {}

    try:
        for src in sources:
            if src == "us":
                input_path = get_bronze_path("us")  # đọc US_BRONZE_PATH hoặc convention
                clean_df   = clean_us_accidents(spark, input_path, run_date)

            elif src == "uk":
                input_path = get_bronze_path("uk")  # đọc UK_BRONZE_PATH hoặc convention
                clean_df   = clean_uk_accidents(spark, input_path, run_date)

            elif src == "tomtom":
                input_path = get_bronze_path("tomtom")
                clean_df   = clean_tomtom_incidents(spark, input_path, run_date)

            else:
                logger.warning("Unknown source: %s — skipping", src)
                continue

            # Validate schema
            valid_df = enforce_schema(
                clean_df,
                source=src,
                quarantine=True,
                log_mlflow=log_mlflow,
                run_date=run_date.isoformat(),
            )

            # Write to silver
            write_parquet_silver(
                valid_df,
                source=f"{src}_accidents",
                run_date=run_date,
                mode="overwrite",
            )

            row_count = valid_df.count()
            results[src] = row_count
            logger.info("Source %s: %d rows written to silver.", src, row_count)

    except Exception as exc:
        logger.exception("Batch ETL FAILED: %s", exc)
        raise
    finally:
        stop_spark(spark)

    logger.info("=== Batch ETL Job DONE | results=%s ===", results)
    return results


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch ETL: Bronze → Silver")
    parser.add_argument(
        "--date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=None,
        help="Run date (YYYY-MM-DD). Default: today.",
    )
    parser.add_argument(
        "--source",
        type=lambda s: [x.strip() for x in s.split(",")],
        default=["us", "uk"],
        help="Comma-separated sources: us,uk,tomtom",
    )
    parser.add_argument("--mlflow", action="store_true", help="Log metrics MLflow")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(run_date=args.date, sources=args.source, log_mlflow=args.mlflow)
