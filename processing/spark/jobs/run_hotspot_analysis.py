"""
run_hotspot_analysis.py
========================
Job 3: Gold → Hotspot Detection + Risk Score + PostGIS Write
Airflow gọi: SparkSubmitOperator hoặc PythonOperator

Mục đích:
  - Đọc gold/features/ đã có đầy đủ temporal + weather + H3 features
  - Chạy hotspot detection (KDE-style qua H3 aggregation + density threshold)
  - Tính risk_score per H3 cell (weighted formula)
  - Ghi kết quả vào:
      * GCS gold/hotspots/ (Parquet)
      * GCS gold/risk_scores/ (Parquet)
      * PostGIS tables: hotspots, risk_score_cells

Usage:
    spark-submit processing/spark/jobs/run_hotspot_analysis.py \
        --date 2024-01-15 \
        --threshold 0.3 \
        --write-postgis

    Hoặc từ Python:
        from processing.spark.jobs.run_hotspot_analysis import run
        run(run_date=date(2024, 1, 15), write_postgis=True)
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
from processing.spark.spatial.hotspot_sedona import run_hotspot_detection
from processing.spark.spatial.risk_aggregator import compute_risk_scores
from processing.spark.spatial.postgis_writer import (
    ensure_tables_exist,
    write_hotspots_to_postgis,
    write_risk_cells_to_postgis,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

_DEFAULT_THRESHOLD = float(os.getenv("HOTSPOT_DENSITY_THRESHOLD", "0.3"))


def run(
    run_date: Optional[date] = None,
    density_threshold: float = _DEFAULT_THRESHOLD,
    write_postgis: bool = True,
    use_sedona: bool = True,
) -> dict[str, int]:
    """
    Chạy Hotspot Analysis + Risk Score computation.

    Parameters
    ----------
    run_date          : Ngày xử lý. None = all data.
    density_threshold : Chỉ giữ H3 cell có density >= threshold (0.0-1.0)
    write_postgis     : True = đẩy kết quả lên PostGIS
    use_sedona        : True = dùng Sedona ST functions cho geometry

    Returns
    -------
    dict: {"hotspots": count, "risk_cells": count}
    """
    run_date = run_date or date.today()

    logger.info(
        "=== Hotspot Analysis Job START | date=%s | threshold=%.2f | postgis=%s ===",
        run_date, density_threshold, write_postgis,
    )

    spark = get_spark("HotspotAnalysis_GoldToPostGIS")
    results: dict[str, int] = {}

    try:
        # ── Step 1: Hotspot detection ────────────────────────────────────────
        logger.info("Step 1/3: Running hotspot detection...")
        hotspot_df = run_hotspot_detection(
            spark,
            run_date=run_date,
            density_threshold=density_threshold,
            use_sedona=use_sedona,
        )
        hotspot_count = hotspot_df.count()
        results["hotspots"] = hotspot_count
        logger.info("Hotspot detection done: %d hotspots found.", hotspot_count)

        # ── Step 2: Risk score per H3 cell ───────────────────────────────────
        logger.info("Step 2/3: Computing risk scores per H3 cell...")
        risk_df = compute_risk_scores(spark, run_date=run_date)
        risk_count = risk_df.count()
        results["risk_cells"] = risk_count
        logger.info("Risk scores computed: %d cells.", risk_count)

        # ── Step 3: Write to PostGIS ─────────────────────────────────────────
        if write_postgis:
            logger.info("Step 3/3: Writing to PostGIS...")
            ensure_tables_exist()
            write_hotspots_to_postgis(hotspot_df)
            write_risk_cells_to_postgis(risk_df)
            logger.info("PostGIS write done.")
        else:
            logger.info("Step 3/3: Skipping PostGIS write (--no-postgis).")

    except Exception as exc:
        logger.exception("Hotspot Analysis FAILED: %s", exc)
        raise
    finally:
        stop_spark(spark)

    logger.info("=== Hotspot Analysis Job DONE | results=%s ===", results)
    return results


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Hotspot Analysis: Gold → Hotspot + Risk Score + PostGIS"
    )
    parser.add_argument(
        "--date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=None,
        help="Run date (YYYY-MM-DD). Default: today.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=_DEFAULT_THRESHOLD,
        help=f"Hotspot density threshold (0.0-1.0). Default: {_DEFAULT_THRESHOLD}",
    )
    parser.add_argument(
        "--no-postgis",
        action="store_true",
        help="Skip writing to PostGIS (only write GCS Parquet).",
    )
    parser.add_argument(
        "--no-sedona",
        action="store_true",
        help="Don't use Sedona ST functions (use H3 Python UDF instead).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(
        run_date=args.date,
        density_threshold=args.threshold,
        write_postgis=not args.no_postgis,
        use_sedona=not args.no_sedona,
    )
