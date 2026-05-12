"""
run_feature_engineering.py
==========================
Job 2: Silver → Gold Feature Engineering
Airflow gọi: SparkSubmitOperator hoặc PythonOperator

Mục đích:
  - Đọc dữ liệu đã clean từ silver/ (US + UK)
  - Union 2 nguồn lại
  - Chạy pipeline: temporal_features → weather_enricher → spatial_features
  - Ghi kết quả vào gold/features/accident_features/ (Parquet, partitioned by source+date)

Usage:
    spark-submit processing/spark/jobs/run_feature_engineering.py \
        --date 2024-01-15 \
        --source us,uk \
        --sedona

    Hoặc từ Python:
        from processing.spark.jobs.run_feature_engineering import run
        run(run_date=date(2024, 1, 15))
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
from processing.spark.feature_engineering.feature_pipeline import run_feature_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def run(
    run_date: Optional[date] = None,
    sources: Optional[list[str]] = None,
    use_sedona_h3: bool = False,
) -> int:
    """
    Chạy Feature Engineering pipeline: silver → gold.

    Parameters
    ----------
    run_date      : Ngày xử lý. None = lấy ngày hôm nay.
    sources       : ["us", "uk"] mặc định
    use_sedona_h3 : True = dùng Sedona ST_H3CellIDs (nhanh hơn trên cluster)
                    False = dùng Python h3 UDF (tương thích mọi nơi)

    Returns
    -------
    int : Số row feature đã ghi vào gold layer
    """
    run_date = run_date or date.today()
    sources  = sources or ["us", "uk"]

    logger.info(
        "=== Feature Engineering Job START | date=%s | sources=%s | sedona_h3=%s ===",
        run_date, sources, use_sedona_h3,
    )

    spark = get_spark("FeatureEngineering_SilverToGold")

    try:
        gold_df = run_feature_pipeline(
            spark,
            run_date=run_date,
            sources=sources,
            use_sedona_h3=use_sedona_h3,
        )
        row_count = gold_df.count()
        logger.info("Feature engineering done. Gold rows: %d", row_count)
        return row_count

    except Exception as exc:
        logger.exception("Feature Engineering FAILED: %s", exc)
        raise
    finally:
        stop_spark(spark)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Feature Engineering: Silver → Gold")
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
        help="Comma-separated sources: us,uk",
    )
    parser.add_argument(
        "--sedona",
        action="store_true",
        help="Use Sedona ST_H3CellIDs instead of Python UDF for H3 indexing.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(run_date=args.date, sources=args.source, use_sedona_h3=args.sedona)
