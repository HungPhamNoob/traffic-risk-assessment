"""
feature_pipeline.py
===================
Orchestrate toàn bộ feature engineering pipeline:
  silver/{us,uk}_accidents/ → gold/features/accident_features/

Thứ tự:
  1. Load từ silver (parquet)
  2. UNION us + uk data
  3. Add temporal features
  4. Enrich weather
  5. Add spatial H3 features
  6. Select gold feature columns
  7. Write Parquet to gold/
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from processing.spark.feature_engineering.temporal_features import add_temporal_features
from processing.spark.feature_engineering.weather_enricher import enrich_weather
from processing.spark.feature_engineering.spatial_features import add_spatial_features
from processing.spark.utils.gcs_utils import (
    read_parquet_silver,
    write_parquet_gold,
)

logger = logging.getLogger(__name__)


def run_feature_pipeline(
    spark: SparkSession,
    run_date: Optional[date] = None,
    sources: Optional[list[str]] = None,
    use_sedona_h3: bool = False,
) -> DataFrame:
    """
    Chạy toàn bộ feature engineering pipeline.

    Parameters
    ----------
    spark         : SparkSession (đã được khởi tạo với Sedona)
    run_date      : Chỉ xử lý partition của ngày này. None = all partitions.
    sources       : ["us", "uk"] mặc định — có thể giới hạn 1 source
    use_sedona_h3 : True → dùng Sedona ST_H3CellIDs thay vì Python UDF

    Returns
    -------
    DataFrame chứa toàn bộ features (đã ghi vào gold)
    """
    sources = sources or ["us", "uk"]

    # ── 1. Load silver data ──────────────────────────────────────────────────
    dfs: list[DataFrame] = []
    for src in sources:
        logger.info("Loading silver data for source: %s", src)
        src_df = read_parquet_silver(spark, f"{src}_accidents", run_date)
        dfs.append(src_df)

    # ── 2. Union all sources ──────────────────────────────────────────────────
    combined = dfs[0]
    for df in dfs[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)

    logger.info("Combined rows before feature engineering: %d", combined.count())

    # ── 3. Temporal features ──────────────────────────────────────────────────
    combined = add_temporal_features(combined)

    # ── 4. Weather enrichment ─────────────────────────────────────────────────
    combined = enrich_weather(combined)

    # ── 5. Spatial H3 features ────────────────────────────────────────────────
    if use_sedona_h3:
        from processing.spark.feature_engineering.spatial_features import (
            add_spatial_features_sedona,
        )
        combined = add_spatial_features_sedona(combined)
    else:
        combined = add_spatial_features(combined)

    # ── 6. Select final gold columns ─────────────────────────────────────────
    gold_cols = [
        "event_id", "source", "event_time", "lat", "lon",
        "severity", "weather_code", "weather_group", "weather_risk_weight",
        "road_type",
        "hour_of_day", "day_of_week", "month", "is_weekend",
        "season", "is_rush_hour", "is_night",
        "h3_index_res8", "h3_index_res6",
        "state_or_region", "city",
        "event_date",
    ]
    existing = [c for c in gold_cols if c in combined.columns]
    gold_df = combined.select(*existing)

    # ── 7. Persist trước khi write (tránh recompute) ─────────────────────────
    gold_df.cache()
    final_count = gold_df.count()
    logger.info("Gold feature rows: %d", final_count)

    # ── 8. Write to gold layer ────────────────────────────────────────────────
    write_parquet_gold(
        gold_df,
        output_type="features/accident_features",
        run_date=run_date,
        mode="overwrite",
        partition_cols=["source", "event_date"],
    )

    return gold_df
