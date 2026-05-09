"""
risk_aggregator.py
==================
Tính toán risk_score per H3 cell dựa trên nhiều yếu tố:
  - accident_count (density)
  - avg_severity
  - weather_risk_weight trung bình
  - temporal risk (rush hour, night ratio)

Formula:
  raw_score = (density_norm * 0.4)
            + (severity_norm * 0.3)
            + (weather_weight_norm * 0.2)
            + (temporal_risk * 0.1)
  risk_score = clip(raw_score, 0.0, 1.0)

Output: h3_index_res8, risk_score, accident_count, avg_severity,
        weather_risk, temporal_risk, centroid_lat, centroid_lon
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

logger = logging.getLogger(__name__)

# ── Trọng số công thức risk ───────────────────────────────────────────────
_W_DENSITY   = 0.40
_W_SEVERITY  = 0.30
_W_WEATHER   = 0.20
_W_TEMPORAL  = 0.10


def compute_risk_scores(
    spark: SparkSession,
    run_date: Optional[date] = None,
) -> DataFrame:
    """
    Tính risk_score per H3 cell (res=8) từ gold features.

    Parameters
    ----------
    spark    : SparkSession
    run_date : Filter theo ngày. None = all.

    Returns
    -------
    DataFrame: h3_index_res8, h3_index_res6, risk_score + sub-scores
    """
    from processing.spark.utils.gcs_utils import read_parquet_gold, write_parquet_gold

    logger.info("Computing risk scores per H3 cell (run_date=%s)", run_date)

    gold_df = read_parquet_gold(spark, "features/accident_features", run_date)

    # ── 1. Aggregate per h3_index_res8 ───────────────────────────────────────
    agg_df = (
        gold_df
        .filter(F.col("h3_index_res8").isNotNull())
        .groupBy("h3_index_res8", "h3_index_res6")
        .agg(
            F.count("event_id").alias("accident_count"),
            F.avg("severity").alias("avg_severity"),
            F.avg("weather_risk_weight").alias("avg_weather_weight"),
            F.avg("is_rush_hour").alias("rush_hour_ratio"),
            F.avg("is_night").alias("night_ratio"),
            F.avg("lat").alias("centroid_lat"),
            F.avg("lon").alias("centroid_lon"),
        )
    )

    # ── 2. Normalize từng component về 0-1 ───────────────────────────────────
    max_count   = agg_df.agg(F.max("accident_count")).collect()[0][0] or 1
    max_weather = 3.0   # _CODE_TO_WEIGHT max = 3.0
    max_severity = 4.0

    agg_df = (
        agg_df
        .withColumn("density_norm",
                    (F.col("accident_count") / F.lit(max_count)).cast(DoubleType()))
        .withColumn("severity_norm",
                    (F.col("avg_severity") / F.lit(max_severity)).cast(DoubleType()))
        .withColumn("weather_norm",
                    # avg_weather_weight / max, clip tới [0,1]
                    F.least(
                        F.lit(1.0),
                        (F.col("avg_weather_weight") / F.lit(max_weather)).cast(DoubleType()),
                    ))
        .withColumn("temporal_risk",
                    # Rush hour ratio weighted 0.6 + night ratio 0.4
                    (F.col("rush_hour_ratio") * F.lit(0.6)
                     + F.col("night_ratio") * F.lit(0.4)).cast(DoubleType()))
    )

    # ── 3. Tính risk_score tổng hợp ──────────────────────────────────────────
    agg_df = agg_df.withColumn(
        "risk_score",
        F.least(
            F.lit(1.0),
            F.greatest(
                F.lit(0.0),
                (
                    F.col("density_norm")   * F.lit(_W_DENSITY)
                    + F.col("severity_norm") * F.lit(_W_SEVERITY)
                    + F.col("weather_norm")  * F.lit(_W_WEATHER)
                    + F.col("temporal_risk") * F.lit(_W_TEMPORAL)
                ).cast(DoubleType()),
            ),
        ),
    )

    # ── 4. Select output ──────────────────────────────────────────────────────
    risk_df = agg_df.select(
        "h3_index_res8",
        "h3_index_res6",
        "centroid_lat",
        "centroid_lon",
        F.col("accident_count"),
        F.col("avg_severity"),
        F.col("avg_weather_weight").alias("weather_risk"),
        F.col("temporal_risk"),
        F.col("density_norm"),
        F.col("risk_score"),
    )

    # ── 5. Ghi vào gold/risk_scores/ ─────────────────────────────────────────
    write_parquet_gold(
        risk_df,
        output_type="risk_scores",
        run_date=run_date,
        mode="overwrite",
    )

    logger.info("Risk scores computed. H3 cells: %d", risk_df.count())
    return risk_df
