"""
temporal_features.py
====================
Tạo các time-based features từ cột event_time.

Features tạo ra:
  - hour_of_day     : 0-23
  - day_of_week     : 1=Monday .. 7=Sunday
  - month           : 1-12
  - is_weekend      : 1 nếu Sat/Sun, 0 ngược lại
  - season          : "spring" | "summer" | "autumn" | "winter" (Northern Hemisphere)
  - is_rush_hour    : 1 nếu 7-9h hoặc 16-19h, 0 ngược lại
  - is_night        : 1 nếu 22-6h, 0 ngược lại
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

logger = logging.getLogger(__name__)


def add_temporal_features(df: DataFrame, time_col: str = "event_time") -> DataFrame:
    """
    Thêm các temporal features vào DataFrame.

    Parameters
    ----------
    df       : DataFrame có cột event_time (TimestampType)
    time_col : Tên cột timestamp. Mặc định "event_time".

    Returns
    -------
    DataFrame với các cột temporal bổ sung
    """
    logger.info("Adding temporal features from column: %s", time_col)

    df = (
        df
        # ── Basic time parts ────────────────────────────────────────────────
        .withColumn("hour_of_day",  F.hour(F.col(time_col)))
        .withColumn("day_of_week",  F.dayofweek(F.col(time_col)))  # 1=Sun, 7=Sat (Spark default)
        .withColumn("month",        F.month(F.col(time_col)))
        .withColumn("year",         F.year(F.col(time_col)))

        # ── is_weekend: Spark dayofweek 1=Sun, 7=Sat ──────────────────────
        .withColumn(
            "is_weekend",
            F.when(
                F.dayofweek(F.col(time_col)).isin([1, 7]),
                F.lit(1),
            ).otherwise(F.lit(0)).cast(IntegerType()),
        )

        # ── Season (Northern Hemisphere) ──────────────────────────────────
        .withColumn(
            "season",
            F.when(F.month(F.col(time_col)).isin([3, 4, 5]),   F.lit("spring"))
            .when(F.month(F.col(time_col)).isin([6, 7, 8]),    F.lit("summer"))
            .when(F.month(F.col(time_col)).isin([9, 10, 11]),  F.lit("autumn"))
            .otherwise(F.lit("winter")),
        )

        # ── Rush hour: 07:00-09:00 và 16:00-19:00 ────────────────────────
        .withColumn(
            "is_rush_hour",
            F.when(
                F.hour(F.col(time_col)).between(7, 8)
                | F.hour(F.col(time_col)).between(16, 18),
                F.lit(1),
            ).otherwise(F.lit(0)).cast(IntegerType()),
        )

        # ── Nighttime: 22:00 - 05:59 ────────────────────────────────────
        .withColumn(
            "is_night",
            F.when(
                (F.hour(F.col(time_col)) >= 22)
                | (F.hour(F.col(time_col)) <= 5),
                F.lit(1),
            ).otherwise(F.lit(0)).cast(IntegerType()),
        )
    )

    logger.info("Temporal features added: hour_of_day, day_of_week, month, is_weekend, season, is_rush_hour, is_night")
    return df
