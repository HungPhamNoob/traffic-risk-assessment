"""
us_cleaner.py
=============
Clean và chuẩn hóa dữ liệu US Accidents (Kaggle) từ bronze → silver.

Dataset: US-Accidents (Moosavi, 2023)
URL: https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents

Mapping các bước:
  1. Đọc CSV với US_BRONZE_SCHEMA
  2. Rename fields về internal naming convention
  3. Parse timestamps
  4. Cast/convert types
  5. Map Weather_Condition string → numeric weather_code
  6. Derive road_type (junction, amenity heuristics)
  7. Drop rows thiếu lat/lon hoặc event_time
  8. Thêm source="us", event_date partition col
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

logger = logging.getLogger(__name__)

# ── Weather condition string → numeric code mapping ─────────────────────────
# Tham chiếu UK STATS19 weather codes (1-9) để unify với UK data
_US_WEATHER_MAP = {
    "Clear":                      1,
    "Fair":                       1,
    "Mostly Cloudy":              2,
    "Cloudy":                     2,
    "Overcast":                   2,
    "Partly Cloudy":              2,
    "Scattered Clouds":           2,
    "Light Rain":                 3,
    "Rain":                       3,
    "Heavy Rain":                 4,
    "Drizzle":                    3,
    "Light Drizzle":              3,
    "Thunderstorm":               5,
    "Thunder":                    5,
    "T-Storm":                    5,
    "Light Snow":                 6,
    "Snow":                       6,
    "Heavy Snow":                 7,
    "Blowing Snow":               7,
    "Snow Grains":                6,
    "Ice Pellets":                8,
    "Sleet":                      8,
    "Freezing Drizzle":           8,
    "Wintry Mix":                 8,
    "Fog":                        9,
    "Mist":                       9,
    "Haze":                       9,
    "Smoke":                      9,
    "Dust":                       9,
    "Sand":                       9,
    "Blowing Dust":               9,
}


def _build_weather_map_expr() -> F.Column:
    """
    Tạo CASE WHEN expression từ _US_WEATHER_MAP.
    Fallback = 0 (unknown).
    """
    expr = F.when(F.lit(False), F.lit(0))  # khởi tạo chain
    for condition_str, code in _US_WEATHER_MAP.items():
        expr = expr.when(
            F.col("Weather_Condition").contains(condition_str),
            F.lit(code),
        )
    return expr.otherwise(F.lit(0))


def clean_us_accidents(
    spark: SparkSession,
    input_path: str,
    run_date: Optional[date] = None,
) -> DataFrame:
    """
    Clean US Accidents CSV và trả về DataFrame với SILVER_SCHEMA.

    Parameters
    ----------
    spark       : SparkSession
    input_path  : GCS path đến file CSV bronze
                  Ví dụ: "gs://road-accident-data/bronze/raw/us_accidents/*.csv"
    run_date    : Ngày chạy job (để gán event_date partition)

    Returns
    -------
    DataFrame với các cột theo SILVER_SCHEMA
    """
    from processing.spark.utils.schema_definitions import US_BRONZE_SCHEMA

    logger.info("Reading US accidents from: %s", input_path)

    raw = (
        spark.read
        .option("header", "true")
        .option("multiLine", "true")
        .option("escape", '"')
        .schema(US_BRONZE_SCHEMA)
        .csv(input_path)
    )

    logger.info("Raw row count: %d", raw.count())

    # ── 1. Drop các hàng thiếu lat/lon (không có tọa độ → vô dụng) ──────────
    df = raw.filter(
        F.col("Start_Lat").isNotNull()
        & F.col("Start_Lng").isNotNull()
        & (F.col("Start_Lat").between(-90, 90))
        & (F.col("Start_Lng").between(-180, 180))
    )

    # ── 2. Parse timestamp từ string ─────────────────────────────────────────
    # Format: "2016-02-08 05:46:00"
    df = df.withColumn(
        "event_time",
        F.to_timestamp(F.col("Start_Time"), "yyyy-MM-dd HH:mm:ss"),
    )

    # Drop nếu event_time parse thất bại
    df = df.filter(F.col("event_time").isNotNull())

    # ── 3. Weather mapping ───────────────────────────────────────────────────
    df = df.withColumn("weather_code", _build_weather_map_expr())

    # ── 4. Road type heuristic (dựa trên Junction + Traffic_Signal) ──────────
    df = df.withColumn(
        "road_type",
        F.when(F.col("Junction") == "True", "junction")
        .when(F.col("Traffic_Signal") == "True", "traffic_signal")
        .when(F.col("Amenity") == "True", "amenity")
        .otherwise("road"),
    )

    # ── 5. Rename và chọn cột final ─────────────────────────────────────────
    df = df.select(
        F.col("ID").alias("event_id"),
        F.lit("us").alias("source"),
        F.col("event_time"),
        F.col("Start_Lat").alias("lat"),
        F.col("Start_Lng").alias("lon"),
        F.col("Severity").cast(IntegerType()).alias("severity"),
        F.col("weather_code"),
        F.col("road_type"),
        F.col("State").alias("state_or_region"),
        F.col("City").alias("city"),
        F.col("Description").alias("description"),
    )

    # ── 6. Thêm event_date (partition column) ────────────────────────────────
    if run_date is not None:
        df = df.withColumn("event_date", F.lit(run_date.isoformat()))
    else:
        df = df.withColumn(
            "event_date",
            F.date_format(F.col("event_time"), "yyyy-MM-dd"),
        )

    # ── 7. Final null guard ───────────────────────────────────────────────────
    df = df.filter(F.col("event_id").isNotNull())

    clean_count = df.count()
    logger.info("Clean US rows: %d", clean_count)
    return df
