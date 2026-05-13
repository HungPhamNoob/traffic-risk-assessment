#!/usr/bin/env python3
"""
Spark Batch Job - Silver to Gold Layer

Purpose:
    Read all Flink-generated feature JSONL files from GCS Silver,
    clean / validate / deduplicate records,
    and write ML-ready Parquet dataset to GCS Gold.

Input:
    gs://big-data-group-4-silver/process/flink_features/

Output:
    gs://big-data-group-4-gold/features/retrain/parquet/
    gs://big-data-group-4-gold/features/retrain/csv/
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    DoubleType,
)

SILVER_PATH = os.getenv(
    "SILVER_FEATURES_PATH",
    "gs://big-data-group-4-silver/process/flink_features",
)

GOLD_RETRAIN_PATH = os.getenv(
    "GOLD_RETRAIN_PATH",
    "gs://big-data-group-4-gold/features/retrain",
)
GOLD_RETRAIN_PARQUET_PATH = os.getenv(
    "GOLD_RETRAIN_PARQUET_PATH",
    f"{GOLD_RETRAIN_PATH.rstrip('/')}/parquet",
)
GOLD_RETRAIN_CSV_PATH = os.getenv(
    "GOLD_RETRAIN_CSV_PATH",
    f"{GOLD_RETRAIN_PATH.rstrip('/')}/csv",
)

WRITE_PARTITIONS = int(os.getenv("SPARK_WRITE_PARTITIONS", "4"))
READ_PARTITIONS = int(os.getenv("SPARK_READ_PARTITIONS", "64"))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger("silver-to-gold")


FEATURE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("event_year", IntegerType(), nullable=False),
        StructField("event_time", StringType(), nullable=True),
        StructField("true_severity", IntegerType(), nullable=True),
        StructField("lat", DoubleType(), nullable=False),
        StructField("lon", DoubleType(), nullable=False),
        StructField("hour", IntegerType(), nullable=True),
        StructField("day_of_week", IntegerType(), nullable=True),
        StructField("is_weekend", IntegerType(), nullable=True),
        StructField("is_rush_hour", IntegerType(), nullable=True),
        StructField("weather_code", IntegerType(), nullable=True),
        StructField("temperature_f", FloatType(), nullable=True),
        StructField("humidity", FloatType(), nullable=True),
        StructField("wind_speed_mph", FloatType(), nullable=True),
        StructField("visibility_mi", FloatType(), nullable=True),
        StructField("road_type_code", IntegerType(), nullable=True),
        StructField("is_junction", IntegerType(), nullable=True),
        StructField("has_traffic_signal", IntegerType(), nullable=True),
        StructField("is_crossing", IntegerType(), nullable=True),
        StructField("is_roundabout", IntegerType(), nullable=True),
        StructField("is_stop", IntegerType(), nullable=True),
        StructField("is_station", IntegerType(), nullable=True),
        StructField("is_railway", IntegerType(), nullable=True),
        StructField("is_night", IntegerType(), nullable=True),
    ]
)

REQUIRED_COLUMNS = [
    "event_id",
    "event_year",
    "true_severity",
    "lat",
    "lon",
]

FILL_DEFAULTS = {
    "hour": 0,
    "day_of_week": 0,
    "is_weekend": 0,
    "is_rush_hour": 0,
    "weather_code": 0,
    "temperature_f": 0.0,
    "humidity": 0.0,
    "wind_speed_mph": 0.0,
    "visibility_mi": 0.0,
    "road_type_code": 0,
    "is_junction": 0,
    "has_traffic_signal": 0,
    "is_crossing": 0,
    "is_roundabout": 0,
    "is_stop": 0,
    "is_station": 0,
    "is_railway": 0,
    "is_night": 0,
}


def main() -> None:
    logger.info("=" * 80)
    logger.info("Spark Silver -> Gold Parquet Job")
    logger.info("Silver path: %s", SILVER_PATH)
    logger.info("Gold Parquet path: %s", GOLD_RETRAIN_PARQUET_PATH)
    logger.info("Gold CSV path:     %s", GOLD_RETRAIN_CSV_PATH)
    logger.info("Read partitions:   %s", READ_PARTITIONS)
    logger.info("Write partitions: %s", WRITE_PARTITIONS)
    logger.info("=" * 80)

    spark = (
        SparkSession.builder.appName("SilverToGoldRetrainDataset")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        logger.info("Reading Silver JSONL recursively...")

        raw_df = (
            spark.read.option("recursiveFileLookup", "true")
            .schema(FEATURE_SCHEMA)
            .json(SILVER_PATH)
        )
        if READ_PARTITIONS > 0:
            raw_df = raw_df.coalesce(READ_PARTITIONS)

        total_raw = raw_df.count()
        logger.info("Raw records read: %s", f"{total_raw:,}")

        if total_raw == 0:
            logger.warning("No Silver data found. Exiting.")
            return

        logger.info("Dropping rows with missing required fields...")
        clean_df = raw_df.dropna(subset=REQUIRED_COLUMNS)

        logger.info("Filtering valid severity, lat, lon...")
        clean_df = (
            clean_df.filter(F.col("true_severity").between(1, 4))
            .filter(F.col("lat").between(-90, 90))
            .filter(F.col("lon").between(-180, 180))
        )

        logger.info("Filling null feature values...")
        clean_df = clean_df.fillna(FILL_DEFAULTS)

        logger.info("Deduplicating by event_id...")
        clean_df = clean_df.dropDuplicates(["event_id"])

        final_count = clean_df.count()
        logger.info("Final clean rows: %s", f"{final_count:,}")

        if final_count == 0:
            logger.warning("No valid rows after cleaning. Exiting.")
            return

        logger.info("Writing Gold Parquet and CSV outputs...")

        partitioned_df = clean_df.repartition(WRITE_PARTITIONS, "event_year")

        partitioned_df.write.mode("overwrite").partitionBy("event_year").parquet(
            GOLD_RETRAIN_PARQUET_PATH
        )
        partitioned_df.write.mode("overwrite").option("header", "true").csv(
            GOLD_RETRAIN_CSV_PATH
        )

        logger.info(
            "Gold Parquet written successfully to %s", GOLD_RETRAIN_PARQUET_PATH
        )
        logger.info("Gold CSV written successfully to %s", GOLD_RETRAIN_CSV_PATH)

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
