"""
gcs_utils.py
============
Helper functions để đọc/ghi dữ liệu lên GCS theo Data Lake layers.

Conventions:
  bronze : gs://{GCS_BRONZE_BUCKET}/{tên-file-hoặc-folder}
  silver : gs://{GCS_SILVER_BUCKET}/enriched/{source}/date={date}/
  gold   : gs://{GCS_GOLD_BUCKET}/{output_type}/
"""
from __future__ import annotations

import os
import logging
from datetime import date, datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

# ── 3 bucket riêng biệt cho từng layer — đặt trong .env ─────────────────────
# Ví dụ .env:
#   GCS_BRONZE_BUCKET=big-data-group-4-bronze
#   GCS_SILVER_BUCKET=big-data-group-4-silver
#   GCS_GOLD_BUCKET=big-data-group-4-gold
_BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET", "")
_SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET", "")
_GOLD_BUCKET   = os.getenv("GCS_GOLD_BUCKET", "")

# ── Đường dẫn trực tiếp đến file CSV bronze (override nếu file không theo convention) ──
# Ví dụ:
#   UK_BRONZE_PATH=gs://big-data-group-4-bronze/Road Safety Data - Collisions - last 5 years.csv
#   US_BRONZE_PATH=gs://big-data-group-4-bronze/us_accidents.csv
_UK_BRONZE_PATH = os.getenv("UK_BRONZE_PATH", "")
_US_BRONZE_PATH = os.getenv("US_BRONZE_PATH", "")


# ─── Path builders ────────────────────────────────────────────────────────────

def bronze_path(source: str, filename: str = "*") -> str:
    """
    Đường dẫn convention trong bronze bucket: gs://{GCS_BRONZE_BUCKET}/{source}/{filename}
    Dùng khi file được tổ chức theo thư mục. Hiện tại ưu tiên UK_BRONZE_PATH/US_BRONZE_PATH.
    """
    return f"gs://{_BRONZE_BUCKET}/{source}/{filename}"


def get_bronze_path(source: str) -> str:
    """
    Lấy đường dẫn thực tế của file bronze, ưu tiên env var override.

    Logic:
      1. Nếu có UK_BRONZE_PATH / US_BRONZE_PATH → dùng thẳng (bất kể tên file)
      2. Nếu không → dùng convention path gs://{GCS_BRONZE_BUCKET}/{source}_accidents/*.csv

    Parameters
    ----------
    source : "uk" | "us"

    Ví dụ đặt env:
      UK_BRONZE_PATH=gs://big-data-group-4-bronze/Road Safety Data - Collisions - last 5 years.csv
    """
    if source == "uk" and _UK_BRONZE_PATH:
        logger.info("Using UK_BRONZE_PATH override: %s", _UK_BRONZE_PATH)
        return _UK_BRONZE_PATH
    if source == "us" and _US_BRONZE_PATH:
        logger.info("Using US_BRONZE_PATH override: %s", _US_BRONZE_PATH)
        return _US_BRONZE_PATH
    # Convention path fallback
    convention = bronze_path(f"{source}_accidents", "*.csv")
    logger.info("Using convention bronze path: %s", convention)
    return convention


def silver_path(source: str, run_date: Optional[date] = None) -> str:
    """
    Đường dẫn vùng silver: gs://{GCS_SILVER_BUCKET}/enriched/{source}/[date={date}/]
    Ví dụ: gs://big-data-group-4-silver/enriched/uk_accidents/date=2024-01-15/
    """
    if run_date is None:
        return f"gs://{_SILVER_BUCKET}/enriched/{source}/"
    return f"gs://{_SILVER_BUCKET}/enriched/{source}/date={run_date.isoformat()}/"


def gold_path(output_type: str, run_date: Optional[date] = None) -> str:
    """
    Đường dẫn vùng gold: gs://{GCS_GOLD_BUCKET}/{output_type}/[date={date}/]
    Ví dụ: gs://big-data-group-4-gold/features/accident_features/
    """
    base = f"gs://{_GOLD_BUCKET}/{output_type}/"
    if run_date:
        base += f"date={run_date.isoformat()}/"
    return base


def checkpoint_path(job_name: str) -> str:
    """Checkpoint dir trên silver bucket."""
    return f"gs://{_SILVER_BUCKET}/checkpoints/spark/{job_name}/"


def mlflow_artifact_path() -> str:
    """Thư mục artifact MLflow trên gold bucket."""
    return f"gs://{_GOLD_BUCKET}/mlflow-artifacts/"


# ─── Readers ──────────────────────────────────────────────────────────────────

def read_csv_bronze(
    spark: SparkSession,
    source: str,
    filename: str = "*.csv",
    header: bool = True,
    infer_schema: bool = False,
) -> DataFrame:
    """
    Đọc CSV từ bronze layer.

    Parameters
    ----------
    source      : "us_accidents" | "uk_accidents"
    filename    : glob pattern, mặc định *.csv
    header      : CSV có header dòng đầu không
    infer_schema: False → mọi cột sẽ là string (an toàn hơn)
    """
    path = bronze_path(source, filename)
    logger.info("Reading CSV from bronze: %s", path)
    return (
        spark.read
        .option("header", str(header).lower())
        .option("inferSchema", str(infer_schema).lower())
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(path)
    )


def read_parquet_silver(
    spark: SparkSession,
    source: str,
    run_date: Optional[date] = None,
) -> DataFrame:
    """Đọc Parquet từ silver layer (có thể filter theo date partition)."""
    path = silver_path(source, run_date)
    logger.info("Reading Parquet from silver: %s", path)
    return spark.read.parquet(path)


def read_parquet_gold(
    spark: SparkSession,
    output_type: str,
    run_date: Optional[date] = None,
) -> DataFrame:
    """Đọc Parquet từ gold layer."""
    path = gold_path(output_type, run_date)
    logger.info("Reading Parquet from gold: %s", path)
    return spark.read.parquet(path)


# ─── Writers ──────────────────────────────────────────────────────────────────

def write_parquet_silver(
    df: DataFrame,
    source: str,
    run_date: Optional[date] = None,
    mode: str = "overwrite",
    partition_cols: Optional[list[str]] = None,
) -> None:
    """
    Ghi Parquet vào silver layer.

    Parameters
    ----------
    mode            : "overwrite" (mặc định) hoặc "append"
    partition_cols  : Nếu None → không partition thêm (đã nhúng date vào path)
    """
    path = silver_path(source, run_date)
    logger.info("Writing Parquet to silver: %s (mode=%s)", path, mode)
    writer = df.write.mode(mode).option("compression", "snappy")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)
    logger.info("Done writing to silver: %s", path)


def write_parquet_gold(
    df: DataFrame,
    output_type: str,
    run_date: Optional[date] = None,
    mode: str = "overwrite",
    partition_cols: Optional[list[str]] = None,
) -> None:
    """Ghi Parquet vào gold layer."""
    path = gold_path(output_type, run_date)
    logger.info("Writing Parquet to gold: %s (mode=%s)", path, mode)
    writer = df.write.mode(mode).option("compression", "snappy")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)
    logger.info("Done writing to gold: %s", path)


def write_geojson_gold(
    df: DataFrame,
    output_type: str,
    run_date: Optional[date] = None,
    geometry_col: str = "geometry",
) -> None:
    """
    Ghi GeoJSON vào gold layer cho dashboard consumption.
    Cần Apache Sedona để convert geometry → GeoJSON string.
    """
    from sedona.spark import SedonaContext  # noqa: F401
    from pyspark.sql import functions as F

    path = gold_path(output_type, run_date)
    logger.info("Writing GeoJSON to gold: %s", path)
    (
        df.withColumn(geometry_col, F.expr(f"ST_AsGeoJSON({geometry_col})"))
        .write
        .mode("overwrite")
        .json(path)
    )
