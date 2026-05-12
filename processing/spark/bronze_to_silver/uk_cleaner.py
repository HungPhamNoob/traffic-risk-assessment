"""
uk_cleaner.py
=============
Clean và chuẩn hóa dữ liệu UK Accidents (STATS19) từ bronze → silver.

Dataset: Road Safety Data - Collisions (STATS19 phiên bản mới)
URL: https://www.data.gov.uk/dataset/cb7ae6f0-4be6-4935-9277-47e5ce24a11f/road-safety-data

Đặc điểm file thực tế (header đã xác nhận):
  - Tên cột dùng prefix `collision_*` thay vì `accident_*` (phiên bản cũ)
  - `collision_index`  : ID duy nhất (thay accident_index)
  - `collision_severity`: mức nghiêm trọng (thay accident_severity)
  - Có thêm nhiều cột mới: junction_control, pedestrian_crossing, carriageway_hazards...
  - Thời gian: `date` (dd/MM/yyyy) + `time` (HH:mm) → kết hợp thành timestamp
  - Severity: 1=Fatal, 2=Serious, 3=Slight (ngược với US)

Mapping các bước:
  1. Đọc CSV với UK_BRONZE_SCHEMA (tên cột collision_*)
  2. Combine date + time → event_time timestamp
  3. Map road_type integer → string
  4. Normalize severity (UK 1=Fatal → normalize về 4=Fatal để thống nhất với US)
  5. Filter invalid lat/lon
  6. Rename fields về SILVER_SCHEMA
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

logger = logging.getLogger(__name__)

# ── UK road_type code → string (STATS19 lookup table) ───────────────────────
# 1=Roundabout, 2=One way street, 3=Dual carriageway,
# 6=Single carriageway, 7=Slip road, 9=Unknown, 12=One way/Slip road
_UK_ROAD_TYPE_MAP = {
    "1":  "roundabout",
    "2":  "one_way",
    "3":  "dual_carriageway",   # motorway-style
    "6":  "single_carriageway",
    "7":  "slip_road",
    "9":  "unknown",
    "12": "one_way",
}

# ── UK severity → normalized (align với US: 1=minor .. 4=fatal) ─────────────
# UK: 1=Fatal, 2=Serious, 3=Slight
# Normalized: 4=Fatal, 3=Serious, 2=Slight
_UK_SEVERITY_MAP = {
    "1": 4,  # Fatal
    "2": 3,  # Serious
    "3": 2,  # Slight
}


def _map_road_type() -> F.Column:
    col = F.col("road_type").cast("string")
    expr = F.when(F.lit(False), F.lit("unknown"))
    for code, label in _UK_ROAD_TYPE_MAP.items():
        expr = expr.when(col == F.lit(code), F.lit(label))
    return expr.otherwise(F.lit("unknown"))


def _map_severity() -> F.Column:
    # ℹ️ Dùng `collision_severity` (tên cột mới trong file thực tế)
    #     thay vì `accident_severity` (phiên bản STATS19 cũ trước 2023)
    col = F.col("collision_severity").cast("string")
    expr = F.when(F.lit(False), F.lit(0))
    for code, norm in _UK_SEVERITY_MAP.items():
        expr = expr.when(col == F.lit(code), F.lit(norm))
    return expr.otherwise(F.lit(1))


def clean_uk_accidents(
    spark: SparkSession,
    input_path: str,
    run_date: Optional[date] = None,
) -> DataFrame:
    """
    Clean UK Accidents CSV và trả về DataFrame với SILVER_SCHEMA.

    Parameters
    ----------
    spark       : SparkSession
    input_path  : GCS path đến file CSV bronze
                  Ví dụ: "gs://road-accident-data/bronze/raw/uk_accidents/*.csv"
    run_date    : Ngày chạy job

    Returns
    -------
    DataFrame với các cột theo SILVER_SCHEMA
    """
    from processing.spark.utils.schema_definitions import UK_BRONZE_SCHEMA

    logger.info("Reading UK accidents from: %s", input_path)

    raw = (
        spark.read
        .option("header", "true")
        .option("multiLine", "true")
        .schema(UK_BRONZE_SCHEMA)
        .csv(input_path)
    )

    logger.info("Raw row count: %d", raw.count())

    # ── 1. Filter invalid coordinates ────────────────────────────────────────
    df = raw.filter(
        F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
        & F.col("latitude").between(-90, 90)
        & F.col("longitude").between(-180, 180)
    )

    # ── 2. Parse timestamp từ date + time columns ─────────────────────────────
    # UK date format: "01/01/2022", time: "07:12"
    df = df.withColumn(
        "event_time",
        F.to_timestamp(
            F.concat_ws(" ", F.col("date"), F.col("time")),
            "dd/MM/yyyy HH:mm",
        ),
    )
    df = df.filter(F.col("event_time").isNotNull())

    # ── 3. Map road_type integer → string ─────────────────────────────────────
    df = df.withColumn("road_type_str", _map_road_type())

    # ── 4. Normalize severity ─────────────────────────────────────────────────
    df = df.withColumn("severity_norm", _map_severity())

    # ── 5. Generate event_id từ collision_index (tên cột mới) ──────────────────
    # Nếu dùng `accident_index` sẽ ra NULL toàn bộ vì cột đó không tồn tại trong file mới
    df = df.withColumn(
        "event_id",
        F.concat_ws("-", F.lit("uk"), F.col("collision_index")),
    )

    # ── 6. Derive state_or_region từ police_force (simplified) ───────────────
    # Dùng police_force code làm proxy cho region
    df = df.withColumn(
        "state_or_region",
        F.col("local_authority_district").cast("string"),
    )

    # ── 7. Select SILVER_SCHEMA columns ──────────────────────────────────────────
    df = df.select(
        F.col("event_id"),
        F.lit("uk").alias("source"),
        F.col("event_time"),
        F.col("latitude").alias("lat"),
        F.col("longitude").alias("lon"),
        F.col("severity_norm").cast(IntegerType()).alias("severity"),
        F.col("weather_conditions").cast(IntegerType()).alias("weather_code"),
        F.col("road_type_str").alias("road_type"),
        F.col("state_or_region"),
        F.lit(None).cast("string").alias("city"),
        F.lit(None).cast("string").alias("description"),
    )

    # ── 8. Thêm event_date partition ─────────────────────────────────────────
    if run_date is not None:
        df = df.withColumn("event_date", F.lit(run_date.isoformat()))
    else:
        df = df.withColumn(
            "event_date",
            F.date_format(F.col("event_time"), "yyyy-MM-dd"),
        )

    df = df.filter(F.col("event_id").isNotNull())

    clean_count = df.count()
    logger.info("Clean UK rows: %d", clean_count)
    return df
