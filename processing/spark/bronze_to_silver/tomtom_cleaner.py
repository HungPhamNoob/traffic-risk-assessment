"""
tomtom_cleaner.py
=================
Clean parsed TomTom Traffic Incident Details records from bronze to silver.

Input records are JSON events produced by
`ingestion/kafka/producers/tomtom_producer.py`.

The batch pipeline's training contract is SILVER_SCHEMA:
event_id, source, event_time, lat, lon, severity, weather_code, road_type,
state_or_region, city, description, event_date.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

logger = logging.getLogger(__name__)


def _map_tomtom_severity() -> F.Column:
    """
    Map TomTom incident signals to the shared 1-4 severity scale.

    Batch convention:
      1 = lowest risk/severity, 4 = highest risk/severity.

    TomTom inputs:
      - magnitudeOfDelay: 0-4 delay severity bucket.
      - iconCategory: incident category. Road closure and accident are high risk
        even when delay is absent.
    """
    magnitude = F.coalesce(F.col("delay_magnitude"), F.lit(0))
    icon = F.col("icon_category")

    delay_severity = (
        F.when(magnitude >= 4, F.lit(4))
        .when(magnitude == 3, F.lit(3))
        .when(magnitude == 2, F.lit(2))
        .otherwise(F.lit(1))
    )

    return (
        F.when(icon == 8, F.greatest(delay_severity, F.lit(4)))  # road closure
        .when(icon == 1, F.greatest(delay_severity, F.lit(3)))   # accident
        .when(icon == 9, F.greatest(delay_severity, F.lit(2)))   # roadworks
        .otherwise(delay_severity)
        .cast(IntegerType())
    )


def _build_description() -> F.Column:
    """Build a compact training/debug description from TomTom fields."""
    return F.concat_ws(
        " | ",
        F.col("incident_description"),
        F.concat(F.lit("from="), F.col("from_road")),
        F.concat(F.lit("to="), F.col("to_road")),
        F.concat(F.lit("icon_category="), F.col("icon_category").cast("string")),
        F.concat(F.lit("delay_magnitude="), F.col("delay_magnitude").cast("string")),
        F.concat(F.lit("delay_seconds="), F.col("delay_seconds").cast("string")),
        F.concat(F.lit("length_meters="), F.col("length_meters").cast("string")),
    )


def clean_tomtom_incidents(
    spark: SparkSession,
    input_path: str,
    run_date: Optional[date] = None,
) -> DataFrame:
    """
    Clean TomTom incident JSON records and return a SILVER_SCHEMA DataFrame.

    Parameters
    ----------
    spark      : SparkSession
    input_path : Bronze JSON path containing parsed TomTom events
    run_date   : Run date for partitioning. None = derive from event_time.
    """
    from processing.spark.utils.schema_definitions import TOMTOM_BRONZE_SCHEMA

    logger.info("Reading TomTom incidents from: %s", input_path)

    raw = (
        spark.read
        .option("multiLine", "false")
        .schema(TOMTOM_BRONZE_SCHEMA)
        .json(input_path)
    )

    logger.info("Raw TomTom row count: %d", raw.count())

    df = raw.filter(
        F.col("event_id").isNotNull()
        & F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
        & F.col("latitude").between(-90, 90)
        & F.col("longitude").between(-180, 180)
    )

    # Spark parses ISO-8601 strings with trailing Z in common deployments.
    # Keep last_report_time as fallback for incidents missing startTime.
    df = df.withColumn(
        "event_time",
        F.coalesce(F.to_timestamp("timestamp"), F.to_timestamp("last_report_time")),
    ).filter(F.col("event_time").isNotNull())

    df = df.withColumn("severity_norm", _map_tomtom_severity())

    silver = df.select(
        F.col("event_id"),
        F.lit("tomtom").alias("source"),
        F.col("event_time"),
        F.col("latitude").alias("lat"),
        F.col("longitude").alias("lon"),
        F.col("severity_norm").alias("severity"),
        F.lit(0).cast(IntegerType()).alias("weather_code"),
        F.lit("unknown").alias("road_type"),
        F.col("state_or_region"),
        F.col("city"),
        _build_description().alias("description"),
    )

    if run_date is not None:
        silver = silver.withColumn("event_date", F.lit(run_date.isoformat()))
    else:
        silver = silver.withColumn("event_date", F.date_format(F.col("event_time"), "yyyy-MM-dd"))

    clean_count = silver.count()
    logger.info("Clean TomTom rows: %d", clean_count)
    return silver
