"""
hotspot_sedona.py
=================
Phát hiện hotspot tai nạn giao thông dùng Apache Sedona + H3 aggregation.

Thuật toán:
  1. Load gold/features/ (đã có h3_index_res8)
  2. Aggregate: count accidents và avg severity per H3 cell (res=8)
  3. Tính normalized density score
  4. Filter ra các ô có density vượt ngưỡng → "hotspot candidates"
  5. Dùng Sedona ST_KNN / spatial clustering để merge các ô kề nhau
  6. Tạo polygon boundary cho mỗi hotspot cluster (ST_ConvexHull hoặc H3 boundary)
  7. Output: hotspot_id, polygon, centroid, risk_score, accident_count

Output schema:
  hotspot_id      : string (h3_index_res6 làm proxy cluster ID)
  h3_res8_cells   : array<string> (các ô con thuộc hotspot)
  centroid_lat    : double
  centroid_lon    : double
  accident_count  : long
  avg_severity    : double
  density_score   : double  (normalized 0-1)
  geometry_wkt    : string  (WKT polygon)
"""
from __future__ import annotations

import logging
import os
from datetime import date
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, LongType

logger = logging.getLogger(__name__)

_GCS_BUCKET      = os.getenv("GCS_BUCKET", "")  # Bắt buộc set GCS_BUCKET trong .env
_HOTSPOT_THRESHOLD = float(os.getenv("HOTSPOT_DENSITY_THRESHOLD", "0.3"))


def _compute_h3_aggregates(df: DataFrame) -> DataFrame:
    """
    Aggregate accidents per H3 cell (res=8).
    Trả về: h3_index_res8, h3_index_res6, accident_count, avg_severity,
             centroid_lat, centroid_lon.
    """
    agg_df = (
        df
        .filter(F.col("h3_index_res8").isNotNull())
        .groupBy("h3_index_res8", "h3_index_res6")
        .agg(
            F.count("event_id").alias("accident_count"),
            F.avg("severity").alias("avg_severity"),
            F.avg("lat").alias("centroid_lat"),
            F.avg("lon").alias("centroid_lon"),
            F.max("event_time").alias("latest_event"),
        )
    )
    return agg_df


def _normalize_density(agg_df: DataFrame) -> DataFrame:
    """
    Tính density_score = accident_count / max(accident_count) trong toàn dataset.
    Normalize về 0-1.
    """
    max_count = agg_df.agg(F.max("accident_count")).collect()[0][0] or 1
    logger.info("Max accident count per H3 cell: %d", max_count)

    return agg_df.withColumn(
        "density_score",
        (F.col("accident_count") / F.lit(max_count)).cast(DoubleType()),
    )


def _generate_h3_polygon_udf() -> F.UserDefinedFunction:
    """UDF tạo WKT polygon từ H3 cell boundary."""
    try:
        import h3 as h3lib

        def h3_to_wkt(h3_index: str) -> str | None:
            if not h3_index:
                return None
            try:
                boundary = h3lib.h3_to_geo_boundary(h3_index, geo_json=True)
                # boundary là list of [lon, lat]
                coords = " ".join(f"{lon} {lat}" for lon, lat in boundary)
                first  = f"{boundary[0][0]} {boundary[0][1]}"
                return f"POLYGON (({coords}, {first}))"
            except Exception:
                return None

        return F.udf(h3_to_wkt, StringType())

    except ImportError:
        return F.udf(lambda x: None, StringType())


def run_hotspot_detection(
    spark: SparkSession,
    run_date: Optional[date] = None,
    density_threshold: float = _HOTSPOT_THRESHOLD,
    use_sedona: bool = True,
) -> DataFrame:
    """
    Chạy hotspot detection và trả về DataFrame của các hotspot.

    Parameters
    ----------
    spark             : SparkSession với Sedona enabled
    run_date          : Filter theo ngày. None = toàn bộ dataset.
    density_threshold : Chỉ giữ ô có density_score >= threshold (0.0-1.0)
    use_sedona        : Dùng Sedona ST functions để tạo geometry

    Returns
    -------
    DataFrame hotspot với polygon geometry
    """
    from processing.spark.utils.gcs_utils import read_parquet_gold, write_parquet_gold

    # ── 1. Load gold features ─────────────────────────────────────────────────
    logger.info("Loading gold features for hotspot detection (date=%s)", run_date)
    gold_df = read_parquet_gold(spark, "features/accident_features", run_date)

    # ── 2. Aggregate per H3 cell ─────────────────────────────────────────────
    agg_df = _compute_h3_aggregates(gold_df)
    agg_df = _normalize_density(agg_df)

    # ── 3. Filter hotspot candidates ─────────────────────────────────────────
    hotspot_candidates = agg_df.filter(
        F.col("density_score") >= F.lit(density_threshold)
    )

    candidate_count = hotspot_candidates.count()
    logger.info(
        "Hotspot candidates (density >= %.2f): %d cells",
        density_threshold, candidate_count,
    )

    # ── 4. Cluster bằng H3 res=6 (merge các ô kề nhau) ───────────────────────
    # Aggregate lại theo res=6 (coarser) để tạo cluster-level hotspot
    cluster_df = (
        hotspot_candidates
        .groupBy("h3_index_res6")
        .agg(
            F.sum("accident_count").alias("accident_count"),
            F.avg("avg_severity").alias("avg_severity"),
            F.avg("centroid_lat").alias("centroid_lat"),
            F.avg("centroid_lon").alias("centroid_lon"),
            F.max("density_score").alias("density_score"),
            F.collect_list("h3_index_res8").alias("h3_res8_cells"),
            F.max("latest_event").alias("latest_event"),
        )
    )

    # ── 5. Tạo polygon geometry ──────────────────────────────────────────────
    if use_sedona:
        # Dùng Sedona ST_MakePoint + ST_Buffer để tạo vùng
        cluster_df = cluster_df.withColumn(
            "geometry_wkt",
            F.expr(
                "ST_AsText(ST_Buffer(ST_MakePoint(centroid_lon, centroid_lat), 0.005))"
            ),
        )
    else:
        # Fallback: H3 boundary polygon qua Python UDF
        h3_poly_udf = _generate_h3_polygon_udf()
        cluster_df = cluster_df.withColumn(
            "geometry_wkt",
            h3_poly_udf(F.col("h3_index_res6")),
        )

    # ── 6. Tạo hotspot_id và normalize risk_score ─────────────────────────────
    # Dùng density_score * severity_weight làm hotspot risk proxy
    cluster_df = (
        cluster_df
        .withColumn(
            "hotspot_id",
            F.concat_ws("-", F.lit("hs"), F.col("h3_index_res6")),
        )
        .withColumn(
            "risk_score",
            F.least(
                F.lit(1.0),
                F.col("density_score") * (F.col("avg_severity") / F.lit(4.0)),
            ).cast(DoubleType()),
        )
    )

    # ── 7. Select output cols ────────────────────────────────────────────────
    hotspot_df = cluster_df.select(
        F.col("hotspot_id"),
        F.col("h3_index_res6"),
        F.col("h3_res8_cells"),
        F.col("centroid_lat"),
        F.col("centroid_lon"),
        F.col("accident_count").cast(LongType()),
        F.col("avg_severity"),
        F.col("density_score"),
        F.col("risk_score"),
        F.col("geometry_wkt"),
        F.col("latest_event"),
    )

    # ── 8. Write to gold/hotspots/ ───────────────────────────────────────────
    write_parquet_gold(
        hotspot_df,
        output_type="hotspots",
        run_date=run_date,
        mode="overwrite",
    )
    logger.info("Hotspot detection complete. Hotspot count: %d", hotspot_df.count())

    return hotspot_df