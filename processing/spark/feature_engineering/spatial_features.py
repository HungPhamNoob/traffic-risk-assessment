"""
spatial_features.py
===================
Tạo các spatial features dùng H3 hexagonal grid (Uber).

Features tạo ra:
  - h3_index_res8  : H3 cell index ở resolution 8 (~460m edge, ~1km² area)
                     Dùng cho clustering chi tiết và hotspot detection
  - h3_index_res6  : H3 cell index ở resolution 6 (~3.7km edge, ~36km² area)
                     Dùng để aggregate risk cho vùng rộng hơn (heatmap)

Lưu ý:
  - H3 UDF khá nặng — cân nhắc persist() sau bước này
  - Cần thư viện: h3==3.7.7 (pip) và h3-java nếu dùng trên Spark cluster
  - Alternative: dùng Apache Sedona ST_H3CellIDs nếu đã cài Sedona
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

logger = logging.getLogger(__name__)

# ── H3 UDF ───────────────────────────────────────────────────────────────────
try:
    import h3 as h3lib

    def _h3_index(lat: float, lon: float, resolution: int) -> str | None:
        if lat is None or lon is None:
            return None
        try:
            return h3lib.geo_to_h3(lat, lon, resolution)
        except Exception:
            return None

    _h3_res8_udf = F.udf(lambda lat, lon: _h3_index(lat, lon, 8), StringType())
    _h3_res6_udf = F.udf(lambda lat, lon: _h3_index(lat, lon, 6), StringType())
    _H3_AVAILABLE = True

except ImportError:
    logger.warning(
        "h3 library not installed. H3 features will be NULL. "
        "Install with: pip install h3==3.7.7"
    )
    _H3_AVAILABLE = False
    _h3_res8_udf = F.udf(lambda lat, lon: None, StringType())
    _h3_res6_udf = F.udf(lambda lat, lon: None, StringType())


def add_spatial_features(df: DataFrame) -> DataFrame:
    """
    Thêm H3 cell index vào DataFrame.

    Parameters
    ----------
    df : DataFrame có cột lat (DoubleType) và lon (DoubleType)

    Returns
    -------
    DataFrame với h3_index_res8 và h3_index_res6 bổ sung
    """
    if not _H3_AVAILABLE:
        logger.warning("H3 not available — adding NULL H3 columns as placeholders")

    logger.info("Adding H3 spatial features (res=8, res=6)")

    df = (
        df
        .withColumn(
            "h3_index_res8",
            _h3_res8_udf(F.col("lat"), F.col("lon")),
        )
        .withColumn(
            "h3_index_res6",
            _h3_res6_udf(F.col("lat"), F.col("lon")),
        )
    )

    logger.info("Spatial features added: h3_index_res8, h3_index_res6")
    return df


def add_spatial_features_sedona(df: DataFrame) -> DataFrame:
    """
    Alternative: Tạo H3 index dùng Apache Sedona ST functions.
    Dùng cách này nếu cài Sedona trên cluster (nhanh hơn Python UDF).

    Cần Sedona >= 1.6 với ST_H3CellIDs.
    """
    logger.info("Adding H3 spatial features via Sedona ST_H3CellIDs")

    df = (
        df
        .withColumn(
            "h3_index_res8",
            F.expr("ST_H3CellIDs(ST_Point(lon, lat), 8, true)[0]"),
        )
        .withColumn(
            "h3_index_res6",
            F.expr("ST_H3CellIDs(ST_Point(lon, lat), 6, true)[0]"),
        )
    )
    return df
