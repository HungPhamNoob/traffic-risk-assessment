"""
weather_enricher.py
===================
Chuẩn hóa weather_code thành nhóm và thêm weather severity weight.

Unified weather codes (dùng chung cho US + UK):
  0  = Unknown
  1  = Clear/Fair
  2  = Cloudy/Overcast
  3  = Light Rain/Drizzle
  4  = Heavy Rain
  5  = Thunderstorm
  6  = Light Snow
  7  = Heavy Snow/Blizzard
  8  = Ice/Sleet/Freezing
  9  = Fog/Mist/Haze

Features bổ sung:
  - weather_group       : string category ("clear", "rain", "snow", "ice", "fog", "other")
  - weather_risk_weight : float 1.0-3.0 (risk multiplier khi tính model feature)
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

# weather_code → group
_CODE_TO_GROUP = {
    0: "unknown",
    1: "clear",
    2: "cloudy",
    3: "rain",
    4: "rain",
    5: "storm",
    6: "snow",
    7: "snow",
    8: "ice",
    9: "fog",
}

# weather_code → risk multiplier
_CODE_TO_WEIGHT = {
    0: 1.0,   # unknown → neutral
    1: 1.0,   # clear
    2: 1.1,   # cloudy
    3: 1.4,   # light rain
    4: 1.8,   # heavy rain
    5: 2.2,   # thunderstorm
    6: 1.6,   # light snow
    7: 2.5,   # heavy snow
    8: 3.0,   # ice (highest risk)
    9: 1.7,   # fog
}


def _build_group_expr() -> F.Column:
    col = F.col("weather_code")
    expr = F.when(F.lit(False), F.lit("unknown"))
    for code, group in _CODE_TO_GROUP.items():
        expr = expr.when(col == F.lit(code), F.lit(group))
    return expr.otherwise(F.lit("unknown"))


def _build_weight_expr() -> F.Column:
    col = F.col("weather_code")
    expr = F.when(F.lit(False), F.lit(1.0))
    for code, weight in _CODE_TO_WEIGHT.items():
        expr = expr.when(col == F.lit(code), F.lit(weight))
    return expr.otherwise(F.lit(1.0))


def enrich_weather(df: DataFrame) -> DataFrame:
    """
    Thêm weather_group và weather_risk_weight vào DataFrame.

    Parameters
    ----------
    df : DataFrame có cột weather_code (IntegerType)

    Returns
    -------
    DataFrame với weather_group và weather_risk_weight bổ sung
    """
    logger.info("Enriching weather features")

    df = (
        df
        .withColumn("weather_group",       _build_group_expr())
        .withColumn("weather_risk_weight", _build_weight_expr())
    )

    logger.info("Weather enrichment done: weather_group, weather_risk_weight")
    return df
