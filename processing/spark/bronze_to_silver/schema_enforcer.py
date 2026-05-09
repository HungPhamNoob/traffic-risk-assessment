"""
schema_enforcer.py
==================
Validate DataFrame theo SILVER_SCHEMA sau khi clean:
  - Kiểm tra cột bắt buộc (NOT NULL)
  - Reject row lỗi vào quarantine path trên GCS
  - Log data quality metrics lên MLflow (optional)
"""
from __future__ import annotations

import logging
import os
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

# Cột bắt buộc phải có giá trị
_REQUIRED_COLS = ["event_id", "source", "lat", "lon"]

# Cột phải nằm trong khoảng hợp lý
_RANGE_CHECKS: dict[str, tuple[float, float]] = {
    "lat":      (-90.0, 90.0),
    "lon":      (-180.0, 180.0),
    "severity": (1.0, 4.0),
}

_BRONZE_BUCKET = os.getenv("GCS_BRONZE_BUCKET", "")  # Quarantine lưu vào bronze bucket


def enforce_schema(
    df: DataFrame,
    source: str,
    quarantine: bool = True,
    log_mlflow: bool = False,
    run_date: Optional[str] = None,
) -> DataFrame:
    """
    Validate DataFrame và trả về DataFrame với chỉ các row hợp lệ.

    Parameters
    ----------
    df          : DataFrame từ us_cleaner hoặc uk_cleaner
    source      : "us" | "uk" — dùng để tạo quarantine path
    quarantine  : Nếu True → ghi row lỗi vào GCS quarantine/
    log_mlflow  : Nếu True → log data quality metrics vào MLflow
    run_date    : "YYYY-MM-DD" để tag quarantine path

    Returns
    -------
    DataFrame chỉ chứa row hợp lệ
    """
    total = df.count()
    logger.info("Schema enforcement start — source=%s, total_rows=%d", source, total)

    # ── Build invalid condition ───────────────────────────────────────────────
    invalid_cond = F.lit(False)

    # 1. NULL checks trên cột bắt buộc
    for col_name in _REQUIRED_COLS:
        if col_name in df.columns:
            invalid_cond = invalid_cond | F.col(col_name).isNull()

    # 2. Range checks
    for col_name, (lo, hi) in _RANGE_CHECKS.items():
        if col_name in df.columns:
            invalid_cond = invalid_cond | (
                ~F.col(col_name).between(lo, hi)
            )

    # ── Split valid / invalid ─────────────────────────────────────────────────
    invalid_df = df.filter(invalid_cond)
    valid_df   = df.filter(~invalid_cond)

    invalid_count = invalid_df.count()
    valid_count   = valid_df.count()
    reject_pct    = (invalid_count / total * 100) if total > 0 else 0.0

    logger.info(
        "Validation result — valid=%d, invalid=%d, reject_pct=%.2f%%",
        valid_count, invalid_count, reject_pct,
    )

    # ── Quarantine ghi row lỗi ────────────────────────────────────────────────
    if quarantine and invalid_count > 0:
        date_tag = run_date or "unknown"
        q_path = (
            f"gs://{_BRONZE_BUCKET}/quarantine/{source}/date={date_tag}/"
        )
        logger.warning(
            "Writing %d quarantine rows to: %s", invalid_count, q_path
        )
        (
            invalid_df
            .withColumn("_quarantine_reason", F.lit("failed_schema_check"))
            .write
            .mode("overwrite")
            .parquet(q_path)
        )

    # ── Log MLflow metrics ────────────────────────────────────────────────────
    if log_mlflow:
        try:
            import mlflow
            with mlflow.start_run(run_name=f"schema_check_{source}_{run_date}"):
                mlflow.log_metrics({
                    "total_rows":   total,
                    "valid_rows":   valid_count,
                    "invalid_rows": invalid_count,
                    "reject_pct":   reject_pct,
                })
                mlflow.set_tag("source", source)
        except Exception as exc:
            logger.warning("MLflow logging failed: %s", exc)

    # Cảnh báo nếu reject quá nhiều
    if reject_pct > 5.0:
        logger.error(
            "HIGH REJECT RATE (%.1f%%) for source=%s — kiểm tra lại data!",
            reject_pct, source,
        )

    return valid_df
