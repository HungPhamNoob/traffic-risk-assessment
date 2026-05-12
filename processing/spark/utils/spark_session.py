"""
spark_session.py
================
Factory để tạo SparkSession với đầy đủ config:
  - GCS Connector (Hadoop 3)
  - Apache Sedona (không gian địa lý)
  - Kyro serializer
  - Adaptive Query Execution

Usage:
    from processing.spark.utils.spark_session import get_spark
    spark = get_spark("BatchETL")
"""
from __future__ import annotations

import os
import logging
from typing import Optional

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# ── Đường dẫn JARs trên node3 ───────────────────────────────────────────────
# Khi triển khai GCP, copy các JAR này vào /opt/spark/jars/ trên node3-batch
_JAR_BASE = os.getenv("SPARK_JARS_DIR", "/opt/spark/jars")
_JARS = [
    f"{_JAR_BASE}/gcs-connector-hadoop3-latest.jar",
    f"{_JAR_BASE}/sedona-spark-shaded-3.0_2.12-1.7.0.jar",
    f"{_JAR_BASE}/geotools-wrapper-1.7.0-28.2.jar",
    f"{_JAR_BASE}/spark-avro_2.12-3.5.1.jar",
    f"{_JAR_BASE}/postgresql-42.7.3.jar",
]

# ── Biến môi trường ──────────────────────────────────────────────────────────
_GCS_BUCKET     = os.getenv("GCS_BUCKET", "")  # Bắt buộc set GCS_BUCKET trong .env
_GCS_PROJECT    = os.getenv("GCS_PROJECT_ID", "your-gcp-project")
_SA_KEY_FILE    = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")  # empty → Workload Identity
_SPARK_MASTER   = os.getenv("SPARK_MASTER_URL", "local[*]")         # local[*] khi dev


def get_spark(
    app_name: str = "RoadAccidentPlatform",
    master: Optional[str] = None,
    enable_hive: bool = False,
    jars: Optional[list[str]] = None,
) -> SparkSession:
    """
    Tạo (hoặc tái sử dụng) SparkSession theo cấu hình platform.

    Parameters
    ----------
    app_name : str
        Tên job hiển thị trên Spark UI.
    master : str | None
        Spark master URL. Mặc định đọc từ env SPARK_MASTER_URL.
        Ví dụ GCP: "spark://node3-internal-ip:7077"
    enable_hive : bool
        Bật Hive metastore support nếu cần.
    jars : list[str] | None
        Override danh sách JAR paths.

    Returns
    -------
    SparkSession
    """
    master_url = master or _SPARK_MASTER
    jar_paths  = jars or _JARS
    jars_str   = ",".join(j for j in jar_paths if os.path.exists(j))

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master_url)

        # ── JARs ──────────────────────────────────────────────
        #.config("spark.jars", jars_str)
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.14,org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.5.1")

        # ── Kyro serialization (bắt buộc cho Sedona) ──────────
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator",
                "org.apache.sedona.core.serde.SedonaKryoRegistrator")

        # ── Sedona SQL extensions ──────────────────────────────
        .config(
            "spark.sql.extensions",
            "org.apache.sedona.viz.sql.SedonaVizExtensions,"
            "org.apache.sedona.sql.SedonaSqlExtensions",
        )

        # ── GCS Hadoop Connector ───────────────────────────────
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")

        # ── GCS authentication ─────────────────────────────────
        # Nếu chạy trên GCE VM với Workload Identity → không cần key file
        # Nếu chạy local → cần set GOOGLE_APPLICATION_CREDENTIALS
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            _SA_KEY_FILE,
        )

        # ── Parquet tuning ─────────────────────────────────────
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.mergeSchema", "false")

        # ── Adaptive Query Execution ───────────────────────────
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
    )

    if enable_hive:
        builder = builder.enableHiveSupport()

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        "SparkSession created: app=%s, master=%s, version=%s",
        app_name, master_url, spark.version,
    )
    return spark


def stop_spark(spark: SparkSession) -> None:
    """Dừng SparkSession an toàn."""
    try:
        spark.stop()
        logger.info("SparkSession stopped.")
    except Exception as exc:
        logger.warning("Error stopping SparkSession: %s", exc)
