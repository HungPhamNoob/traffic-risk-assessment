"""
postgis_writer.py
=================
Ghi kết quả hotspot và risk_score từ gold layer vào PostGIS.

Tables được ghi:
  - public.hotspots        : polygon geometry, risk metadata
  - public.risk_score_cells: point per H3 cell với risk_score

Strategy:
  - Dùng JDBC + psycopg2 để upsert (INSERT ... ON CONFLICT DO UPDATE)
  - Geometry column lưu dạng WKT → convert sang PostGIS geometry qua ST_GeomFromText
  - Batch collect() từng partition để không bị OOM
"""
from __future__ import annotations

import logging
import os
from typing import Iterator, Optional

import psycopg2
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

_PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
_PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
_PG_USER = os.getenv("POSTGRES_USER", "postgres")
_PG_PASS = os.getenv("POSTGRES_PASSWORD", "changeme")
_PG_DB   = os.getenv("POSTGRES_DB", "accident_risk")


def _get_pg_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=_PG_HOST, port=_PG_PORT,
        user=_PG_USER, password=_PG_PASS,
        dbname=_PG_DB,
    )


def ensure_tables_exist() -> None:
    """
    Tạo PostGIS tables nếu chưa tồn tại.
    Chạy một lần khi setup.
    """
    create_hotspots_sql = """
    CREATE TABLE IF NOT EXISTS public.hotspots (
        hotspot_id       TEXT PRIMARY KEY,
        h3_index_res6    TEXT NOT NULL,
        centroid_lat     DOUBLE PRECISION,
        centroid_lon     DOUBLE PRECISION,
        accident_count   BIGINT,
        avg_severity     DOUBLE PRECISION,
        density_score    DOUBLE PRECISION,
        risk_score       DOUBLE PRECISION,
        geometry         GEOMETRY(Polygon, 4326),
        latest_event     TIMESTAMP,
        updated_at       TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_hotspots_geom
        ON public.hotspots USING GIST(geometry);
    CREATE INDEX IF NOT EXISTS idx_hotspots_risk
        ON public.hotspots(risk_score DESC);
    """

    create_risk_cells_sql = """
    CREATE TABLE IF NOT EXISTS public.risk_score_cells (
        h3_index_res8    TEXT PRIMARY KEY,
        h3_index_res6    TEXT,
        centroid_lat     DOUBLE PRECISION,
        centroid_lon     DOUBLE PRECISION,
        accident_count   BIGINT,
        avg_severity     DOUBLE PRECISION,
        weather_risk     DOUBLE PRECISION,
        temporal_risk    DOUBLE PRECISION,
        risk_score       DOUBLE PRECISION,
        geometry         GEOMETRY(Point, 4326),
        updated_at       TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_risk_cells_geom
        ON public.risk_score_cells USING GIST(geometry);
    CREATE INDEX IF NOT EXISTS idx_risk_cells_score
        ON public.risk_score_cells(risk_score DESC);
    CREATE INDEX IF NOT EXISTS idx_risk_cells_h3_res6
        ON public.risk_score_cells(h3_index_res6);
    """

    with _get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(create_hotspots_sql)
        cur.execute(create_risk_cells_sql)
        conn.commit()
    logger.info("PostGIS tables ensured: hotspots, risk_score_cells")


def write_hotspots_to_postgis(hotspot_df: DataFrame) -> None:
    """
    Upsert hotspot DataFrame vào PostGIS table public.hotspots.

    Dùng foreachPartition để tránh collect toàn bộ về driver.
    """
    logger.info("Writing hotspots to PostGIS...")

    upsert_sql = """
    INSERT INTO public.hotspots
        (hotspot_id, h3_index_res6, centroid_lat, centroid_lon,
         accident_count, avg_severity, density_score, risk_score,
         geometry, latest_event, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
            ST_GeomFromText(%s, 4326), %s, NOW())
    ON CONFLICT (hotspot_id) DO UPDATE SET
        accident_count = EXCLUDED.accident_count,
        avg_severity   = EXCLUDED.avg_severity,
        density_score  = EXCLUDED.density_score,
        risk_score     = EXCLUDED.risk_score,
        geometry       = EXCLUDED.geometry,
        latest_event   = EXCLUDED.latest_event,
        updated_at     = NOW();
    """

    # Chọn đúng cột cần ghi
    cols = [
        "hotspot_id", "h3_index_res6", "centroid_lat", "centroid_lon",
        "accident_count", "avg_severity", "density_score", "risk_score",
        "geometry_wkt", "latest_event",
    ]
    existing_cols = [c for c in cols if c in hotspot_df.columns]
    df_to_write = hotspot_df.select(*existing_cols)

    pg_host, pg_port = _PG_HOST, _PG_PORT
    pg_user, pg_pass, pg_db = _PG_USER, _PG_PASS, _PG_DB

    def upsert_partition(rows: Iterator) -> None:
        conn = psycopg2.connect(
            host=pg_host, port=pg_port,
            user=pg_user, password=pg_pass, dbname=pg_db,
        )
        cur = conn.cursor()
        batch = []
        for row in rows:
            batch.append((
                row.hotspot_id, row.h3_index_res6,
                row.centroid_lat, row.centroid_lon,
                row.accident_count, row.avg_severity,
                row.density_score, row.risk_score,
                row.geometry_wkt,
                getattr(row, "latest_event", None),
            ))
            if len(batch) >= 500:
                cur.executemany(upsert_sql, batch)
                conn.commit()
                batch.clear()
        if batch:
            cur.executemany(upsert_sql, batch)
            conn.commit()
        cur.close()
        conn.close()

    df_to_write.foreachPartition(upsert_partition)
    logger.info("Hotspots written to PostGIS successfully.")


def write_risk_cells_to_postgis(risk_df: DataFrame) -> None:
    """
    Upsert risk_score per H3 cell vào PostGIS table public.risk_score_cells.
    """
    logger.info("Writing risk score cells to PostGIS...")

    upsert_sql = """
    INSERT INTO public.risk_score_cells
        (h3_index_res8, h3_index_res6, centroid_lat, centroid_lon,
         accident_count, avg_severity, weather_risk, temporal_risk,
         risk_score, geometry, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326), NOW())
    ON CONFLICT (h3_index_res8) DO UPDATE SET
        risk_score    = EXCLUDED.risk_score,
        accident_count = EXCLUDED.accident_count,
        weather_risk  = EXCLUDED.weather_risk,
        temporal_risk = EXCLUDED.temporal_risk,
        geometry      = EXCLUDED.geometry,
        updated_at    = NOW();
    """

    pg_host, pg_port = _PG_HOST, _PG_PORT
    pg_user, pg_pass, pg_db = _PG_USER, _PG_PASS, _PG_DB

    def upsert_partition(rows: Iterator) -> None:
        conn = psycopg2.connect(
            host=pg_host, port=pg_port,
            user=pg_user, password=pg_pass, dbname=pg_db,
        )
        cur = conn.cursor()
        batch = []
        for row in rows:
            batch.append((
                row.h3_index_res8, row.h3_index_res6,
                row.centroid_lat, row.centroid_lon,
                row.accident_count, row.avg_severity,
                getattr(row, "weather_risk", None),
                getattr(row, "temporal_risk", None),
                row.risk_score,
                row.centroid_lon, row.centroid_lat,  # ST_MakePoint(lon, lat)
            ))
            if len(batch) >= 500:
                cur.executemany(upsert_sql, batch)
                conn.commit()
                batch.clear()
        if batch:
            cur.executemany(upsert_sql, batch)
            conn.commit()
        cur.close()
        conn.close()

    risk_df.foreachPartition(upsert_partition)
    logger.info("Risk score cells written to PostGIS successfully.")
