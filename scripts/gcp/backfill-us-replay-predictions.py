#!/usr/bin/env python3
"""Backfill missing US replay H2O predictions in PostgreSQL without resetting state."""

from __future__ import annotations

import argparse
import os
import re
import time
from typing import Any

from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
import requests

from shared.risk_scoring import compute_unified_risk_score, infer_severity_from_prediction


MODEL_FEATURE_COLUMNS = [
    "lat",
    "lon",
    "hour",
    "day_of_week",
    "is_weekend",
    "is_rush_hour",
    "weather_code",
    "temperature_f",
    "humidity",
    "wind_speed_mph",
    "visibility_mi",
    "road_type_code",
    "is_junction",
    "has_traffic_signal",
    "is_crossing",
    "is_roundabout",
    "is_stop",
    "is_station",
    "is_railway",
    "is_night",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    return parser.parse_args()


def table_name(value: str) -> str:
    selected = value.split(".")[-1]
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", selected):
        raise ValueError(f"Invalid PostgreSQL table name: {value}")
    return selected


def request_predictions(
    session: requests.Session,
    endpoint: str,
    rows: list[dict[str, Any]],
    timeout_seconds: float,
) -> list[Any]:
    payload = {
        "dataframe_split": {
            "columns": MODEL_FEATURE_COLUMNS,
            "data": [[row.get(column) for column in MODEL_FEATURE_COLUMNS] for row in rows],
        }
    }
    response = session.post(
        endpoint,
        json=payload,
        timeout=timeout_seconds,
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
    return list(response.json().get("predictions", []))


def request_prediction_one(
    session: requests.Session,
    endpoint: str,
    row: dict[str, Any],
    timeout_seconds: float,
) -> Any:
    predictions = request_predictions(session, endpoint, [row], timeout_seconds)
    return predictions[0] if predictions else None


def build_update_rows(
    session: requests.Session,
    endpoint: str,
    rows: list[dict[str, Any]],
    timeout_seconds: float,
) -> list[tuple[Any, ...]]:
    predictions: list[Any] = []
    try:
        predictions = request_predictions(session, endpoint, rows, timeout_seconds)
        if len(predictions) != len(rows):
            raise ValueError(
                f"Expected {len(rows)} predictions but received {len(predictions)}"
            )
    except Exception:
        predictions = []
        for row in rows:
            try:
                predictions.append(
                    request_prediction_one(session, endpoint, row, timeout_seconds)
                )
            except Exception:
                predictions.append(None)

    updates = []
    for row, prediction in zip(rows, predictions):
        predicted_severity = infer_severity_from_prediction(prediction)
        model_status = "ok" if predicted_severity is not None else "backfill_failed"
        risk_score = compute_unified_risk_score(
            severity=predicted_severity or row.get("true_severity"),
            is_night=row.get("is_night"),
            is_weekend=row.get("is_weekend"),
            road_type_code=row.get("road_type_code"),
            weather_code=row.get("weather_code"),
        )
        updates.append(
            (
                row["event_id"],
                predicted_severity,
                risk_score,
                model_status,
            )
        )
    return updates


def main() -> None:
    args = parse_args()
    load_dotenv(".env.cloud")
    load_dotenv(".env")

    pg_host = os.getenv("POSTGRES_HOST", "localhost")
    pg_port = int(os.getenv("POSTGRES_PORT", "5432"))
    pg_db = os.getenv("POSTGRES_DB", "capstone_db")
    pg_user = os.getenv("POSTGRES_USER", "capstone")
    pg_password = os.getenv("POSTGRES_PASSWORD", "123")
    endpoint = os.getenv("MLFLOW_SERVING_ENDPOINT", "http://localhost:5001/invocations")
    timeout_seconds = float(os.getenv("ML_TIMEOUT_SECONDS", "20"))
    prediction_table = table_name(
        os.getenv("POSTGRES_US_PREDICTION_TABLE")
        or os.getenv("POSTGRES_PREDICTION_TABLE", "traffic_risk_predictions")
    )

    connection = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        dbname=pg_db,
        user=pg_user,
        password=pg_password,
    )
    session = requests.Session()

    processed = 0
    updated = 0
    started_at = time.time()

    try:
        while True:
            remaining_limit = max(args.limit - processed, 0) if args.limit else None
            if remaining_limit == 0:
                break

            batch_size = (
                min(args.batch_size, remaining_limit)
                if remaining_limit is not None
                else args.batch_size
            )
            if batch_size <= 0:
                break

            with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    f"""
                    SELECT
                        event_id,
                        true_severity,
                        weather_code,
                        road_type_code,
                        is_weekend,
                        is_rush_hour,
                        is_junction,
                        has_traffic_signal,
                        is_crossing,
                        is_roundabout,
                        is_stop,
                        is_station,
                        is_railway,
                        is_night,
                        lat,
                        lon,
                        hour,
                        day_of_week,
                        temperature_f,
                        humidity,
                        wind_speed_mph,
                        visibility_mi
                    FROM {prediction_table}
                    WHERE (
                        predicted_severity IS NULL
                        OR COALESCE(model_status, '') <> 'ok'
                        OR risk_score IS NULL
                        OR risk_score < 0
                    )
                      AND COALESCE(model_status, '') <> 'backfill_failed'
                    ORDER BY created_at ASC NULLS LAST, event_id ASC
                    LIMIT %s
                    """,
                    (batch_size,),
                )
                rows = [dict(row) for row in cursor.fetchall()]

            if not rows:
                break

            update_rows = build_update_rows(session, endpoint, rows, timeout_seconds)
            success_count = sum(1 for _, severity, _, status in update_rows if severity is not None and status == "ok")

            with connection:
                with connection.cursor() as cursor:
                    psycopg2.extras.execute_values(
                        cursor,
                        f"""
                        UPDATE {prediction_table} AS target
                        SET predicted_severity = source.predicted_severity,
                            risk_score = source.risk_score,
                            model_status = source.model_status
                        FROM (
                            VALUES %s
                        ) AS source(event_id, predicted_severity, risk_score, model_status)
                        WHERE target.event_id = source.event_id
                        """,
                        update_rows,
                        template="(%s, %s, %s, %s)",
                    )

            processed += len(rows)
            updated += success_count
            elapsed = time.time() - started_at
            print(
                f"Processed {processed} rows, restored {updated} H2O predictions, "
                f"elapsed {elapsed:.1f}s"
            )

            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)
    finally:
        session.close()
        connection.close()

    print(f"Backfill completed. Rows scanned: {processed}. H2O predictions restored: {updated}.")


if __name__ == "__main__":
    main()
