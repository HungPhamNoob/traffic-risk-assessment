#!/bin/bash
# Run a local end-to-end pipeline with configurable data volume.
#
# Inputs:
#   - data/process/us_train_offline_before_2020.csv for before-2020 training features.
#   - data/split/us_pipeline_from_2020.csv for after-2020 replay simulation.
#
# Outputs:
#   - data/simulation/silver/flink_features/events.jsonl
#   - data/simulation/gold/features/retrain/csv/local_retrain_features.csv
#   - data/simulation/gold/features/retrain/parquet/
#   - data/simulation/api/*.json responses from the FastAPI backend.
#   - data/simulation/performance/local_stream_metrics.json
#
# This script uses LOCAL_SAMPLE_ROWS by default so validation remains practical
# on laptops. Set LOCAL_SAMPLE_ROWS=0 to process the full replay file.

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${ENV_FILE:-${PROJECT_ROOT}/.env}"
LOCAL_SAMPLE_ROWS="${LOCAL_SAMPLE_ROWS:-200}"
LOCAL_RUN_TRAINING="${LOCAL_RUN_TRAINING:-false}"

cd "${PROJECT_ROOT}"

if [ ! -f "${ENV_FILE}" ]; then
  echo "ERROR: ${ENV_FILE} does not exist. Create it from .env.example first."
  exit 1
fi

set -a
. "${ENV_FILE}"
set +a

mkdir -p \
  "${SIMULATION_DATA_DIR}/silver/flink_features" \
  "${SIMULATION_DATA_DIR}/gold/features/retrain/csv" \
  "${SIMULATION_DATA_DIR}/gold/features/retrain/parquet" \
  "${SIMULATION_DATA_DIR}/checkpoints/flink" \
  "${SIMULATION_DATA_DIR}/checkpoints/spark" \
  "${SIMULATION_DATA_DIR}/api" \
  "${SIMULATION_DATA_DIR}/performance" \
  "${SIMULATION_DATA_DIR}/mlflow-artifacts"

wait_for_container_health() {
  local container_name="$1"
  local label="$2"

  echo "Waiting for ${label} health."
  for attempt in $(seq 1 90); do
    status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${container_name}" 2>/dev/null || true)"
    if [ "${status}" = "healthy" ] || [ "${status}" = "running" ]; then
      return 0
    fi
    sleep 2
    if [ "${attempt}" = "90" ]; then
      echo "ERROR: ${label} did not become healthy. Last status: ${status:-missing}"
      docker compose --env-file "${ENV_FILE}" ps
      exit 1
    fi
  done
}

echo "Starting local infrastructure: PostgreSQL, Redis, Kafka, MLflow, and FastAPI."
docker compose --env-file "${ENV_FILE}" up -d --build \
  postgres redis zookeeper kafka-1 kafka-2 kafka-3 mlflow fastapi

wait_for_container_health "local-kafka-1" "Kafka broker 1"
wait_for_container_health "local-kafka-2" "Kafka broker 2"
wait_for_container_health "local-kafka-3" "Kafka broker 3"

echo "Starting Kafka topic initializer."
docker compose --env-file "${ENV_FILE}" up -d kafka-topic-init
wait_for_container_health "local-kafka-topic-init" "Kafka topic initializer"

echo "Waiting for FastAPI health endpoint."
for attempt in $(seq 1 60); do
  if curl -fsS "http://localhost:8000/health" >/dev/null 2>&1; then
    break
  fi
  sleep 2
  if [ "${attempt}" = "60" ]; then
    echo "ERROR: FastAPI did not become healthy."
    docker compose --env-file "${ENV_FILE}" ps
    exit 1
  fi
done

echo "Building local simulation features from ${US_PIPELINE_REPLAY_PATH}."
.venv/bin/python - <<'PY'
import json
import os
from pathlib import Path
import shutil
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from processing.feature_engineering import build_features

sample_rows = int(os.getenv("LOCAL_SAMPLE_ROWS", "200"))
source_path = Path(os.getenv("US_PIPELINE_REPLAY_PATH", "data/split/us_pipeline_from_2020.csv"))
simulation_dir = Path(os.getenv("SIMULATION_DATA_DIR", "data/simulation"))
silver_path = simulation_dir / "silver" / "flink_features" / "events.jsonl"
gold_csv_path = simulation_dir / "gold" / "features" / "retrain" / "csv" / "local_retrain_features.csv"
gold_parquet_path = simulation_dir / "gold" / "features" / "retrain" / "parquet"
performance_path = simulation_dir / "performance" / "local_stream_metrics.json"

if not source_path.exists():
    raise FileNotFoundError(f"Replay source does not exist: {source_path}")

silver_path.parent.mkdir(parents=True, exist_ok=True)
gold_csv_path.parent.mkdir(parents=True, exist_ok=True)
gold_parquet_path.mkdir(parents=True, exist_ok=True)

# Each local replay run must produce a clean Silver/Gold snapshot. Reusing an
# existing CSV in append mode would duplicate rows and can insert repeated CSV
# headers, which later appears to H2O as missing label values.
for stale_file in [silver_path, gold_csv_path, performance_path]:
    stale_file.unlink(missing_ok=True)
for stale_entry in gold_parquet_path.glob("*"):
    if stale_entry.is_dir():
        shutil.rmtree(stale_entry)
    else:
        stale_entry.unlink()

start_time = time.time()
rows_written = 0
latencies = []
csv_header_written = False
parquet_writer = None
parquet_file = gold_parquet_path / "local_retrain_features.parquet"

with silver_path.open("w", encoding="utf-8") as silver_file:
    for chunk in pd.read_csv(source_path, chunksize=10000, low_memory=False):
        chunk_features = []
        for raw_row in chunk.to_dict(orient="records"):
            ingestion_time = time.time()
            feature_row = build_features(raw_row)
            if feature_row is None:
                continue
            processed_time = time.time()
            latency_ms = (processed_time - ingestion_time) * 1000.0
            feature_row["ingestion_time_epoch"] = ingestion_time
            feature_row["processed_time_epoch"] = processed_time
            feature_row["end_to_end_latency_ms"] = latency_ms
            chunk_features.append(feature_row)
            silver_file.write(json.dumps(feature_row, ensure_ascii=False) + "\n")
            latencies.append(latency_ms)
            rows_written += 1
            if sample_rows > 0 and rows_written >= sample_rows:
                break

        if chunk_features:
            chunk_df = pd.DataFrame(chunk_features)
            chunk_df.to_csv(
                gold_csv_path,
                mode="a",
                index=False,
                header=not csv_header_written,
            )
            csv_header_written = True

            table = pa.Table.from_pandas(chunk_df, preserve_index=False)
            if parquet_writer is None:
                parquet_writer = pq.ParquetWriter(parquet_file, table.schema)
            parquet_writer.write_table(table)

        if sample_rows > 0 and rows_written >= sample_rows:
            break

if parquet_writer is not None:
    parquet_writer.close()

if rows_written == 0:
    raise RuntimeError("No valid replay features were generated.")

elapsed = max(time.time() - start_time, 1e-6)
latency_series = pd.Series(latencies)
performance = {
    "rows": int(rows_written),
    "elapsed_seconds": round(elapsed, 4),
    "throughput_tps": round(rows_written / elapsed, 4),
    "latency_ms_avg": round(float(latency_series.mean()), 4),
    "latency_ms_p95": round(float(latency_series.quantile(0.95)), 4),
    "window_seconds": 30,
    "target_latency_seconds": 10,
    "target_throughput_tps": 50,
}
performance_path.parent.mkdir(parents=True, exist_ok=True)
performance_path.write_text(json.dumps(performance, indent=2), encoding="utf-8")

print(f"Wrote {rows_written} simulation rows to {silver_path}")
print(f"Wrote {rows_written} Gold CSV rows to {gold_csv_path}")
print(f"Wrote Gold Parquet to {gold_parquet_path}")
print(f"Wrote local performance metrics to {performance_path}")
PY

if [ "${LOCAL_RUN_TRAINING}" = "true" ]; then
  echo "Running bounded before-2020 H2O training."
  export US_TRAIN_OFFLINE_PATH="${US_TRAIN_OFFLINE_PATH}"
  export H2O_MAX_RUNTIME="${H2O_MAX_RUNTIME:-120}"
  .venv/bin/python ml/training/h2o_before_2020.py
else
  echo "Skipping H2O training. Set LOCAL_RUN_TRAINING=true to enable it."
fi

echo "Creating and seeding the local prediction table."
.venv/bin/python - <<'PY'
import json
import os
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_batch

simulation_dir = Path(os.getenv("SIMULATION_DATA_DIR", "data/simulation"))
silver_path = simulation_dir / "silver" / "flink_features" / "events.jsonl"
table_name = os.getenv("POSTGRES_PREDICTION_TABLE", "traffic_risk_predictions")

connection = psycopg2.connect(
    host="localhost",
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    dbname=os.getenv("POSTGRES_DB", "capstone_db"),
    user=os.getenv("POSTGRES_USER", "capstone"),
    password=os.getenv("POSTGRES_PASSWORD", "123"),
)

create_sql = f"""
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE TABLE IF NOT EXISTS {table_name} (
    event_id VARCHAR PRIMARY KEY,
    event_year INT,
    event_time TIMESTAMP,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    true_severity INT,
    predicted_severity INT,
    risk_score DOUBLE PRECISION,
    weather_code INT,
    temperature_f DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    wind_speed_mph DOUBLE PRECISION,
    visibility_mi DOUBLE PRECISION,
    road_type_code INT,
    hour INT,
    day_of_week INT,
    is_weekend INT,
    is_rush_hour INT,
    is_junction INT,
    has_traffic_signal INT,
    is_crossing INT,
    is_roundabout INT,
    is_stop INT,
    is_station INT,
    is_railway INT,
    is_night INT,
    model_status VARCHAR(20),
    inference_latency_ms DOUBLE PRECISION,
    ingestion_time TIMESTAMP,
    processed_time TIMESTAMP,
    end_to_end_latency_ms DOUBLE PRECISION,
    geom GEOMETRY(Point, 4326),
    created_at TIMESTAMP DEFAULT NOW()
);
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS ingestion_time TIMESTAMP;
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS processed_time TIMESTAMP;
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS end_to_end_latency_ms DOUBLE PRECISION;
"""

insert_sql = f"""
INSERT INTO {table_name} (
    event_id, event_year, event_time, lat, lon, true_severity,
    predicted_severity, risk_score, weather_code, temperature_f,
    humidity, wind_speed_mph, visibility_mi, road_type_code, hour,
    day_of_week, is_weekend, is_rush_hour, is_junction, has_traffic_signal,
    is_crossing, is_roundabout, is_stop, is_station, is_railway, is_night,
    model_status, inference_latency_ms, ingestion_time, processed_time,
    end_to_end_latency_ms, geom
) VALUES (
    %(event_id)s, %(event_year)s, %(event_time)s, %(lat)s, %(lon)s,
    %(true_severity)s, %(predicted_severity)s, %(risk_score)s,
    %(weather_code)s, %(temperature_f)s, %(humidity)s, %(wind_speed_mph)s,
    %(visibility_mi)s, %(road_type_code)s, %(hour)s, %(day_of_week)s,
    %(is_weekend)s, %(is_rush_hour)s, %(is_junction)s, %(has_traffic_signal)s,
    %(is_crossing)s, %(is_roundabout)s, %(is_stop)s, %(is_station)s,
    %(is_railway)s, %(is_night)s, 'local', 0.0, NOW(), NOW(),
    %(end_to_end_latency_ms)s,
    ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)
) ON CONFLICT (event_id) DO UPDATE SET
    risk_score = EXCLUDED.risk_score,
    predicted_severity = EXCLUDED.predicted_severity,
    model_status = EXCLUDED.model_status,
    end_to_end_latency_ms = EXCLUDED.end_to_end_latency_ms,
    created_at = NOW();
"""

with connection:
    with connection.cursor() as cursor:
        cursor.execute(create_sql)
        inserted = 0
        batch = []
        with silver_path.open("r", encoding="utf-8") as input_file:
            for line in input_file:
                row = json.loads(line)
                predicted_severity = int(row["true_severity"])
                risk_score = max(0.0, min(1.0, (predicted_severity - 1) / 3))
                row.update(
                    {
                        "predicted_severity": predicted_severity,
                        "risk_score": risk_score,
                    }
                )
                batch.append(row)
                inserted += 1
                if len(batch) >= 1000:
                    execute_batch(cursor, insert_sql, batch, page_size=1000)
                    batch.clear()
        if batch:
            execute_batch(cursor, insert_sql, batch, page_size=1000)
print(f"Seeded {inserted} prediction rows into {table_name}.")
connection.close()
PY

echo "Collecting API JSON outputs from the FastAPI backend."
fetch_backend_json() {
  local url="$1"
  local output_path="$2"
  local temp_path
  temp_path="$(mktemp)"

  curl -fsS -H "Accept: application/json" "${url}" -o "${temp_path}"
  .venv/bin/python -m json.tool "${temp_path}" > "${output_path}"
  rm -f "${temp_path}"
}

fetch_backend_json "http://localhost:8000/api/v1/overview/summary" \
  "${SIMULATION_DATA_DIR}/api/overview_summary.json"
fetch_backend_json "http://localhost:8000/api/v1/predictions/map?limit=20" \
  "${SIMULATION_DATA_DIR}/api/predictions_map.json"
fetch_backend_json "http://localhost:8000/api/v1/hotspots?limit=10&min_events=1" \
  "${SIMULATION_DATA_DIR}/api/hotspots.json"
fetch_backend_json "http://localhost:8000/api/v1/analytics/severity-distribution" \
  "${SIMULATION_DATA_DIR}/api/severity_distribution.json"
fetch_backend_json "http://localhost:8000/api/v1/analytics/risk-by-hour" \
  "${SIMULATION_DATA_DIR}/api/risk_by_hour.json"
fetch_backend_json "http://localhost:8000/api/v1/system/status" \
  "${SIMULATION_DATA_DIR}/api/system_status.json"
fetch_backend_json "http://localhost:8000/api/v1/system/health" \
  "${SIMULATION_DATA_DIR}/api/system_health_alias.json"

.venv/bin/python - <<'PY'
import json
import os
from pathlib import Path

import requests

simulation_dir = Path(os.getenv("SIMULATION_DATA_DIR", "data/simulation"))
payload = {
    "baseline": {
        "lat": 39.865147,
        "lon": -84.058723,
        "hour": 12,
        "day_of_week": 2,
        "is_weekend": 0,
        "is_rush_hour": 0,
        "weather_code": 0,
        "temperature_f": 60.0,
        "humidity": 50.0,
        "wind_speed_mph": 5.0,
        "visibility_mi": 10.0,
        "road_type_code": 1,
        "is_junction": 0,
        "has_traffic_signal": 0,
        "is_crossing": 0,
        "is_roundabout": 0,
        "is_stop": 0,
        "is_station": 0,
        "is_railway": 0,
        "is_night": 0,
    },
    "scenario": {
        "lat": 39.865147,
        "lon": -84.058723,
        "hour": 18,
        "day_of_week": 2,
        "is_weekend": 0,
        "is_rush_hour": 1,
        "weather_code": 1,
        "temperature_f": 36.9,
        "humidity": 91.0,
        "wind_speed_mph": 25.0,
        "visibility_mi": 2.0,
        "road_type_code": 1,
        "is_junction": 1,
        "has_traffic_signal": 1,
        "is_crossing": 0,
        "is_roundabout": 0,
        "is_stop": 0,
        "is_station": 0,
        "is_railway": 0,
        "is_night": 1,
    },
}
response = requests.post(
    "http://localhost:8000/api/v1/scenarios/compare",
    json=payload,
    timeout=15,
)
response.raise_for_status()
output_path = simulation_dir / "api" / "scenario_compare.json"
output_path.write_text(json.dumps(response.json(), indent=2), encoding="utf-8")
PY

echo "Local pipeline completed successfully."
