#!/bin/bash
# Run a local end-to-end smoke pipeline with bounded data volume.
#
# Inputs:
#   - data/process/us_train_offline_before_2020.csv for before-2020 training features.
#   - data/split/us_pipeline_from_2020.csv for after-2020 replay simulation.
#
# Outputs:
#   - data/simulation/silver/flink_features/events.jsonl
#   - data/simulation/gold/features/retrain/local_retrain_features.csv
#   - data/simulation/api/*.json responses from the FastAPI backend.
#
# This script intentionally uses a small record limit by default so local
# validation is repeatable on laptops. Set LOCAL_RUN_TRAINING=true when you
# want to run H2O training as part of the local smoke flow.

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
  "${SIMULATION_DATA_DIR}/gold/features/retrain" \
  "${SIMULATION_DATA_DIR}/checkpoints/flink" \
  "${SIMULATION_DATA_DIR}/checkpoints/spark" \
  "${SIMULATION_DATA_DIR}/api" \
  "${SIMULATION_DATA_DIR}/mlflow-artifacts"

echo "Starting local infrastructure: PostgreSQL, Redis, Kafka, MLflow, and FastAPI."
docker compose --env-file "${ENV_FILE}" up -d postgres redis kafka-topic-init mlflow fastapi

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

echo "Building bounded local simulation features from ${US_PIPELINE_REPLAY_PATH}."
.venv/bin/python - <<'PY'
import csv
import json
import os
from pathlib import Path

from processing.feature_engineering import build_features

sample_rows = int(os.getenv("LOCAL_SAMPLE_ROWS", "200"))
source_path = Path(os.getenv("US_PIPELINE_REPLAY_PATH", "data/split/us_pipeline_from_2020.csv"))
simulation_dir = Path(os.getenv("SIMULATION_DATA_DIR", "data/simulation"))
silver_path = simulation_dir / "silver" / "flink_features" / "events.jsonl"
gold_path = simulation_dir / "gold" / "features" / "retrain" / "local_retrain_features.csv"

if not source_path.exists():
    raise FileNotFoundError(f"Replay source does not exist: {source_path}")

features = []
with source_path.open("r", encoding="utf-8", newline="") as source_file:
    reader = csv.DictReader(source_file)
    for raw_row in reader:
        feature_row = build_features(raw_row)
        if feature_row is None:
            continue
        features.append(feature_row)
        if len(features) >= sample_rows:
            break

if not features:
    raise RuntimeError("No valid replay features were generated.")

silver_path.parent.mkdir(parents=True, exist_ok=True)
with silver_path.open("w", encoding="utf-8") as output_file:
    for feature_row in features:
        output_file.write(json.dumps(feature_row, ensure_ascii=False) + "\n")

gold_path.parent.mkdir(parents=True, exist_ok=True)
with gold_path.open("w", encoding="utf-8", newline="") as output_file:
    writer = csv.DictWriter(output_file, fieldnames=list(features[0].keys()))
    writer.writeheader()
    writer.writerows(features)

print(f"Wrote {len(features)} simulation rows to {silver_path}")
print(f"Wrote {len(features)} retrain rows to {gold_path}")
PY

if [ "${LOCAL_RUN_TRAINING}" = "true" ]; then
  echo "Running bounded before-2020 H2O training."
  export US_TRAIN_OFFLINE_PATH="${US_TRAIN_OFFLINE_PATH}"
  export H2O_MAX_RUNTIME="${H2O_MAX_RUNTIME:-120}"
  .venv/bin/python ml/training/train_before_2020.py
else
  echo "Skipping H2O training. Set LOCAL_RUN_TRAINING=true to enable it."
fi

echo "Creating and seeding the local prediction table."
.venv/bin/python - <<'PY'
import json
import os
from pathlib import Path

import psycopg2

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
    geom GEOMETRY(Point, 4326),
    created_at TIMESTAMP DEFAULT NOW()
);
"""

insert_sql = f"""
INSERT INTO {table_name} (
    event_id, event_year, event_time, lat, lon, true_severity,
    predicted_severity, risk_score, weather_code, temperature_f,
    humidity, wind_speed_mph, visibility_mi, road_type_code, hour,
    day_of_week, is_weekend, is_rush_hour, is_junction, has_traffic_signal,
    is_crossing, is_roundabout, is_stop, is_station, is_railway, is_night,
    model_status, inference_latency_ms, geom
) VALUES (
    %(event_id)s, %(event_year)s, %(event_time)s, %(lat)s, %(lon)s,
    %(true_severity)s, %(predicted_severity)s, %(risk_score)s,
    %(weather_code)s, %(temperature_f)s, %(humidity)s, %(wind_speed_mph)s,
    %(visibility_mi)s, %(road_type_code)s, %(hour)s, %(day_of_week)s,
    %(is_weekend)s, %(is_rush_hour)s, %(is_junction)s, %(has_traffic_signal)s,
    %(is_crossing)s, %(is_roundabout)s, %(is_stop)s, %(is_station)s,
    %(is_railway)s, %(is_night)s, 'local', 0.0,
    ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)
) ON CONFLICT (event_id) DO UPDATE SET
    risk_score = EXCLUDED.risk_score,
    predicted_severity = EXCLUDED.predicted_severity,
    model_status = EXCLUDED.model_status,
    created_at = NOW();
"""

with connection:
    with connection.cursor() as cursor:
        cursor.execute(create_sql)
        inserted = 0
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
                cursor.execute(insert_sql, row)
                inserted += 1
print(f"Seeded {inserted} prediction rows into {table_name}.")
connection.close()
PY

echo "Collecting API JSON outputs."
curl -fsS "http://localhost:8000/api/v1/overview/summary" \
  | tee "${SIMULATION_DATA_DIR}/api/overview_summary.json" >/dev/null
curl -fsS "http://localhost:8000/api/v1/predictions/map?limit=20" \
  | tee "${SIMULATION_DATA_DIR}/api/predictions_map.json" >/dev/null
curl -fsS "http://localhost:8000/api/v1/hotspots?limit=10&min_events=1" \
  | tee "${SIMULATION_DATA_DIR}/api/hotspots.json" >/dev/null
curl -fsS "http://localhost:8000/api/v1/analytics/severity-distribution" \
  | tee "${SIMULATION_DATA_DIR}/api/severity_distribution.json" >/dev/null
curl -fsS "http://localhost:8000/api/v1/system/status" \
  | tee "${SIMULATION_DATA_DIR}/api/system_status.json" >/dev/null

echo "Local pipeline smoke flow completed successfully."
