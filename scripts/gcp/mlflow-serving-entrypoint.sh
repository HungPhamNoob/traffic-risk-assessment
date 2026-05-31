#!/bin/bash
# Start the MLflow serving container once tracking is reachable and a model exists.

set -euo pipefail

if ! command -v java >/dev/null 2>&1; then
  apt-get update
  apt-get install -y --no-install-recommends openjdk-17-jre-headless
  rm -rf /var/lib/apt/lists/*
fi

MISSING_PYTHON_PACKAGES=()
python3 -c "import h2o" >/dev/null 2>&1 || MISSING_PYTHON_PACKAGES+=("h2o==3.46.0.6")
python3 -c "import pandas" >/dev/null 2>&1 || MISSING_PYTHON_PACKAGES+=("pandas")
python3 -c "import numpy" >/dev/null 2>&1 || MISSING_PYTHON_PACKAGES+=("numpy")
python3 -c "import sklearn" >/dev/null 2>&1 || MISSING_PYTHON_PACKAGES+=("scikit-learn")
python3 -c "import google.auth" >/dev/null 2>&1 || MISSING_PYTHON_PACKAGES+=("google-auth")
python3 -c "import google.cloud.storage" >/dev/null 2>&1 || MISSING_PYTHON_PACKAGES+=("google-cloud-storage")
python3 -c "import requests" >/dev/null 2>&1 || MISSING_PYTHON_PACKAGES+=("requests")

if [ "${#MISSING_PYTHON_PACKAGES[@]}" -gt 0 ]; then
  pip install --no-cache-dir "${MISSING_PYTHON_PACKAGES[@]}"
fi

MODEL_NAME="${ML_MODEL_NAME:-traffic-risk-model}"
MLFLOW_TRACKING_URL="${MLFLOW_TRACKING_URI:-http://mlflow:5000}"

echo "Waiting for MLflow tracking server to be healthy..."
python3 -c "
import sys
import time
import requests

url = '${MLFLOW_TRACKING_URL}/health'
for attempt in range(1, 121):
    try:
        response = requests.get(url, timeout=5)
        if response.ok:
            print('MLflow tracking server is reachable.')
            sys.exit(0)
    except Exception:
        pass
    print(f'  MLflow not ready yet ({attempt}/120). Sleeping 5s.')
    time.sleep(5)

print('MLflow tracking server did not become ready in time.', file=sys.stderr)
sys.exit(1)
"

echo "MLflow is healthy. Looking for registered models..."
while true; do
  FOUND_MODEL="$(python3 -c "
import os
import requests

base_url = '${MLFLOW_TRACKING_URL}/api/2.0/mlflow'
model_name = os.environ.get('ML_MODEL_NAME', '${MODEL_NAME}')

try:
    response = requests.get(
        f'{base_url}/registered-models/get',
        params={'name': model_name},
        timeout=10,
    )
    if response.ok:
        print(model_name)
        raise SystemExit(0)
except Exception:
    pass

try:
    response = requests.get(f'{base_url}/registered-models/list', timeout=10)
    response.raise_for_status()
    registered_models = response.json().get('registered_models') or []
    if registered_models:
        print(registered_models[0].get('name', ''))
except Exception:
    pass
")"

  if [ -n "${FOUND_MODEL}" ]; then
    echo "Serving model: ${FOUND_MODEL}"
    mlflow models serve \
      -m "models:/${FOUND_MODEL}/latest" \
      -h 0.0.0.0 \
      -p 5001 \
      -t "${MLFLOW_SERVING_TIMEOUT_SECONDS:-300}" \
      --env-manager local || {
        echo "Model serving exited. Retrying in 10s..."
        sleep 10
      }
  else
    echo "No registered model found in MLflow. Waiting 15s before retry..."
    sleep 15
  fi
done
