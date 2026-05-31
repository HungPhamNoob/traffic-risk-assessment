#!/bin/bash
# Run request/response checks for the main cloud services after a full run.

set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-big-data-group-4}"
ZONE="${GCP_ZONE:-us-central1-a}"
NODE1="${NODE1:-node1-control}"
NODE2="${NODE2:-node2-streaming}"
NODE3="${NODE3:-node3-batch}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
LOG_DIR="${LOG_DIR:-logs/cloud_runs}"
OUTPUT_DIR="${LOG_DIR}/${RUN_ID}"
REALTIME_OBSERVE_SECONDS="${REALTIME_OBSERVE_SECONDS:-75}"

mkdir -p "${OUTPUT_DIR}"

node_external_ip() {
  local node="$1"
  gcloud compute instances describe "${node}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --format='get(networkInterfaces[0].accessConfigs[0].natIP)'
}

curl_report() {
  local label="$1"
  local url="$2"
  local body_file
  body_file="$(mktemp)"
  local status_code

  status_code="$(curl -sS --max-time 20 -o "${body_file}" -w '%{http_code}' "${url}" || true)"

  {
    echo "### ${label}"
    echo
    echo "- Request: \`GET ${url}\`"
    echo "- HTTP status: \`${status_code}\`"
    echo "- Response preview:"
    echo
    echo '```'
    python3 - "${body_file}" <<'PY'
from pathlib import Path
import json
import sys

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8", errors="replace").strip()
if not text:
    print("<empty>")
    raise SystemExit(0)
try:
    obj = json.loads(text)
except Exception:
    lines = text.splitlines()
    preview = "\n".join(lines[:20])
    print(preview[:4000])
else:
    pretty = json.dumps(obj, indent=2, ensure_ascii=False)
    print(pretty[:4000])
PY
    echo '```'
    echo
  } >> "${OUTPUT_DIR}/service-checks.md"

  rm -f "${body_file}"
}

NODE1_IP="$(node_external_ip "${NODE1}")"
NODE2_IP="$(node_external_ip "${NODE2}")"
NODE3_IP="$(node_external_ip "${NODE3}")"

SUMMARY_BEFORE_FILE="${OUTPUT_DIR}/summary-before.json"
SUMMARY_AFTER_FILE="${OUTPUT_DIR}/summary-after.json"

curl -fsS "http://${NODE1_IP}:8000/api/v1/overview/summary?mode=full" > "${SUMMARY_BEFORE_FILE}" || true
sleep "${REALTIME_OBSERVE_SECONDS}"
curl -fsS "http://${NODE1_IP}:8000/api/v1/overview/summary?mode=full" > "${SUMMARY_AFTER_FILE}" || true

{
  echo "# Traffic Risk Service Checks"
  echo
  echo "- Collected at: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "- Realtime observation window: ${REALTIME_OBSERVE_SECONDS}s"
  echo
  echo "## Public endpoints"
  echo
} > "${OUTPUT_DIR}/service-checks.md"

curl_report "Dashboard homepage" "http://${NODE1_IP}:3001"
curl_report "FastAPI health" "http://${NODE1_IP}:8000/health"
curl_report "FastAPI system status" "http://${NODE1_IP}:8000/api/v1/system/status"
curl_report "FastAPI overview summary" "http://${NODE1_IP}:8000/api/v1/overview/summary?mode=full"
curl_report "FastAPI throughput" "http://${NODE1_IP}:8000/api/v1/pipeline/throughput?window=5m"
curl_report "FastAPI latency" "http://${NODE1_IP}:8000/api/v1/pipeline/latency?metric=p95"
curl_report "FastAPI replay health" "http://${NODE1_IP}:8000/api/v1/pipeline/replay-health"
curl_report "FastAPI model history" "http://${NODE1_IP}:8000/api/v1/model/retrain-history?limit=1"
curl_report "FastAPI performance trend" "http://${NODE1_IP}:8000/api/v1/model/performance-trend?limit=5"
curl_report "Airflow health" "http://${NODE1_IP}:8080/health"
curl_report "MLflow health" "http://${NODE1_IP}:5000/health"
curl_report "Prometheus health" "http://${NODE1_IP}:9090/-/healthy"
curl_report "Prometheus API query" "http://${NODE1_IP}:9090/api/v1/query?query=up"
curl_report "Grafana health" "http://${NODE1_IP}:3000/api/health"
curl_report "Flink overview" "http://${NODE2_IP}:8081/overview"
curl_report "Flink jobs" "http://${NODE2_IP}:8081/jobs"
curl_report "Spark master" "http://${NODE3_IP}:8080/json/"

{
  echo "## Realtime observation"
  echo
  python3 - "${SUMMARY_BEFORE_FILE}" "${SUMMARY_AFTER_FILE}" <<'PY'
from pathlib import Path
import json
import sys

before_path = Path(sys.argv[1])
after_path = Path(sys.argv[2])

def load_json(path: Path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

before = load_json(before_path)
after = load_json(after_path)

before_events = before.get("total_events")
after_events = after.get("total_events")
before_time = before.get("latest_event_time")
after_time = after.get("latest_event_time")

print(f"- Before total events: `{before_events}`")
print(f"- After total events: `{after_events}`")
print(f"- Before latest event time: `{before_time}`")
print(f"- After latest event time: `{after_time}`")

changed = before_events != after_events or before_time != after_time
print(f"- Realtime movement detected: `{'yes' if changed else 'no'}`")
PY
  echo
  echo "## Notes"
  echo
  echo "- The pipeline is expected to update the dashboard APIs in near realtime when TomTom polls or US replay inserts new rows."
  echo "- If throughput remains low and latency remains high, the strongest bottlenecks are the single Kafka partition, Flink parallelism of 1, one HTTP MLflow call per US event, one PostgreSQL connection per event, and one GCS object write per US event."
} >> "${OUTPUT_DIR}/service-checks.md"

echo "Service checks written to ${OUTPUT_DIR}/service-checks.md"
