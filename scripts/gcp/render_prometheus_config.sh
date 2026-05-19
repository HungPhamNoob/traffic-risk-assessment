#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/opt/traffic}"
ENV_FILE="${1:-${ENV_FILE:-${PROJECT_ROOT}/.env.cloud}}"
TEMPLATE_FILE="${PROJECT_ROOT}/config/monitoring/prometheus.cloud.yml.template"
OUTPUT_FILE="${PROJECT_ROOT}/config/monitoring/prometheus.cloud.yml"

if [ ! -f "${ENV_FILE}" ]; then
  echo "ERROR: ${ENV_FILE} does not exist."
  exit 1
fi

if [ ! -f "${TEMPLATE_FILE}" ]; then
  echo "ERROR: ${TEMPLATE_FILE} does not exist."
  exit 1
fi

set -a
. "${ENV_FILE}"
set +a

: "${NODE2_INTERNAL_IP:?NODE2_INTERNAL_IP is required to render Prometheus config}"
: "${NODE3_INTERNAL_IP:?NODE3_INTERNAL_IP is required to render Prometheus config}"

sed \
  -e "s|__NODE2_INTERNAL_IP__|${NODE2_INTERNAL_IP}|g" \
  -e "s|__NODE3_INTERNAL_IP__|${NODE3_INTERNAL_IP}|g" \
  "${TEMPLATE_FILE}" > "${OUTPUT_FILE}"

echo "Rendered ${OUTPUT_FILE} from ${TEMPLATE_FILE}"
