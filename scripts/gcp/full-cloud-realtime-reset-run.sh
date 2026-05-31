#!/bin/bash
# Reset and run the realtime branch from the beginning without forcing a new offline baseline.
#
# This script still resets Kafka, Flink, Spark, PostgreSQL, and the run-specific
# GCS prefixes so the replay and TomTom streams start from a clean state. It
# assumes a usable MLflow baseline model already exists. If the registry is
# empty, Node 1 will bootstrap the baseline automatically.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RUN_OFFLINE_TRAINING="${RUN_OFFLINE_TRAINING:-false}" \
  bash "${SCRIPT_DIR}/full-cloud-reset-run.sh"
