#!/bin/bash
# Start Node 2 and Node 3 together after a failure or replay reset.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

bash "${SCRIPT_DIR}/node23-lifecycle.sh" start
