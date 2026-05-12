#!/bin/bash
# Stop Node 2 and Node 3 as one synchronized pair.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

bash "${SCRIPT_DIR}/node23-lifecycle.sh" stop
