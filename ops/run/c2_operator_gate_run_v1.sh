#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

DAY_UTC="$(date -u +%F)"

cd "${REPO_ROOT}"

exec python3 \
  "${REPO_ROOT}/ops/tools/run_c2_daily_operator_gate_v1.py" \
  --day_utc "$DAY_UTC"
