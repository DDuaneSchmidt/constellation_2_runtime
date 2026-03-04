#!/usr/bin/env bash
set -euo pipefail

cd /home/node/constellation_2_runtime

DAY_UTC="$(date -u +%F)"

# Fail-closed: require tool exists
test -f ops/tools/run_c2_multi_sleeve_orchestrator_v1.py

# Run
python3 ops/tools/run_c2_multi_sleeve_orchestrator_v1.py --day_utc "${DAY_UTC}"

# Proof line (for logs)
test -f "constellation_2/runtime/truth/reports/sleeve_rollup_v1/${DAY_UTC}/sleeve_rollup.v1.json"
test -f "constellation_2/runtime/truth/reports/sleeve_rollup_v1/${DAY_UTC}/canonical_pointer_index.v1.jsonl"
echo "OK: c2_multi_sleeve_orchestrator_run_v1 day_utc=${DAY_UTC}"
