#!/usr/bin/env bash
set -euo pipefail

cd /home/node/constellation_2_runtime

DAY_UTC="$(date -u +%F)"

exec /home/node/constellation_2_runtime/.venv_c2/bin/python \
  /home/node/constellation_2_runtime/ops/tools/run_c2_daily_operator_gate_v1.py \
  --day_utc "$DAY_UTC"
