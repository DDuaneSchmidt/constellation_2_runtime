#!/usr/bin/env bash
set -euo pipefail

cd /home/node/constellation_2_runtime

DAY_UTC="$(date -u +%F)"
PY="/home/node/constellation_2_runtime/.venv_c2/bin/python"

exec "$PY" /home/node/constellation_2_runtime/ops/tools/run_c2_auto_repair_controller_v1.py \
  --day_utc "$DAY_UTC" \
  --launch_mode LIVE
