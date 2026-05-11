#!/usr/bin/env bash
set -euo pipefail

RUNTIME_ROOT="/home/node/constellation_2_runtime"
cd "${RUNTIME_ROOT}"

export PYTHONPATH="${RUNTIME_ROOT}${PYTHONPATH:+:${PYTHONPATH}}"

DAY_UTC="${C2_AUTO_REPAIR_DAY_UTC:-$(date -u +%F)}"
PY="${C2_AUTO_REPAIR_PY:-${RUNTIME_ROOT}/.venv_c2/bin/python}"
TOOL="${C2_AUTO_REPAIR_TOOL:-${RUNTIME_ROOT}/ops/tools/run_c2_auto_repair_controller_v1.py}"

REQUESTED_LAUNCH_MODE="${C2_AUTO_REPAIR_LAUNCH_MODE:-DRY_RUN}"
REQUESTED_LAUNCH_MODE="${REQUESTED_LAUNCH_MODE^^}"
EFFECTIVE_LAUNCH_MODE="DRY_RUN"

if [[ "${REQUESTED_LAUNCH_MODE}" == "LIVE" && "${C2_AUTO_REPAIR_ALLOW_LIVE:-0}" == "1" ]]; then
  EFFECTIVE_LAUNCH_MODE="LIVE"
fi

echo "C2_AUTO_REPAIR_CONTROLLER_WRAPPER_V1 requested_launch_mode=${REQUESTED_LAUNCH_MODE} effective_launch_mode=${EFFECTIVE_LAUNCH_MODE} live_allowed=${C2_AUTO_REPAIR_ALLOW_LIVE:-0} day_utc=${DAY_UTC}" >&2

exec "$PY" "$TOOL" \
  --day_utc "$DAY_UTC" \
  --launch_mode "$EFFECTIVE_LAUNCH_MODE"
