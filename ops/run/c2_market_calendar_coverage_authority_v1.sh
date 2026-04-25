#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
RUNTIME_DATA_ROOT="/home/node/constellation_runtime_data"
RUNTIME_TRUTH_ROOT="${C2_TRUTH_ROOT:-${RUNTIME_DATA_ROOT}/truth}"
SOURCE_ROOT="${C2_MARKET_CALENDAR_SOURCE_ROOT:-}"
MODE="${C2_MARKET_CALENDAR_COVERAGE_MODE:-CHECK}"
BUFFER_CALENDAR_DAYS="${C2_MARKET_CALENDAR_BUFFER_CALENDAR_DAYS:-3}"
MIN_REQUIRED_OFFSET_CALENDAR_DAYS="${C2_MARKET_CALENDAR_MIN_REQUIRED_OFFSET_CALENDAR_DAYS:-1}"
REQUIRED_TARGET_DAY="${C2_MARKET_CALENDAR_REQUIRED_TARGET_DAY:-}"
PY="${PYTHON_BIN_OVERRIDE:-python3}"

cd "${REPO_ROOT}"

command -v "${PY}" >/dev/null 2>&1

ARGS=(
  --truth_root "${RUNTIME_TRUTH_ROOT}"
  --mode "${MODE}"
  --buffer_calendar_days "${BUFFER_CALENDAR_DAYS}"
  --minimum_required_offset_calendar_days "${MIN_REQUIRED_OFFSET_CALENDAR_DAYS}"
)

if [[ -n "${SOURCE_ROOT}" ]]; then
  ARGS+=(--source_root "${SOURCE_ROOT}")
fi
if [[ -n "${REQUIRED_TARGET_DAY}" ]]; then
  ARGS+=(--required_target_day "${REQUIRED_TARGET_DAY}")
fi

exec "${PY}" "${REPO_ROOT}/ops/tools/run_market_calendar_coverage_authority_v1.py" "${ARGS[@]}"
