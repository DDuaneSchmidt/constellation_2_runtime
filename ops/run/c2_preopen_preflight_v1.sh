#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

# Compatibility wrapper: this surface remains non-authoritative for launch,
# but the pre-open check must still resolve through the canonical
# TRADING_DAY_STATE_MACHINE path and report the final_start_decision from the
# authoritative downstream state machine output.
DAY_UTC="${1:-$(date -u +%F)}"
TRUTH_ROOT="${C2_TRUTH_ROOT:-}"

CMD=(/usr/bin/python3 "${REPO_ROOT}/ops/tools/run_trading_day_state_machine_v1.py" --day_utc "${DAY_UTC}")
if [[ -n "${TRUTH_ROOT}" ]]; then
  CMD+=(--truth_root "${TRUTH_ROOT}")
fi

exec "${CMD[@]}"
