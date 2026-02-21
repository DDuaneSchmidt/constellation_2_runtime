#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/home/node/constellation_2_runtime"
TARGET="${REPO_ROOT}/ops/tools/run_global_kill_switch_v1.py"

if [[ ! -f "${TARGET}" ]]; then
  echo "FAIL: missing kill switch script: ${TARGET}" >&2
  exit 2
fi

# Require: single final verdict consumption
if ! rg -n --no-heading 'gate_stack_verdict_v1' "${TARGET}" >/dev/null 2>&1; then
  echo "FAIL: kill switch must reference gate_stack_verdict_v1" >&2
  exit 2
fi

# Forbid: legacy direct-consumption surfaces
FORBID_PATTERNS=(
  'operator_gate_verdict_v'
  'capital_risk_envelope_v'
  'reconciliation_report_v2'
)

for pat in "${FORBID_PATTERNS[@]}"; do
  if rg -n --no-heading "${pat}" "${TARGET}" >/dev/null 2>&1; then
    echo "FAIL: kill switch must not reference legacy surface token: ${pat}" >&2
    rg -n --no-heading "${pat}" "${TARGET}" | sed -n '1,40p' >&2 || true
    exit 2
  fi
done

echo "[c2-preflight] PASS: kill switch consumes only gate_stack_verdict_v1"
