#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/home/node/constellation_2_runtime"
TRUTH="${REPO_ROOT}/constellation_2/runtime/truth"
REG_AUTH="${REPO_ROOT}/governance/02_REGISTRIES/TRUTH_SURFACE_AUTHORITY_V1.json"
REG_GATES="${REPO_ROOT}/governance/02_REGISTRIES/GATE_HIERARCHY_V1.json"

# 1) Runtime verdict surfaces must not exist (structural elimination)
FORBID_DIRS=(
  "${TRUTH}/reports/operator_gate_verdict_v1"
  "${TRUTH}/reports/operator_gate_verdict_v2"
  "${TRUTH}/reports/operator_gate_verdict_v3"
)

for d in "${FORBID_DIRS[@]}"; do
  if [[ -d "${d}" ]]; then
    echo "FAIL: forbidden verdict surface directory exists: ${d}" >&2
    ls -la "${d}" | sed -n '1,60p' >&2 || true
    exit 2
  fi
done

# 2) Registries must not claim operator_gate_verdict authority or gate status
if [[ -f "${REG_AUTH}" ]]; then
  if rg -n --no-heading '"operator_gate_verdict"' "${REG_AUTH}" >/dev/null 2>&1; then
    echo "FAIL: TRUTH_SURFACE_AUTHORITY must not include operator_gate_verdict" >&2
    rg -n --no-heading 'operator_gate_verdict' "${REG_AUTH}" | sed -n '1,40p' >&2 || true
    exit 2
  fi
fi

if [[ -f "${REG_GATES}" ]]; then
  if rg -n --no-heading 'operator_gate_verdict' "${REG_GATES}" >/dev/null 2>&1; then
    echo "FAIL: GATE_HIERARCHY must not include operator_gate_verdict" >&2
    rg -n --no-heading 'operator_gate_verdict' "${REG_GATES}" | sed -n '1,40p' >&2 || true
    exit 2
  fi
fi

echo "[c2-preflight] PASS: operator_gate_verdict verdict surfaces eliminated"
