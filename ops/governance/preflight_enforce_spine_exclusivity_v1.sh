#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/home/node/constellation_2_runtime"
TRUTH_ROOT="${REPO_ROOT}/constellation_2/runtime/truth"
RUN_PTR="${TRUTH_ROOT}/latest.json"
REG="${REPO_ROOT}/governance/02_REGISTRIES/C2_SPINE_AUTHORITY_V1.json"
PY_CHECK="${REPO_ROOT}/ops/governance/preflight_enforce_spine_exclusivity_v1.py"

echo "[c2-preflight] enforcing spine exclusivity (day-scoped) via ${REG}"

if [[ ! -f "${RUN_PTR}" ]]; then
  echo "FAIL: missing run pointer: ${RUN_PTR}" >&2
  exit 2
fi
if [[ ! -f "${REG}" ]]; then
  echo "FAIL: missing spine authority registry: ${REG}" >&2
  exit 2
fi
if [[ ! -f "${PY_CHECK}" ]]; then
  echo "FAIL: missing python checker: ${PY_CHECK}" >&2
  exit 2
fi

python3 "${PY_CHECK}"
