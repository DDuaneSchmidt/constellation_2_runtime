#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/home/node/constellation_2_runtime"
TRUTH_ROOT="${REPO_ROOT}/constellation_2/runtime/truth"

echo "[c2-preflight] enforcing single latest.json pointer under truth root"

# PROOF: truth root exists
if [[ ! -d "${TRUTH_ROOT}" ]]; then
  echo "FAIL: truth root missing: ${TRUTH_ROOT}" >&2
  exit 2
fi

# Discover all latest.json under truth root
LATEST_LIST="$(find "${TRUTH_ROOT}" -type f -name 'latest.json' 2>/dev/null | sort || true)"
LATEST_COUNT="$(printf "%s\n" "${LATEST_LIST}" | sed '/^\s*$/d' | wc -l | tr -d ' ')"

ALLOWED="${TRUTH_ROOT}/latest.json"

if [[ "${LATEST_COUNT}" -eq 0 ]]; then
  echo "FAIL: no latest.json exists under truth root; expected exactly 1 at ${ALLOWED}" >&2
  exit 2
fi

if [[ "${LATEST_COUNT}" -ne 1 ]]; then
  echo "FAIL: latest.json fan-out detected under truth root (count=${LATEST_COUNT}); expected exactly 1" >&2
  echo "${LATEST_LIST}" >&2
  exit 2
fi

if [[ "${LATEST_LIST}" != "${ALLOWED}" ]]; then
  echo "FAIL: only allowed latest.json path is ${ALLOWED}; found: ${LATEST_LIST}" >&2
  exit 2
fi

# Validate pointer content shape minimally (no jq required):
# Must reference gate_stack_verdict_v1 artifact path somewhere in the file.
if ! grep -q "gate_stack_verdict_v1" "${ALLOWED}"; then
  echo "FAIL: run pointer does not reference gate_stack_verdict_v1: ${ALLOWED}" >&2
  echo "HINT: ${ALLOWED} must point to reports/gate_stack_verdict_v1/<DAY>/gate_stack_verdict.v1.json" >&2
  exit 2
fi

echo "[c2-preflight] PASS: single latest.json run pointer enforced"
