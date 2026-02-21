#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/home/node/constellation_2_runtime"
GOV_ROOT="${REPO_ROOT}/governance"
MANIFEST="${GOV_ROOT}/00_MANIFEST.yaml"
INDEX="${GOV_ROOT}/00_INDEX.md"
TRUTH_ROOT="${REPO_ROOT}/constellation_2/runtime/truth"

echo "[c2-preflight] repo_root=${REPO_ROOT}"
echo "[c2-preflight] governance_root=${GOV_ROOT}"
echo "[c2-preflight] truth_root=${TRUTH_ROOT}"

# 1) Hard anchor: must be run from inside the repo root path.
PWD_ACTUAL="$(pwd)"
if [[ "${PWD_ACTUAL}" != "${REPO_ROOT}" ]]; then
  echo "FAIL: wrong working directory: pwd=${PWD_ACTUAL} expected=${REPO_ROOT}" >&2
  exit 2
fi

# 2) Required governance anchors must exist.
if [[ ! -f "${MANIFEST}" ]]; then
  echo "FAIL: missing governance manifest: ${MANIFEST}" >&2
  exit 2
fi
if [[ ! -f "${INDEX}" ]]; then
  echo "FAIL: missing governance index: ${INDEX}" >&2
  exit 2
fi

# 3) Truth root must exist and must be a directory.
if [[ ! -d "${TRUTH_ROOT}" ]]; then
  echo "FAIL: missing truth root directory: ${TRUTH_ROOT}" >&2
  exit 2
fi

# 4) Git cleanliness gate (fail-closed): no unstaged/uncommitted changes.
#    (Operator can intentionally bypass by running without preflight; this is the gate.)
if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "FAIL: not a git work tree" >&2
  exit 2
fi

PORCELAIN="$(git status --porcelain=v1 || true)"
if [[ -n "${PORCELAIN}" ]]; then
  echo "FAIL: working tree not clean" >&2
  echo "${PORCELAIN}" >&2
  exit 2
fi

# 5) Minimal "manifest contains the index" consistency check (no YAML parser required).
#    We only enforce that both anchor paths appear somewhere in the manifest text.
if ! grep -q "path: governance/00_INDEX.md" "${MANIFEST}"; then
  echo "FAIL: manifest does not reference governance/00_INDEX.md" >&2
  exit 2
fi
if ! grep -q "path: docs/c2/F_ACCOUNTING_SPINE_V1.md" "${MANIFEST}"; then
  echo "FAIL: manifest missing expected FG doc placeholder entry (F_ACCOUNTING_SPINE_V1)" >&2
  exit 2
fi

# 6) Structural invariant preflights (fail-closed)
"${REPO_ROOT}/ops/governance/preflight_require_kill_switch_uses_gate_stack_verdict_v1.sh"
"${REPO_ROOT}/ops/governance/preflight_forbid_operator_gate_verdict_surfaces_v1.sh"
bash "${REPO_ROOT}/ops/governance/preflight_enforce_spine_exclusivity_v1.sh"
bash "${REPO_ROOT}/ops/governance/preflight_enforce_single_latest_pointer_v1.sh"

echo "[c2-preflight] PASS"
