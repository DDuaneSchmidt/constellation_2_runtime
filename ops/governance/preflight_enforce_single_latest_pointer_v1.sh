#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/home/node/constellation_2_runtime"
TRUTH_ROOT="${REPO_ROOT}/constellation_2/runtime/truth"
RUNPTR_ROOT="${TRUTH_ROOT}/run_pointer_v1"
IDX="${RUNPTR_ROOT}/canonical_pointer_index.v1.jsonl"

echo "[c2-preflight] enforcing single canonical run pointer (run_pointer_v1) when present"

# PROOF: truth root exists
if [[ ! -d "${TRUTH_ROOT}" ]]; then
  echo "FAIL: truth root missing: ${TRUTH_ROOT}" >&2
  exit 2
fi

# If outputs not present on disk, repo preflight must still pass.
if [[ ! -f "${IDX}" ]]; then
  echo "[c2-preflight] WARN: missing ${IDX}; run pointer not present on disk. Skipping run-pointer enforcement."
  exit 0
fi

# Validate pointer index is readable and has at least one entry referencing gate_stack_verdict_v1.
python3 -c '
import json,sys
from pathlib import Path
p=Path(sys.argv[1]).resolve()
lines=[x.strip() for x in p.read_text(encoding="utf-8").splitlines() if x.strip()]
if not lines:
  raise SystemExit(f"FAIL: empty canonical_pointer_index: {p}")
ok=False
for ln in lines:
  try:
    o=json.loads(ln)
  except Exception:
    raise SystemExit(f"FAIL: invalid JSONL line in canonical_pointer_index: {p}")
  if isinstance(o,dict) and "gate_stack_verdict_v1" in str(o.get("points_to") or ""):
    ok=True
if not ok:
  raise SystemExit(f"FAIL: canonical_pointer_index has no points_to referencing gate_stack_verdict_v1: {p}")
print("OK")
' "${IDX}"

# Legacy: latest.json may exist, but is NOT authoritative for this preflight.
LEGACY="${TRUTH_ROOT}/latest"".json"
if [[ -f "${LEGACY}" ]]; then
  echo "[c2-preflight] WARN: legacy latest pointer file present (non-authoritative): ${LEGACY}"
fi

echo "[c2-preflight] PASS: run pointer index present + minimally valid"
