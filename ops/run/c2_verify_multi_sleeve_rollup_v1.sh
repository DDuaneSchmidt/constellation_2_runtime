#!/usr/bin/env bash
set -euo pipefail

cd /home/node/constellation_2_runtime

DAY_UTC="${DAY_UTC:-$(date -u +%F)}"
ROOT="/home/node/constellation_2_runtime/constellation_2/runtime/truth/reports/sleeve_rollup_v1/${DAY_UTC}"
IDX="${ROOT}/canonical_pointer_index.v1.jsonl"

export DAY_UTC
export ROOT
export IDX

# Fail-closed: must exist
test -f "${IDX}"

# Extract the last non-empty line (fail if empty)
LAST_LINE="$(tac "${IDX}" | rg -m1 -n "." | head -n 1)"
test -n "${LAST_LINE}"

# Parse JSON (fail closed)
python3 - <<PY
import json, sys, os, hashlib

day = os.environ["DAY_UTC"]
root = os.environ["ROOT"]
idx = os.environ["IDX"]

# Load last pointer line
lines = [ln.strip() for ln in open(idx, "r", encoding="utf-8").read().splitlines() if ln.strip()]
if not lines:
    raise SystemExit("FAIL: empty_pointer_index")

ptr = json.loads(lines[-1])
if ptr.get("schema_id") != "C2_SLEEVE_ROLLUP_POINTER_INDEX_V1":
    raise SystemExit(f"FAIL: bad_pointer_schema_id: {ptr.get('schema_id')!r}")

points_to = ptr.get("points_to")
if not isinstance(points_to, str) or not points_to.strip():
    raise SystemExit("FAIL: points_to missing")

if not os.path.isfile(points_to):
    raise SystemExit(f"FAIL: points_to missing file: {points_to}")

# Verify sha if present
want = ptr.get("points_to_sha256")
if isinstance(want, str) and want.strip():
    h = hashlib.sha256(open(points_to, "rb").read()).hexdigest()
    if h != want:
        raise SystemExit(f"FAIL: points_to_sha256 mismatch: want={want} got={h}")

# Load rollup json
rollup = json.load(open(points_to, "r", encoding="utf-8"))
if rollup.get("schema_id") != "C2_SLEEVE_ROLLUP_V1":
    raise SystemExit(f"FAIL: bad_rollup_schema_id: {rollup.get('schema_id')!r}")

status = str(rollup.get("status") or "").strip().upper()
if status not in ("PASS","DEGRADED","FAIL","ABORTED"):
    raise SystemExit(f"FAIL: bad_rollup_status: {status!r}")

sleeves = rollup.get("sleeves")
if not isinstance(sleeves, list) or not sleeves:
    raise SystemExit("FAIL: rollup missing sleeves")

bad = []
for s in sleeves:
    if not isinstance(s, dict):
        continue
    sid = str(s.get("sleeve_id") or "").strip() or "UNKNOWN"
    vs = str(s.get("verdict_status") or "").strip().upper()
    if vs in ("ABORTED","FAIL"):
        bad.append((sid, vs))

# Policy: ABORTED/FAIL are hard failures
if bad:
    raise SystemExit("FAIL: sleeve_verdicts_bad: " + ", ".join([f"{sid}:{vs}" for sid,vs in bad]))

print("OK: multi_sleeve_rollup_verified day_utc=" + day + " status=" + status)
PY
