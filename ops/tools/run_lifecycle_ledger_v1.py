#!/usr/bin/env python3
"""
run_lifecycle_ledger_v1.py

Bundled C: position_lifecycle_ledger.v1.json writer (immutable truth artifact).

Enforces legal lifecycle transitions and emits explicit close-request states based on delta order plan.

Writes:
  constellation_2/runtime/truth/position_lifecycle_v1/ledger/<DAY>/position_lifecycle_ledger.v1.json

Validates schema:
  governance/04_DATA/SCHEMAS/C2/POSITIONS/position_lifecycle_ledger.v1.schema.json

Run:
  python3 ops/tools/run_lifecycle_ledger_v1.py --day_utc YYYY-MM-DD
"""

from __future__ import annotations

# --- Import bootstrap (audit-grade, deterministic, fail-closed) ---
import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")
if not (_REPO_ROOT_FROM_FILE / "governance").exists():
    raise SystemExit(f"FATAL: repo_root_missing_governance: derived={_REPO_ROOT_FROM_FILE}")

import argparse
import hashlib
import json
import subprocess
from typing import Any, Dict, List

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_LEDGER = "governance/04_DATA/SCHEMAS/C2/POSITIONS/position_lifecycle_ledger.v1.schema.json"

POS_PTR_ROOT = (TRUTH / "positions_v1" / "effective_v1" / "days").resolve()
OUT_ROOT = (TRUTH / "position_lifecycle_v1" / "ledger").resolve()
DELTA_PLAN_ROOT = (TRUTH / "reports" / "delta_order_plan_v1").resolve()


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ValueError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json_obj(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(p)}")
    return obj


def _canonical_bytes(obj: Dict[str, Any]) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _compute_self_sha(obj: Dict[str, Any], field: str) -> str:
    o2 = dict(obj)
    o2[field] = None
    return _sha256_bytes(_canonical_bytes(o2))


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_lifecycle_ledger_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    day = _parse_day_utc(args.day_utc)

    # Deterministic produced_utc for replay (schema requires non-empty string).
    produced_utc = f"{day}T00:00:00Z"

    ptr_path = (POS_PTR_ROOT / day / "positions_effective_pointer.v1.json").resolve()
    plan_path = (DELTA_PLAN_ROOT / day / "delta_order_plan.v1.json").resolve()

    input_manifest: List[Dict[str, str]] = []
    reason_codes: List[str] = []

    def add(t: str, p: Path) -> None:
        if p.exists() and p.is_file():
            input_manifest.append({"type": t, "path": str(p), "sha256": _sha256_file(p)})
        else:
            input_manifest.append({"type": f"{t}_missing", "path": str(p), "sha256": _sha256_bytes(b"")})
            reason_codes.append("C2_LIFECYCLE_INPUTS_MISSING_FAILCLOSED")

    add("positions_effective_pointer", ptr_path)
    add("delta_order_plan_v1", plan_path)

    pos_items: List[Dict[str, Any]] = []
    if ptr_path.exists():
        try:
            ptr = _read_json_obj(ptr_path)
            sp = ptr.get("pointers", {}).get("snapshot_path")
            if not isinstance(sp, str) or not sp.strip():
                raise ValueError("POSITIONS_EFFECTIVE_POINTER_MISSING_SNAPSHOT_PATH")
            snap_path = Path(sp).resolve()
            add("positions_snapshot", snap_path)
            if snap_path.exists():
                snap = _read_json_obj(snap_path)
                items = snap.get("positions", {}).get("items", [])
                if isinstance(items, list):
                    pos_items = [x for x in items if isinstance(x, dict)]
        except Exception:
            reason_codes.append("C2_LIFECYCLE_INPUTS_MISSING_FAILCLOSED")

    close_underlyings: set[str] = set()
    if plan_path.exists():
        try:
            plan = _read_json_obj(plan_path)
            orders = plan.get("orders", [])
            if isinstance(orders, list):
                for o in orders:
                    if not isinstance(o, dict):
                        continue
                    if str(o.get("action") or "") == "CLOSE":
                        u = str(o.get("underlying") or "").strip()
                        if u:
                            close_underlyings.add(u)
        except Exception:
            reason_codes.append("C2_LIFECYCLE_INPUTS_MISSING_FAILCLOSED")

    pos_items_sorted = sorted(pos_items, key=lambda x: str(x.get("position_id") or ""))

    positions_out: List[Dict[str, Any]] = []
    status = "OK"

    for it in pos_items_sorted:
        pid = str(it.get("position_id") or "").strip()
        if not pid:
            continue

        prior_state = "UNKNOWN"
        new_state = "OPEN" if str(it.get("status") or "").strip().upper() == "OPEN" else "UNKNOWN"
        transition = "NOOP"
        legal = True
        rcs: List[str] = []
        pointers: List[str] = []

        inst = it.get("instrument", {})
        und = str(inst.get("underlying") or "").strip()

        if new_state == "OPEN" and und and und in close_underlyings:
            new_state = "CLOSING_REQUESTED"
            transition = "REQUEST_CLOSE"
            rcs.append("C2_EXPOSURE_KILL_SWITCH_FORCES_FLATTEN")

        allowed = {
            ("UNKNOWN", "OPEN"),
            ("UNKNOWN", "UNKNOWN"),
            ("OPEN", "OPEN"),
            ("OPEN", "CLOSING_REQUESTED"),
            ("CLOSING_REQUESTED", "CLOSING_REQUESTED"),
            ("CLOSING_REQUESTED", "CLOSED"),
            ("UNKNOWN", "CLOSING_REQUESTED"),
            ("UNKNOWN", "CLOSED"),
        }
        if (prior_state, new_state) not in allowed:
            legal = False
            transition = "ILLEGAL"
            rcs.append("C2_LIFECYCLE_ILLEGAL_TRANSITION_FAILCLOSED")
            status = "FAIL"

        positions_out.append(
            {
                "position_id": pid,
                "prior_state": prior_state,
                "new_state": new_state,
                "transition": transition,
                "legal": legal,
                "reason_codes": rcs,
                "pointers": pointers,
            }
        )

    reason_codes = sorted(list(dict.fromkeys(reason_codes)))

    payload: Dict[str, Any] = {
        "schema_id": "position_lifecycle_ledger",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_lifecycle_ledger_v1.py", "git_sha": _git_sha()},
        "status": status,
        "reason_codes": reason_codes,
        "input_manifest": input_manifest if input_manifest else [{"type": "none", "path": "<none>", "sha256": _sha256_bytes(b"")}],
        "positions": positions_out,
        "ledger_sha256": None,
    }
    payload["ledger_sha256"] = _compute_self_sha(payload, "ledger_sha256")

    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_LEDGER)

    out_path = (OUT_ROOT / day / "position_lifecycle_ledger.v1.json").resolve()
    try:
        wr = write_file_immutable_v1(path=out_path, data=_canonical_bytes(payload), create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}") from e

    print(f"OK: POSITION_LIFECYCLE_LEDGER_WRITTEN day_utc={day} status={status} path={wr.path} sha256={wr.sha256} action={wr.action}")
    return 0 if status == "OK" else 2


if __name__ == "__main__":
    raise SystemExit(main())
