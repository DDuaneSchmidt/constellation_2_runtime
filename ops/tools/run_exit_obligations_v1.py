#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA = "governance/04_DATA/SCHEMAS/C2/EXIT_OBLIGATIONS/exit_obligations.v1.schema.json"
LIFE_PATH = TRUTH / "position_lifecycle_v2"
KILL_PATH = TRUTH / "risk_v1" / "kill_switch_v1"


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _read_json_obj(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        o = json.load(f)
    if not isinstance(o, dict):
        raise ValueError("TOP_LEVEL_NOT_OBJECT")
    return o


def _kill_switch_active(day: str) -> bool:
    p = (KILL_PATH / day / "global_kill_switch_state.v1.json").resolve()
    if not p.exists():
        # fail-closed policy: missing kill switch treated as active by PhaseD; here we mirror conservative stance
        return True
    o = _read_json_obj(p)
    # accept either {"active": true} or {"status":"ACTIVE"} patterns
    if isinstance(o.get("active"), bool):
        return bool(o["active"])
    s = str(o.get("status") or "").strip().upper()
    if s in ("ACTIVE", "ON", "TRUE"):
        return True
    if s in ("INACTIVE", "OFF", "FALSE"):
        return False
    # unknown => fail-closed => active
    return True


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_exit_obligations_v1")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()
    day = args.day_utc.strip()

    p_life = (LIFE_PATH / day / "position_lifecycle_snapshot.v2.json").resolve()
    if not p_life.exists():
        print(f"FAIL: MISSING_POSITION_LIFECYCLE_SNAPSHOT_V2: {str(p_life)}", file=sys.stderr)
        return 2

    life = _read_json_obj(p_life)
    validate_against_repo_schema_v1(life, REPO_ROOT, "governance/04_DATA/SCHEMAS/C2/POSITION_LIFECYCLE/position_lifecycle_snapshot.v2.schema.json")

    items = life.get("items")
    if not isinstance(items, list):
        print("FAIL: LIFECYCLE_ITEMS_NOT_LIST", file=sys.stderr)
        return 2

    now_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    kill_active = _kill_switch_active(day)

    obligations: List[Dict[str, Any]] = []
    reason_codes: List[str] = []
    status = "OK"

    if kill_active:
        reason_codes.append("KILL_SWITCH_ACTIVE_FAILCLOSED")
        # Force close all positions deterministically
        for it in items:
            if not isinstance(it, dict):
                status = "FAIL"
                reason_codes.append("LIFECYCLE_ITEM_NOT_OBJECT")
                continue
            obligations.append(
                {
                    "position_id": str(it["position_id"]),
                    "engine_id": str(it["engine_id"]),
                    "obligation_type": "FORCED_CLOSE",
                    "required_qty": 0,
                    "deadline_utc": now_utc,
                    "reason_code": "KILL_SWITCH_ACTIVE",
                    "regime_condition": None,
                    "kill_switch_condition": "ACTIVE_OR_MISSING"
                }
            )
    else:
        reason_codes.append("NO_KILL_SWITCH_FORCED_OBLIGATIONS")
        # Baseline: obligations empty until desired exposure delta integration is added.
        obligations = []

    out: Dict[str, Any] = {
        "schema_id": "C2_EXIT_OBLIGATIONS",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": now_utc,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_exit_obligations_v1.py"},
        "status": status,
        "reason_codes": reason_codes,
        "obligations": obligations,
        "canonical_json_hash": None
    }
    out["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out)
    validate_against_repo_schema_v1(out, REPO_ROOT, SCHEMA)

    try:
        payload = canonical_json_bytes_v1(out) + b"\n"
    except CanonicalizationError as e:
        print(f"FAIL: CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4

    out_path = (TRUTH / "exit_obligations_v1" / day / "exit_obligations.v1.json").resolve()
    try:
        _ = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: IMMUTABLE_WRITE_FAILED: {e}", file=sys.stderr)
        return 4

    if status != "OK":
        print("FAIL: EXIT_OBLIGATIONS_V1")
        return 2

    print("OK: EXIT_OBLIGATIONS_V1_WRITTEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
