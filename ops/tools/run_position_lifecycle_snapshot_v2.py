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

SCHEMA = "governance/04_DATA/SCHEMAS/C2/POSITION_LIFECYCLE/position_lifecycle_snapshot.v2.schema.json"
POS_V5_PATH = TRUTH / "positions_v1" / "snapshots"


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _read_json_obj(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        o = json.load(f)
    if not isinstance(o, dict):
        raise ValueError("TOP_LEVEL_NOT_OBJECT")
    return o


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_position_lifecycle_snapshot_v2")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()

    day = args.day_utc.strip()
    p_pos = (POS_V5_PATH / day / "positions_snapshot.v5.json").resolve()
    if not p_pos.exists():
        print(f"FAIL: MISSING_POSITIONS_SNAPSHOT_V5: {str(p_pos)}", file=sys.stderr)
        return 2

    pos = _read_json_obj(p_pos)
    # positions_snapshot.v5 schema is governed; validate if present
    validate_against_repo_schema_v1(pos, REPO_ROOT, "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v5.schema.json")

    items_in = pos.get("items")
    if not isinstance(items_in, list):
        print("FAIL: POSITIONS_ITEMS_NOT_LIST", file=sys.stderr)
        return 2

    now_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    out_items: List[Dict[str, Any]] = []

    # Deterministic state mapping (v2 baseline):
    # - Any position present in positions snapshot is at least MANAGING
    # - If kill switch active, later stages will upgrade to FORCED_CLOSE_PENDING (done by obligations + monitor)
    for it in items_in:
        if not isinstance(it, dict):
            print("FAIL: POSITION_ITEM_NOT_OBJECT", file=sys.stderr)
            return 2

        position_id = str(it.get("position_id") or "").strip()
        engine_id = str(it.get("engine_id") or "").strip()
        source_intent_id = str(it.get("source_intent_id") or "").strip()
        intent_sha256 = str(it.get("intent_sha256") or "").strip()

        if not position_id or not engine_id or not source_intent_id or len(intent_sha256) != 64:
            print("FAIL: POSITION_ATTRIBUTION_MISSING_IN_POSITIONS_V5", file=sys.stderr)
            return 2

        out_items.append(
            {
                "position_id": position_id,
                "engine_id": engine_id,
                "source_intent_id": source_intent_id,
                "intent_sha256": intent_sha256,
                "lifecycle_state": "MANAGING",
                "lifecycle_reason_code": "PRESENT_IN_POSITIONS_SNAPSHOT_V5",
                "opened_day_utc": str(it.get("opened_day_utc") or day),
                "last_transition_utc": now_utc,
                "exit_policy_ref": None,
                "regime_snapshot_ref": None,
                "kill_switch_override": False
            }
        )

    produced_utc = now_utc
    out: Dict[str, Any] = {
        "schema_id": "C2_POSITION_LIFECYCLE_SNAPSHOT",
        "schema_version": 2,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_position_lifecycle_snapshot_v2.py"},
        "status": "OK",
        "reason_codes": ["DERIVED_FROM_POSITIONS_SNAPSHOT_V5"],
        "items": out_items,
        "canonical_json_hash": None
    }

    out["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out)
    validate_against_repo_schema_v1(out, REPO_ROOT, SCHEMA)

    try:
        payload = canonical_json_bytes_v1(out) + b"\n"
    except CanonicalizationError as e:
        print(f"FAIL: CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4

    out_path = (TRUTH / "position_lifecycle_v2" / day / "position_lifecycle_snapshot.v2.json").resolve()
    try:
        _ = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: IMMUTABLE_WRITE_FAILED: {e}", file=sys.stderr)
        return 4

    print("OK: POSITION_LIFECYCLE_SNAPSHOT_V2_WRITTEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
