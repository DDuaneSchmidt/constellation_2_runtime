#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, DefaultDict
from collections import defaultdict

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA = "governance/04_DATA/SCHEMAS/C2/EXPOSURE_RECONCILIATION/exposure_reconciliation.v2.schema.json"


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
    ap = argparse.ArgumentParser(prog="run_exposure_reconciliation_v2")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()
    day = args.day_utc.strip()

    p_pos = (TRUTH / "positions_v1/snapshots" / day / "positions_snapshot.v5.json").resolve()
    p_obl = (TRUTH / "exit_obligations_v1" / day / "exit_obligations.v1.json").resolve()

    if not p_pos.exists():
        print(f"FAIL: MISSING_POSITIONS_SNAPSHOT_V5: {str(p_pos)}", file=sys.stderr)
        return 2
    if not p_obl.exists():
        print(f"FAIL: MISSING_EXIT_OBLIGATIONS_V1: {str(p_obl)}", file=sys.stderr)
        return 2

    pos = _read_json_obj(p_pos)
    validate_against_repo_schema_v1(pos, REPO_ROOT, "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v5.schema.json")
    obl = _read_json_obj(p_obl)
    validate_against_repo_schema_v1(obl, REPO_ROOT, "governance/04_DATA/SCHEMAS/C2/EXIT_OBLIGATIONS/exit_obligations.v1.schema.json")

    items = pos.get("items")
    if not isinstance(items, list):
        print("FAIL: POSITIONS_ITEMS_NOT_LIST", file=sys.stderr)
        return 2

    obligations = obl.get("obligations")
    if not isinstance(obligations, list):
        print("FAIL: OBLIGATIONS_NOT_LIST", file=sys.stderr)
        return 2

    actual_by_engine: DefaultDict[str, int] = defaultdict(int)
    orphaned_by_engine: DefaultDict[str, int] = defaultdict(int)
    for it in items:
        if not isinstance(it, dict):
            print("FAIL: POSITION_ITEM_NOT_OBJECT", file=sys.stderr)
            return 2
        eng = str(it.get("engine_id") or "").strip()
        if not eng:
            eng = "ORPHANED"
        actual_by_engine[eng] += 1
        if eng == "ORPHANED":
            orphaned_by_engine[eng] += 1

    obl_by_engine: DefaultDict[str, int] = defaultdict(int)
    for o in obligations:
        if not isinstance(o, dict):
            print("FAIL: OBLIGATION_ITEM_NOT_OBJECT", file=sys.stderr)
            return 2
        eng = str(o.get("engine_id") or "").strip()
        if not eng:
            eng = "ORPHANED"
        obl_by_engine[eng] += 1

    engines_out: List[Dict[str, Any]] = []
    reason_codes: List[str] = ["COUNT_BASED_RECONCILIATION_V2_BASELINE"]
    status = "OK"

    # Baseline desired_positions unknown until intent delta integration; set desired=actual for now
    # Breach defined if obligations exist or orphaned positions exist
    engine_ids = sorted(set(list(actual_by_engine.keys()) + list(obl_by_engine.keys())))
    for eng in engine_ids:
        actual = int(actual_by_engine.get(eng, 0))
        desired = int(actual)  # baseline
        delta = int(desired - actual)
        orphaned = int(orphaned_by_engine.get(eng, 0))
        obl_count = int(obl_by_engine.get(eng, 0))
        breach = bool(obl_count > 0 or orphaned > 0)
        if breach:
            status = "FAIL"
        engines_out.append(
            {
                "engine_id": eng,
                "desired_positions": desired,
                "actual_positions": actual,
                "delta_positions": delta,
                "orphaned_positions": orphaned,
                "exit_obligation_count": obl_count,
                "breach_flag": breach
            }
        )

    if status != "OK":
        reason_codes.append("RECONCILIATION_BREACH")

    produced_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    out: Dict[str, Any] = {
        "schema_id": "C2_EXPOSURE_RECONCILIATION",
        "schema_version": 2,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_exposure_reconciliation_v2.py"},
        "status": status,
        "reason_codes": reason_codes,
        "engines": engines_out,
        "canonical_json_hash": None
    }
    out["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out)
    validate_against_repo_schema_v1(out, REPO_ROOT, SCHEMA)

    try:
        payload = canonical_json_bytes_v1(out) + b"\n"
    except CanonicalizationError as e:
        print(f"FAIL: CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4

    out_path = (TRUTH / "exposure_reconciliation_v2" / day / "exposure_reconciliation.v2.json").resolve()
    try:
        _ = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: IMMUTABLE_WRITE_FAILED: {e}", file=sys.stderr)
        return 4

    if status != "OK":
        print("FAIL: EXPOSURE_RECONCILIATION_V2")
        return 2

    print("OK: EXPOSURE_RECONCILIATION_V2_WRITTEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
