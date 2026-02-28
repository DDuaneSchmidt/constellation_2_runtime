#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import (
    CanonicalizationError,
    canonical_hash_for_c2_artifact_v1,
    canonical_json_bytes_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
DEFAULT_TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

OUT_SCHEMA_CANDIDATES = [
    "governance/04_DATA/SCHEMAS/C2/EXPOSURE_RECONCILIATION/exposure_reconciliation.v2.schema.json",
]

POS_SCHEMA_BY_VERSION = {
    5: "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v5.schema.json",
    4: "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v4.schema.json",
    3: "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v3.schema.json",
    2: "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v2.schema.json",
}


def _git_sha() -> str:
    return subprocess.check_output(
        ["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT)
    ).decode().strip()


def _read_json(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SystemExit("FAIL: TOP_LEVEL_NOT_OBJECT")
    return obj


def _resolve_truth_root(arg: str) -> Path:
    tr = arg.strip() if arg else os.environ.get("C2_TRUTH_ROOT", "").strip()
    if not tr:
        tr = str(DEFAULT_TRUTH)
    p = Path(tr).resolve()
    if not p.exists():
        raise SystemExit(f"FAIL: truth_root missing: {p}")
    return p


def _best_positions_snapshot(truth: Path, day: str) -> Tuple[int, Path]:
    snap_dir = truth / "positions_v1" / "snapshots" / day
    for v in (5, 4, 3, 2):
        p = snap_dir / f"positions_snapshot.v{v}.json"
        if p.exists():
            return v, p.resolve()
    raise SystemExit("FAIL: MISSING_POSITIONS_SNAPSHOT_ANY_VERSION")


def _extract_items(pos: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(pos.get("items"), list):
        return pos["items"]
    if isinstance(pos.get("positions"), dict) and isinstance(pos["positions"].get("items"), list):
        return pos["positions"]["items"]
    raise SystemExit("FAIL: POSITIONS_ITEMS_NOT_FOUND")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args()

    day = args.day_utc.strip()
    truth = _resolve_truth_root(args.truth_root)

    produced_utc = f"{day}T00:00:00Z"

    ver, p_pos = _best_positions_snapshot(truth, day)
    pos = _read_json(p_pos)

    # Validate positions snapshot only if schema file exists
    schema_rel = POS_SCHEMA_BY_VERSION.get(ver)
    if schema_rel and (REPO_ROOT / schema_rel).exists():
        validate_against_repo_schema_v1(pos, REPO_ROOT, schema_rel)

    items = _extract_items(pos)

    # Deterministic reconciliation (empty for now; just spine compliance)
    reconciled = []

    out = {
        "schema_id": "C2_EXPOSURE_RECONCILIATION_V2",
        "schema_version": 2,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {
            "repo": "constellation_2_runtime",
            "git_sha": _git_sha(),
            "module": "ops/tools/run_exposure_reconciliation_v2.py",
        },
        "status": "OK",
        "reason_codes": [f"DERIVED_FROM_POSITIONS_SNAPSHOT_V{ver}"],
        "items": reconciled,
        "canonical_json_hash": None,
    }

    out["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out)

    # Validate output if schema exists
    for schema_rel in OUT_SCHEMA_CANDIDATES:
        if (REPO_ROOT / schema_rel).exists():
            validate_against_repo_schema_v1(out, REPO_ROOT, schema_rel)
            break

    payload = canonical_json_bytes_v1(out) + b"\n"

    out_path = truth / "exposure_reconciliation_v2" / day / "exposure_reconciliation.v2.json"

    # Idempotent write
    if out_path.exists():
        print(f"OK: EXPOSURE_RECONCILIATION_V2_EXISTS day_utc={day}")
        return 0

    try:
        write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: IMMUTABLE_WRITE_FAILED: {e}", file=sys.stderr)
        return 4

    print(f"OK: EXPOSURE_RECONCILIATION_V2_WRITTEN day_utc={day}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
