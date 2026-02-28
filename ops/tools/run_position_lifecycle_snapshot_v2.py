#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import (
    CanonicalizationError,
    canonical_hash_for_c2_artifact_v1,
    canonical_json_bytes_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
DEFAULT_TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

OUT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/POSITION_LIFECYCLE/position_lifecycle_snapshot.v2.schema.json"

# Candidate input schemas by version (validated only if schema file exists on disk)
POS_SNAPSHOT_SCHEMA_BY_VERSION: Dict[int, str] = {
    5: "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v5.schema.json",
    4: "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v4.schema.json",
    3: "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v3.schema.json",
    2: "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v2.schema.json",
}


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _read_json_obj(path: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        raise SystemExit(f"FAIL: JSON_READ_OR_PARSE_FAILED: path={path} err={e!r}") from e
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: TOP_LEVEL_NOT_OBJECT: path={path}")
    return obj


def _resolve_truth_root(args_truth_root: str) -> Path:
    """
    Deterministic truth_root resolution order:
      1) --truth_root if provided
      2) env C2_TRUTH_ROOT if set
      3) DEFAULT_TRUTH (canonical)
    Hard guard: truth_root must be under repo root.
    """
    tr = (args_truth_root or "").strip()
    if not tr:
        tr = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if not tr:
        tr = str(DEFAULT_TRUTH)

    truth_root = Path(tr).resolve()
    if not truth_root.exists() or not truth_root.is_dir():
        raise SystemExit(f"FATAL: truth_root missing or not directory: {truth_root}")

    try:
        truth_root.relative_to(REPO_ROOT)
    except Exception:
        raise SystemExit(f"FATAL: truth_root not under repo root: truth_root={truth_root} repo_root={REPO_ROOT}")

    return truth_root


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _best_positions_snapshot_path(truth: Path, day: str) -> Tuple[int, Path]:
    """
    Prefer highest version available: v5 -> v4 -> v3 -> v2.
    """
    snap_dir = (truth / "positions_v1" / "snapshots" / day).resolve()
    for v in (5, 4, 3, 2):
        p = (snap_dir / f"positions_snapshot.v{v}.json").resolve()
        if p.exists() and p.is_file():
            return (v, p)
    raise SystemExit(f"FAIL: MISSING_POSITIONS_SNAPSHOT_ANY_VERSION: dir={snap_dir}")


def _validate_positions_snapshot_if_schema_present(pos_obj: Dict[str, Any], version: int) -> List[str]:
    """
    Validate the positions snapshot against governed schema *only if the schema file exists on disk*.
    Returns reason_codes additions.
    """
    reasons: List[str] = []
    schema_rel = POS_SNAPSHOT_SCHEMA_BY_VERSION.get(version)
    if not schema_rel:
        reasons.append(f"POSITIONS_SCHEMA_UNKNOWN_FOR_V{version}")
        return reasons

    schema_abs = (REPO_ROOT / schema_rel).resolve()
    if not schema_abs.exists():
        reasons.append(f"POSITIONS_SCHEMA_MISSING_SKIP_VALIDATION_V{version}")
        return reasons

    validate_against_repo_schema_v1(pos_obj, REPO_ROOT, schema_rel)
    reasons.append(f"POSITIONS_SCHEMA_VALIDATED_V{version}")
    return reasons


def _extract_items_from_positions_snapshot(pos_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Supports multiple historical shapes:
      - v5: items likely at pos["items"]
      - v2/v3: positions block may be pos["positions"]["items"]
    """
    if isinstance(pos_obj.get("items"), list):
        items = pos_obj.get("items")
        return [x for x in items if isinstance(x, dict)]

    positions = pos_obj.get("positions")
    if isinstance(positions, dict) and isinstance(positions.get("items"), list):
        items2 = positions.get("items")
        return [x for x in items2 if isinstance(x, dict)]

    raise SystemExit("FAIL: POSITIONS_ITEMS_NOT_FOUND_IN_SUPPORTED_LOCATIONS")


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _safe_sha256_or_zeros(v: Any) -> str:
    s = _safe_str(v)
    if len(s) == 64 and all(c in "0123456789abcdef" for c in s.lower()):
        return s.lower()
    return "0" * 64


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_position_lifecycle_snapshot_v2")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument(
        "--truth_root",
        default="",
        help="Override truth root (must be under repo root). If omitted, uses env C2_TRUTH_ROOT, else canonical.",
    )
    args = ap.parse_args()

    day = _parse_day_utc(str(args.day_utc))
    truth = _resolve_truth_root(str(args.truth_root))

    produced_utc = f"{day}T00:00:00Z"
    last_transition_utc = produced_utc

    ver, p_pos = _best_positions_snapshot_path(truth, day)
    pos = _read_json_obj(p_pos)

    reason_codes: List[str] = [f"DERIVED_FROM_POSITIONS_SNAPSHOT_V{ver}"]
    # Validate snapshot only if schema file exists
    reason_codes.extend(_validate_positions_snapshot_if_schema_present(pos, ver))

    items_in = _extract_items_from_positions_snapshot(pos)

    out_items: List[Dict[str, Any]] = []
    for it in items_in:
        position_id = _safe_str(it.get("position_id") or it.get("positionId") or "")
        engine_id = _safe_str(it.get("engine_id") or it.get("engineId") or "unknown")
        opened_day_utc = _safe_str(it.get("opened_day_utc") or it.get("openedDayUtc") or day)

        # Attribution (may be missing in v2/v3). We synthesize placeholders to keep lifecycle spine runnable.
        source_intent_id = _safe_str(it.get("source_intent_id") or it.get("sourceIntentId") or "")
        intent_sha256 = _safe_sha256_or_zeros(it.get("intent_sha256") or it.get("intentSha256") or "")

        if not position_id:
            # fail-closed: cannot build lifecycle state without a stable position_id
            raise SystemExit("FAIL: POSITION_ID_MISSING_IN_POSITIONS_SNAPSHOT")

        if not source_intent_id:
            source_intent_id = f"UNKNOWN_INTENT:{position_id}"
            reason_codes.append("MISSING_ATTRIBUTION_SOURCE_INTENT_ID_SYNTHESIZED")

        if intent_sha256 == "0" * 64:
            reason_codes.append("MISSING_ATTRIBUTION_INTENT_SHA256_SYNTHESIZED_ZEROS")

        out_items.append(
            {
                "position_id": position_id,
                "engine_id": engine_id if engine_id else "unknown",
                "source_intent_id": source_intent_id,
                "intent_sha256": intent_sha256,
                "lifecycle_state": "MANAGING",
                "lifecycle_reason_code": f"PRESENT_IN_POSITIONS_SNAPSHOT_V{ver}",
                "opened_day_utc": opened_day_utc if opened_day_utc else day,
                "last_transition_utc": last_transition_utc,
                "exit_policy_ref": None,
                "regime_snapshot_ref": None,
                "kill_switch_override": False,
            }
        )

    # stable de-dupe reason_codes
    seen = set()
    reason_codes_stable: List[str] = []
    for r in reason_codes:
        if r not in seen:
            seen.add(r)
            reason_codes_stable.append(r)

    out: Dict[str, Any] = {
        "schema_id": "C2_POSITION_LIFECYCLE_SNAPSHOT",
        "schema_version": 2,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {
            "repo": "constellation_2_runtime",
            "git_sha": _git_sha(),
            "module": "ops/tools/run_position_lifecycle_snapshot_v2.py",
        },
        "status": "OK",
        "reason_codes": reason_codes_stable,
        "items": out_items,
        "canonical_json_hash": None,
    }

    out["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out)
    validate_against_repo_schema_v1(out, REPO_ROOT, OUT_SCHEMA)

    try:
        payload = canonical_json_bytes_v1(out) + b"\n"
    except CanonicalizationError as e:
        print(f"FAIL: CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4

    out_path = (truth / "position_lifecycle_v2" / day / "position_lifecycle_snapshot.v2.json").resolve()
    try:
        _ = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: IMMUTABLE_WRITE_FAILED: {e}", file=sys.stderr)
        return 4

    print(f"OK: POSITION_LIFECYCLE_SNAPSHOT_V2_WRITTEN day_utc={day} src_v={ver} src_path={p_pos} out={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
