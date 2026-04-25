#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")

import argparse
import hashlib
import json
import os
import subprocess
from typing import Any, Dict, List

from constellation_2.phaseD.lib.canon_json_v1 import (
    CanonicalizationError,
    canonical_hash_for_c2_artifact_v1,
    canonical_json_bytes_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1
from constellation_2.common.truth_root_v1 import resolve_truth_root

REPO_ROOT = _REPO_ROOT_FROM_FILE.resolve()
DEFAULT_TRUTH = resolve_truth_root(repo_root=REPO_ROOT)

OUT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/POSITION_LIFECYCLE/position_lifecycle_snapshot.v2.schema.json"

POS_SNAPSHOT_SCHEMA_V5 = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v5.schema.json"
VALID_LIFECYCLE_STATES = {
    "OPEN",
    "MANAGING",
    "REDUCE_PENDING",
    "REDUCING",
    "CLOSE_PENDING",
    "CLOSING",
    "CLOSED",
    "FORCED_CLOSE_PENDING",
    "FORCED_CLOSED",
    "ORPHANED",
}


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json_obj(path: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        raise SystemExit(f"FAIL: JSON_READ_OR_PARSE_FAILED: path={path} err={e!r}") from e
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: TOP_LEVEL_NOT_OBJECT: path={path}")
    return obj


def _resolve_truth_root(args_truth_root: str) -> Path:
    tr = (args_truth_root or "").strip()
    if not tr:
        tr = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if not tr:
        tr = str(DEFAULT_TRUTH)

    truth_root = Path(tr).resolve()
    if not truth_root.exists() or not truth_root.is_dir():
        raise SystemExit(f"FATAL: truth_root missing or not directory: {truth_root}")
    return truth_root


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _positions_snapshot_v5_path(truth: Path, day: str) -> Path:
    p = (truth / "positions_v1" / "snapshots" / day / "positions_snapshot.v5.json").resolve()
    if not p.exists() or not p.is_file():
        raise SystemExit(f"FAIL: MISSING_POSITIONS_SNAPSHOT_V5: path={p}")
    return p


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _safe_sha256_or_zeros(v: Any) -> str:
    s = _safe_str(v)
    if len(s) == 64 and all(c in "0123456789abcdef" for c in s.lower()):
        return s.lower()
    return "0" * 64


def _return_if_existing_report(out_path: Path, expected_day_utc: str) -> int | None:
    """
    Idempotency: if the immutable output already exists for this day, return without rewriting.
    This avoids ATTEMPTED_REWRITE when the pipeline reruns.
    """
    if not out_path.exists():
        return None

    if not out_path.is_file():
        raise SystemExit(f"FAIL: EXISTING_OUTPUT_NOT_FILE: path={out_path}")

    existing = _read_json_obj(out_path)
    existing_sha = _sha256_file(out_path)

    schema_id = str(existing.get("schema_id") or "").strip()
    day_utc = str(existing.get("day_utc") or "").strip()
    status = str(existing.get("status") or "").strip().upper()

    if schema_id != "C2_POSITION_LIFECYCLE_SNAPSHOT":
        raise SystemExit(f"FAIL: EXISTING_REPORT_SCHEMA_MISMATCH: schema_id={schema_id!r} path={out_path}")
    if day_utc != expected_day_utc:
        raise SystemExit(
            f"FAIL: EXISTING_REPORT_DAY_MISMATCH: day_utc={day_utc!r} expected={expected_day_utc!r} path={out_path}"
        )
    if status not in {"OK", "FAIL"}:
        raise SystemExit(f"FAIL: EXISTING_REPORT_STATUS_INVALID: status={status!r} path={out_path}")

    print(
        f"OK: POSITION_LIFECYCLE_SNAPSHOT_V2_WRITTEN day_utc={expected_day_utc} "
        f"status={status} path={out_path} sha256={existing_sha} action=EXISTS"
    )
    return 0 if status == "OK" else 2


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
    p_pos = _positions_snapshot_v5_path(truth, day)
    pos = _read_json_obj(p_pos)
    validate_against_repo_schema_v1(pos, REPO_ROOT, POS_SNAPSHOT_SCHEMA_V5)

    reason_codes: List[str] = ["DERIVED_FROM_POSITIONS_SNAPSHOT_V5", "POSITIONS_SCHEMA_VALIDATED_V5"]
    items_in = pos.get("items")
    if not isinstance(items_in, list):
        raise SystemExit("FAIL: POSITIONS_V5_ITEMS_NOT_LIST")

    out_items: List[Dict[str, Any]] = []
    validation_errors: List[str] = []
    for it in items_in:
        if not isinstance(it, dict):
            continue
        position_id = _safe_str(it.get("position_id") or it.get("positionId") or "")
        engine_id = _safe_str(it.get("engine_id") or it.get("engineId") or "")
        opened_day_utc = _safe_str(it.get("opened_day_utc") or it.get("openedDayUtc") or day)
        source_intent_id = _safe_str(it.get("source_intent_id") or it.get("sourceIntentId") or "")
        intent_sha256 = _safe_sha256_or_zeros(it.get("intent_sha256") or it.get("intentSha256") or "")
        lifecycle_state = _safe_str(it.get("lifecycle_state") or "").upper()
        last_transition_utc = _safe_str(it.get("last_transition_utc") or produced_utc) or produced_utc
        lifecycle_reason_code = _safe_str(it.get("lifecycle_reason_code") or "")
        item_errors: List[str] = []
        imported_position_attribution = (
            engine_id == "imported_position"
            and source_intent_id.startswith("IMPORTED_POSITION:")
            and intent_sha256 == "0" * 64
        )

        if not position_id:
            item_errors.append("POSITION_ID_MISSING_IN_POSITIONS_SNAPSHOT")
        if position_id:
            if not engine_id:
                item_errors.append(f"LIFECYCLE_ENGINE_ID_MISSING:position_id={position_id}")
            if lifecycle_state not in VALID_LIFECYCLE_STATES:
                item_errors.append(f"LIFECYCLE_STATE_INVALID:position_id={position_id}:value={lifecycle_state or 'MISSING'}")
            if not lifecycle_reason_code:
                item_errors.append(f"LIFECYCLE_REASON_CODE_MISSING:position_id={position_id}")
            if not source_intent_id:
                item_errors.append(f"LIFECYCLE_SOURCE_INTENT_ID_MISSING:position_id={position_id}")
            if intent_sha256 == "0" * 64 and not imported_position_attribution:
                item_errors.append(f"LIFECYCLE_INTENT_SHA256_INVALID:position_id={position_id}")
        if item_errors:
            validation_errors.extend(item_errors)
            continue

        out_items.append(
            {
                "position_id": position_id,
                "engine_id": engine_id,
                "source_intent_id": source_intent_id,
                "intent_sha256": intent_sha256,
                "lifecycle_state": lifecycle_state,
                "lifecycle_reason_code": lifecycle_reason_code,
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
        "status": ("FAIL" if validation_errors else "OK"),
        "reason_codes": (reason_codes_stable + sorted(set(validation_errors))),
        "items": ([] if validation_errors else out_items),
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

    # Idempotency: if output exists, do not rewrite.
    ex = _return_if_existing_report(out_path, expected_day_utc=day)
    if ex is not None:
        return ex

    try:
        _ = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: IMMUTABLE_WRITE_FAILED: {e}", file=sys.stderr)
        return 4

    print(
        f"OK: POSITION_LIFECYCLE_SNAPSHOT_V2_WRITTEN day_utc={day} src_v=5 "
        f"src_path={p_pos} out={out_path}"
    )
    return 0 if out["status"] == "OK" else 2


if __name__ == "__main__":
    raise SystemExit(main())
