#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import (
    CanonicalizationError,
    canonical_hash_for_c2_artifact_v1,
    canonical_json_bytes_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
DEFAULT_TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_OUT = "governance/04_DATA/SCHEMAS/C2/EXPOSURE_RECONCILIATION/exposure_reconciliation.v2.schema.json"


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


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


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


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _iter_positions_items_any_version(truth: Path, day: str) -> Tuple[Optional[Path], List[Dict[str, Any]], List[str]]:
    """
    Read positions snapshot items from the best available version for day:
      v5, v4, v3, v2 (first found wins)
    Supported shapes:
      - top-level "items": [...]
      - "positions": {"items": [...]}
    Returns (path_used, items, reason_codes_additions)
    """
    reasons: List[str] = []
    snap_dir = (truth / "positions_v1" / "snapshots" / day).resolve()
    if not snap_dir.exists() or not snap_dir.is_dir():
        return (None, [], ["POSITIONS_SNAPSHOT_DIR_MISSING"])

    for v in (5, 4, 3, 2):
        p = (snap_dir / f"positions_snapshot.v{v}.json").resolve()
        if not p.exists() or not p.is_file():
            continue

        obj = _read_json_obj(p)

        items: Any = obj.get("items")
        if isinstance(items, list):
            out = [x for x in items if isinstance(x, dict)]
            reasons.append(f"POSITIONS_ITEMS_FROM_V{v}_TOPLEVEL")
            return (p, out, reasons)

        pos_block = obj.get("positions")
        if isinstance(pos_block, dict) and isinstance(pos_block.get("items"), list):
            out2 = [x for x in pos_block.get("items") if isinstance(x, dict)]
            reasons.append(f"POSITIONS_ITEMS_FROM_V{v}_POSITIONS_BLOCK")
            return (p, out2, reasons)

        # Found snapshot but unsupported shape
        reasons.append(f"POSITIONS_ITEMS_SHAPE_UNSUPPORTED_V{v}")
        return (p, [], reasons)

    return (None, [], ["POSITIONS_SNAPSHOT_MISSING_ALL_VERSIONS"])


def _read_exit_obligations_engine_ids(truth: Path, day: str) -> Tuple[Optional[Path], Set[str], List[str]]:
    """
    Exit obligations v1 writer output path:
      truth/exit_obligations_v1/<DAY>/exit_obligations.v1.json
    We only need engine_id set + obligation count per engine.
    """
    reasons: List[str] = []
    p = (truth / "exit_obligations_v1" / day / "exit_obligations.v1.json").resolve()
    if not p.exists() or not p.is_file():
        return (None, set(), ["EXIT_OBLIGATIONS_MISSING"])

    obj = _read_json_obj(p)

    # Common shapes we support:
    # - obj["obligations"] : list[dict] with "engine_id"
    # - obj["items"] : list[dict] with "engine_id"
    candidates: List[Dict[str, Any]] = []
    if isinstance(obj.get("obligations"), list):
        candidates = [x for x in obj["obligations"] if isinstance(x, dict)]
        reasons.append("EXIT_OBLIGATIONS_FROM_FIELD:obligations")
    elif isinstance(obj.get("items"), list):
        candidates = [x for x in obj["items"] if isinstance(x, dict)]
        reasons.append("EXIT_OBLIGATIONS_FROM_FIELD:items")
    else:
        reasons.append("EXIT_OBLIGATIONS_SHAPE_UNKNOWN_NO_LIST")
        return (p, set(), reasons)

    engs: Set[str] = set()
    for it in candidates:
        eid = _safe_str(it.get("engine_id") or it.get("engineId") or it.get("engine") or "")
        if eid:
            engs.add(eid)
    return (p, engs, reasons)


def _read_intents_engine_ids(truth: Path, day: str) -> Tuple[Optional[Path], Set[str], List[str]]:
    """
    Intents day dir:
      truth/intents_v1/snapshots/<DAY>/
    We scan JSON files and collect engine.engine_id if present.
    """
    reasons: List[str] = []
    d = (truth / "intents_v1" / "snapshots" / day).resolve()
    if not d.exists() or not d.is_dir():
        return (None, set(), ["INTENTS_DAY_DIR_MISSING"])

    engs: Set[str] = set()
    any_json = False
    for p in d.rglob("*.json"):
        if not p.is_file():
            continue
        any_json = True
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        eng = obj.get("engine")
        if isinstance(eng, dict):
            eid = _safe_str(eng.get("engine_id") or eng.get("engineId") or "")
            if eid:
                engs.add(eid)

    if not any_json:
        reasons.append("INTENTS_DAY_DIR_EMPTY")
    else:
        reasons.append("INTENTS_SCANNED")

    return (d, engs, reasons)


def _return_if_existing_report(out_path: Path, expected_day_utc: str) -> int | None:
    """
    Idempotency: if immutable output already exists for this day, do not rewrite.
    Return 0 for OK, 2 for FAIL (fail-closed), else raise for invalid existing shape.
    """
    if not out_path.exists():
        return None
    if not out_path.is_file():
        raise SystemExit(f"FAIL: EXISTING_OUTPUT_NOT_FILE: path={out_path}")

    existing = _read_json_obj(out_path)
    existing_sha = _sha256_file(out_path)

    schema_id = _safe_str(existing.get("schema_id"))
    schema_version = existing.get("schema_version")
    day_utc = _safe_str(existing.get("day_utc"))
    status = _safe_str(existing.get("status")).upper()

    if schema_id != "C2_EXPOSURE_RECONCILIATION":
        raise SystemExit(f"FAIL: EXISTING_REPORT_SCHEMA_MISMATCH: schema_id={schema_id!r} path={out_path}")
    if schema_version != 2:
        raise SystemExit(f"FAIL: EXISTING_REPORT_VERSION_MISMATCH: schema_version={schema_version!r} path={out_path}")
    if day_utc != expected_day_utc:
        raise SystemExit(
            f"FAIL: EXISTING_REPORT_DAY_MISMATCH: day_utc={day_utc!r} expected={expected_day_utc!r} path={out_path}"
        )
    if status not in ("OK", "FAIL"):
        raise SystemExit(f"FAIL: EXISTING_REPORT_STATUS_INVALID: status={status!r} path={out_path}")

    print(
        f"OK: EXPOSURE_RECONCILIATION_V2_WRITTEN day_utc={expected_day_utc} status={status} "
        f"path={out_path} sha256={existing_sha} action=EXISTS"
    )
    return 0 if status == "OK" else 2


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_exposure_reconciliation_v2")
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

    out_path = (truth / "exposure_reconciliation_v2" / day / "exposure_reconciliation.v2.json").resolve()

    # Idempotency
    ex = _return_if_existing_report(out_path, expected_day_utc=day)
    if ex is not None:
        return ex

    reason_codes: List[str] = []

    # Inputs (best-effort)
    p_pos, pos_items, rc_pos = _iter_positions_items_any_version(truth, day)
    reason_codes.extend(rc_pos)

    p_obl, obl_engs, rc_obl = _read_exit_obligations_engine_ids(truth, day)
    reason_codes.extend(rc_obl)

    p_int, intent_engs, rc_int = _read_intents_engine_ids(truth, day)
    reason_codes.extend(rc_int)

    # Actual positions per engine from positions snapshot items
    actual_by_engine: Dict[str, int] = {}
    orphaned_by_engine: Dict[str, int] = {}

    for it in pos_items:
        eid = _safe_str(it.get("engine_id") or it.get("engineId") or it.get("engine") or "")
        if not eid:
            # orphan: a position without engine_id cannot be reconciled
            orphaned_by_engine["__UNKNOWN__"] = orphaned_by_engine.get("__UNKNOWN__", 0) + 1
            continue
        actual_by_engine[eid] = actual_by_engine.get(eid, 0) + 1

    # Exit obligations count per engine (best-effort)
    exit_count_by_engine: Dict[str, int] = {}
    if p_obl is not None:
        obl_obj = _read_json_obj(p_obl)
        obl_list: List[Dict[str, Any]] = []
        if isinstance(obl_obj.get("obligations"), list):
            obl_list = [x for x in obl_obj["obligations"] if isinstance(x, dict)]
        elif isinstance(obl_obj.get("items"), list):
            obl_list = [x for x in obl_obj["items"] if isinstance(x, dict)]
        for o in obl_list:
            eid = _safe_str(o.get("engine_id") or o.get("engineId") or o.get("engine") or "")
            if not eid:
                continue
            exit_count_by_engine[eid] = exit_count_by_engine.get(eid, 0) + 1

    # Desired positions per engine:
    # For now: count intents presence as a proxy for “desired”. This is a minimal deterministic reconciliation
    # that is still fail-closed if there is any mismatch.
    desired_by_engine: Dict[str, int] = {}
    if p_int is not None and p_int.is_dir():
        for p in p_int.rglob("*.json"):
            if not p.is_file():
                continue
            try:
                obj = json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue
            eng = obj.get("engine")
            if not isinstance(eng, dict):
                continue
            eid = _safe_str(eng.get("engine_id") or eng.get("engineId") or "")
            if not eid:
                continue
            desired_by_engine[eid] = desired_by_engine.get(eid, 0) + 1

    # Engine universe
    engine_ids: Set[str] = set()
    engine_ids.update(actual_by_engine.keys())
    engine_ids.update(desired_by_engine.keys())
    engine_ids.update(exit_count_by_engine.keys())
    engine_ids.update(obl_engs)
    engine_ids.update(intent_engs)

    status = "OK"
    engines_out: List[Dict[str, Any]] = []

    for eid in sorted(engine_ids):
        desired = int(desired_by_engine.get(eid, 0))
        actual = int(actual_by_engine.get(eid, 0))
        delta = int(desired - actual)
        orphaned = int(orphaned_by_engine.get(eid, 0))
        exit_cnt = int(exit_count_by_engine.get(eid, 0))

        breach = False
        if delta != 0:
            breach = True
        if orphaned > 0:
            breach = True

        engines_out.append(
            {
                "engine_id": eid,
                "desired_positions": desired,
                "actual_positions": actual,
                "delta_positions": delta,
                "orphaned_positions": orphaned,
                "exit_obligation_count": exit_cnt,
                "breach_flag": bool(breach),
            }
        )

        if breach:
            status = "FAIL"

    if not engines_out:
        reason_codes.append("NO_ENGINES_DERIVED_EMPTY_RECONCILIATION")
        # With nothing to reconcile, status stays OK.

    # Stable de-dupe reason_codes
    seen = set()
    reason_codes_stable: List[str] = []
    for r in reason_codes:
        if r not in seen:
            seen.add(r)
            reason_codes_stable.append(r)

    out: Dict[str, Any] = {
        "schema_id": "C2_EXPOSURE_RECONCILIATION",
        "schema_version": 2,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_exposure_reconciliation_v2.py"},
        "status": status,
        "reason_codes": reason_codes_stable,
        "engines": engines_out,
        "canonical_json_hash": None,
    }

    out["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out)

    validate_against_repo_schema_v1(out, REPO_ROOT, SCHEMA_OUT)

    try:
        payload = canonical_json_bytes_v1(out) + b"\n"
    except CanonicalizationError as e:
        print(f"FAIL: CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4

    try:
        _ = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: IMMUTABLE_WRITE_FAILED: {e}", file=sys.stderr)
        return 4

    out_sha = _sha256_file(out_path)
    print(
        f"OK: EXPOSURE_RECONCILIATION_V2_WRITTEN day_utc={day} status={status} "
        f"path={out_path} sha256={out_sha} action=WROTE"
    )
    return 0 if status == "OK" else 2


if __name__ == "__main__":
    raise SystemExit(main())
