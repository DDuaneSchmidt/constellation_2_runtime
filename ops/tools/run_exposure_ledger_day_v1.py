#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.common.truth_root_v1 import resolve_truth_root


SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/EXPOSURE/exposure_ledger.v1.schema.json"
FILL_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/fill_ledger.v1.schema.json"
POSITIONS_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v4.schema.json"
AUTH_LEDGER_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/intent_authorization_ledger.v1.schema.json"


def _parse_day(day: str) -> str:
    value = str(day or "").strip()
    if len(value) != 10 or value[4] != "-" or value[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {value!r}")
    return value


def _require_truth_root(raw: str) -> Path:
    if str(raw or "").strip():
        path = Path(raw).expanduser().resolve()
    else:
        path = resolve_truth_root(repo_root=REPO_ROOT).resolve()
    if not path.is_absolute() or not path.exists() or not path.is_dir():
        raise SystemExit(f"FAIL: invalid truth_root: {path}")
    return path


def _read_json_obj(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git_sha() -> str:
    try:
        output = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        value = output.decode("utf-8").strip()
    except Exception:
        value = "0" * 40
    if len(value) != 40:
        value = "0" * 40
    return value


def _write_immutable(path: Path, payload: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    candidate_sha = _sha256_bytes(payload)
    if path.exists():
        if not path.is_file():
            raise SystemExit(f"FAIL: TARGET_NOT_FILE: {path}")
        existing_sha = _sha256_file(path)
        if existing_sha == candidate_sha:
            return candidate_sha
        raise SystemExit(
            f"FAIL: ATTEMPTED_REWRITE_IMMUTABLE path={path} existing_sha={existing_sha} candidate_sha={candidate_sha}"
        )
    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_bytes(payload)
    tmp.replace(path)
    return candidate_sha


def _write_replace_if_changed(path: Path, payload: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    candidate_sha = _sha256_bytes(payload)
    if path.exists():
        existing_sha = _sha256_file(path)
        if existing_sha == candidate_sha:
            return candidate_sha
    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_bytes(payload)
    tmp.replace(path)
    return candidate_sha


def _load_fill_ledgers(day_utc: str, truth_root: Path) -> Dict[str, Dict[str, Any]]:
    fill_dir = (truth_root / "fill_ledger_v1" / day_utc).resolve()
    if not fill_dir.exists() or not fill_dir.is_dir():
        raise SystemExit(f"FAIL: FILL_LEDGER_DIR_MISSING: {fill_dir}")
    result: Dict[str, Dict[str, Any]] = {}
    for path in sorted(fill_dir.glob("*.fill_ledger.v1.json")):
        obj = _read_json_obj(path)
        validate_against_repo_schema_v1(obj, REPO_ROOT, FILL_SCHEMA_RELPATH)
        binding_hash = str(obj.get("binding_hash") or "").strip()
        if not binding_hash:
            raise SystemExit(f"FAIL: FILL_LEDGER_BINDING_HASH_MISSING: {path}")
        result[binding_hash] = {"path": path, "obj": obj}
    if not result:
        raise SystemExit(f"FAIL: FILL_LEDGER_FILES_MISSING: {fill_dir}")
    return result


def _load_auth_ledgers(day_utc: str, truth_root: Path) -> Dict[str, Dict[str, Any]]:
    auth_dir = (truth_root / "engine_activity_v1" / "intent_authorization_ledger_v1" / day_utc).resolve()
    if not auth_dir.exists() or not auth_dir.is_dir():
        raise SystemExit(f"FAIL: AUTH_LEDGER_DIR_MISSING: {auth_dir}")
    result: Dict[str, Dict[str, Any]] = {}
    for path in sorted(auth_dir.glob("*.intent_authorization_ledger.v1.jsonl")):
        events: List[Dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            obj = json.loads(line)
            if not isinstance(obj, dict):
                raise SystemExit(f"FAIL: AUTH_LEDGER_LINE_NOT_OBJECT: {path}")
            validate_against_repo_schema_v1(obj, REPO_ROOT, AUTH_LEDGER_SCHEMA_RELPATH)
            events.append(obj)
        if not events:
            raise SystemExit(f"FAIL: AUTH_LEDGER_EMPTY: {path}")
        latest = events[-1]
        intent_hash = str(latest.get("intent_hash") or "").strip()
        if not intent_hash:
            raise SystemExit(f"FAIL: AUTH_LEDGER_INTENT_HASH_MISSING: {path}")
        result[intent_hash] = {"path": path, "latest": latest}
    if not result:
        raise SystemExit(f"FAIL: AUTH_LEDGER_FILES_MISSING: {auth_dir}")
    return result


def _write_build_status(
    *,
    truth_root: Path,
    day_utc: str,
    status: str,
    reason_codes: List[str],
    exposure_count: int,
    positions_snapshot_path: Path | None,
) -> str:
    out_obj = {
        "schema_id": "C2_EXPOSURE_LEDGER_BUILD_V1_UNGOVERNED",
        "schema_version": 1,
        "day_utc": day_utc,
        "status": status,
        "reason_codes": sorted(set(code for code in reason_codes if code)),
        "exposure_count": exposure_count,
        "positions_snapshot_path": "" if positions_snapshot_path is None else str(positions_snapshot_path),
    }
    out_path = (
        truth_root / "reports" / "exposure_ledger_build_v1" / day_utc / "exposure_ledger_build.v1.json"
    ).resolve()
    payload = canonical_json_bytes_v1(out_obj) + b"\n"
    out_sha = _write_replace_if_changed(out_path, payload)
    print(
        "OK: EXPOSURE_LEDGER_BUILD_STATUS_WRITTEN "
        f"day_utc={day_utc} path={out_path} sha256={out_sha} git_sha={_git_sha()}"
    )
    return out_sha


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_exposure_ledger_day_v1")
    ap.add_argument("--day", required=True, help="UTC day in YYYY-MM-DD")
    ap.add_argument("--truth_root", default="", help="Optional explicit truth root")
    args = ap.parse_args()

    day_utc = _parse_day(args.day)
    truth_root = _require_truth_root(args.truth_root)

    positions_path = (truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v4.json").resolve()
    if not positions_path.exists() or not positions_path.is_file():
        _write_build_status(
            truth_root=truth_root,
            day_utc=day_utc,
            status="NO_EXPOSURE",
            reason_codes=["NO_EXPOSURE", "POSITIONS_SNAPSHOT_MISSING"],
            exposure_count=0,
            positions_snapshot_path=positions_path,
        )
        print(f"OK: NO_EXPOSURE day_utc={day_utc} reason=POSITIONS_SNAPSHOT_MISSING")
        return 0
    positions_snapshot = _read_json_obj(positions_path)
    validate_against_repo_schema_v1(positions_snapshot, REPO_ROOT, POSITIONS_SCHEMA_RELPATH)
    positions_block = positions_snapshot.get("positions")
    if not isinstance(positions_block, dict):
        raise SystemExit("FAIL: POSITIONS_BLOCK_MISSING")
    items = positions_block.get("items")
    if not isinstance(items, list):
        raise SystemExit("FAIL: POSITIONS_ITEMS_INVALID")
    if not items:
        _write_build_status(
            truth_root=truth_root,
            day_utc=day_utc,
            status="NO_EXPOSURE",
            reason_codes=["NO_EXPOSURE", "POSITIONS_EMPTY"],
            exposure_count=0,
            positions_snapshot_path=positions_path,
        )
        print(f"OK: NO_EXPOSURE day_utc={day_utc} reason=POSITIONS_EMPTY")
        return 0

    fill_ledgers = _load_fill_ledgers(day_utc, truth_root)
    auth_ledgers = _load_auth_ledgers(day_utc, truth_root)
    position_snapshot_sha = _sha256_file(positions_path)
    written = 0

    for item in items:
        if not isinstance(item, dict):
            raise SystemExit("FAIL: POSITION_ITEM_NOT_OBJECT")
        binding_hash = str(item.get("position_id") or "").strip()
        if not binding_hash:
            raise SystemExit("FAIL: POSITION_ID_MISSING")
        fill_entry = fill_ledgers.get(binding_hash)
        if fill_entry is None:
            raise SystemExit(f"FAIL: POSITION_FILL_LINK_MISSING: {binding_hash}")
        fill_obj = fill_entry["obj"]
        intent_hash = str(fill_obj.get("intent_sha256") or "").strip()
        auth_entry = auth_ledgers.get(intent_hash)
        if auth_entry is None:
            raise SystemExit(f"FAIL: FILL_AUTH_LINK_MISSING: {intent_hash}")
        auth_latest = auth_entry["latest"]
        if str(fill_obj.get("source_intent_id") or "").strip() != str(auth_latest.get("intent_id") or "").strip():
            raise SystemExit(f"FAIL: SOURCE_INTENT_ID_MISMATCH: {binding_hash}")
        if str(fill_obj.get("engine_id") or "").strip() != str(auth_latest.get("engine_id") or "").strip():
            raise SystemExit(f"FAIL: ENGINE_ID_MISMATCH: {binding_hash}")

        reason_codes: List[str] = []
        for raw_code in positions_snapshot.get("reason_codes") or []:
            code = str(raw_code or "").strip()
            if code:
                reason_codes.append(code)
        for raw_code in fill_obj.get("reason_codes") or []:
            code = str(raw_code or "").strip()
            if code:
                reason_codes.append(code)
        reason_codes = sorted(set(reason_codes))

        record = {
            "schema_id": "C2_EXPOSURE_LEDGER_V1",
            "schema_version": 1,
            "day_utc": day_utc,
            "position_id": binding_hash,
            "binding_hash": binding_hash,
            "submission_id": str(fill_obj.get("submission_id") or ""),
            "engine_id": str(fill_obj.get("engine_id") or ""),
            "canonical_sleeve_id": str(auth_latest.get("canonical_sleeve_id") or ""),
            "source_intent_id": str(fill_obj.get("source_intent_id") or ""),
            "intent_hash": intent_hash,
            "authorization_ledger_ref": {
                "path": str(auth_entry["path"]),
                "sha256": _sha256_file(auth_entry["path"]),
            },
            "fill_ledger_ref": {
                "path": str(fill_entry["path"]),
                "sha256": _sha256_file(fill_entry["path"]),
            },
            "position_snapshot_ref": {
                "path": str(positions_path),
                "sha256": position_snapshot_sha,
            },
            "qty": int(item.get("qty") or 0),
            "avg_cost_cents": int(item.get("avg_cost_cents") or 0),
            "status": str(item.get("status") or ""),
            "reason_codes": reason_codes,
        }
        validate_against_repo_schema_v1(record, REPO_ROOT, SCHEMA_RELPATH)
        out_path = (
            truth_root / "exposure_v1" / "exposure_ledger_v1" / day_utc / f"{binding_hash}.exposure_ledger.v1.json"
        ).resolve()
        payload = canonical_json_bytes_v1(record) + b"\n"
        out_sha = _write_immutable(out_path, payload)
        written += 1
        print(
            "OK: EXPOSURE_LEDGER_WRITTEN "
            f"day_utc={day_utc} path={out_path} sha256={out_sha} git_sha={_git_sha()}"
        )

    _write_build_status(
        truth_root=truth_root,
        day_utc=day_utc,
        status="OK",
        reason_codes=[],
        exposure_count=written,
        positions_snapshot_path=positions_path,
    )
    print(f"OK: EXPOSURE_LEDGER_COUNT day_utc={day_utc} count={written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
