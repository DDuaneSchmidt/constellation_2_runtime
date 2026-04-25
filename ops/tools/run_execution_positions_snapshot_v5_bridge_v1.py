#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

HERE = Path(__file__).resolve()
REPO_ROOT = HERE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.artifact_authority_v1 import (
    get_artifact_contract_v1,
    get_artifact_mirror_contract_v1,
    resolve_artifact_authority_path_v1,
)
from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root, resolve_truth_sleeves_root
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

POSITIONS_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v5.schema.json"


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _parse_day(day_utc: str) -> str:
    day = str(day_utc or "").strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT:{day!r}")
    return day


def _require_existing_dir(path: Path, *, label: str) -> Path:
    if not path.is_absolute():
        raise SystemExit(f"FAIL: {label}_NOT_ABSOLUTE:{path}")
    if not path.exists() or not path.is_dir():
        raise SystemExit(f"FAIL: {label}_MISSING:{path}")
    return path


def _require_under_root(path: Path, *, root: Path, label: str) -> Path:
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise SystemExit(f"FAIL: {label}_OUTSIDE_ALLOWED_ROOT:{path}") from exc
    return path


def _read_json_obj(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: TOP_LEVEL_NOT_OBJECT:{path}")
    return obj


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    tmp.write_bytes(data)
    fd = os.open(str(tmp), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(str(tmp), str(path))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_execution_positions_snapshot_v5_bridge_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--source_truth_root", required=False, default=None)
    ap.add_argument("--truth_root", required=True)
    args = ap.parse_args(argv)

    day = _parse_day(args.day_utc)
    canonical_root_allowed = _require_existing_dir(resolve_canonical_truth_root().resolve(), label="CANONICAL_TRUTH_ROOT")
    canonical_truth_root = _require_existing_dir(
        Path(str(args.source_truth_root or resolve_canonical_truth_root())).resolve(),
        label="SOURCE_TRUTH_ROOT",
    )
    truth_sleeves_root = _require_existing_dir(resolve_truth_sleeves_root().resolve(), label="TRUTH_SLEEVES_ROOT")
    execution_truth_root = _require_existing_dir(Path(str(args.truth_root)).resolve(), label="TRUTH_ROOT")

    _require_under_root(canonical_truth_root, root=canonical_root_allowed, label="SOURCE_TRUTH_ROOT")
    _require_under_root(execution_truth_root, root=truth_sleeves_root, label="TRUTH_ROOT")

    contract = get_artifact_contract_v1(REPO_ROOT, "positions_snapshot_v5")
    if str(contract.get("root_policy") or "").strip() != "mirrored":
        raise SystemExit("FAIL: POSITIONS_SNAPSHOT_V5_NOT_DECLARED_MIRRORED")
    mirror_contract = get_artifact_mirror_contract_v1(
        REPO_ROOT,
        "positions_snapshot_v5",
        "execution_positions_snapshot_v5",
    )
    if str(mirror_contract.get("authoritative_bridge") or "").strip() != "ops/tools/run_execution_positions_snapshot_v5_bridge_v1.py":
        raise SystemExit("FAIL: POSITIONS_SNAPSHOT_V5_BRIDGE_NOT_AUTHORITATIVE")

    source_path = resolve_artifact_authority_path_v1(
        repo_root=REPO_ROOT,
        artifact_id="positions_snapshot_v5",
        day_utc=day,
        canonical_truth_root=canonical_truth_root,
        execution_truth_root=execution_truth_root,
        path_role="authoritative",
    )
    if not source_path.exists() or not source_path.is_file():
        raise SystemExit(f"FAIL: SOURCE_POSITIONS_SNAPSHOT_V5_MISSING:{source_path}")

    payload = _read_json_obj(source_path)
    validate_against_repo_schema_v1(payload, REPO_ROOT, POSITIONS_SCHEMA_RELPATH)
    status = str(payload.get("status") or "").strip().upper()
    if status != "OK":
        raise SystemExit(f"FAIL: SOURCE_POSITIONS_SNAPSHOT_V5_STATUS_NOT_OK:{status or 'MISSING'}")
    if str(payload.get("day_utc") or "").strip() != day:
        raise SystemExit("FAIL: SOURCE_POSITIONS_SNAPSHOT_V5_DAY_MISMATCH")

    data = source_path.read_bytes()
    target_path = resolve_artifact_authority_path_v1(
        repo_root=REPO_ROOT,
        artifact_id="positions_snapshot_v5",
        day_utc=day,
        canonical_truth_root=canonical_truth_root,
        execution_truth_root=execution_truth_root,
        path_role="mirror",
        mirror_id="execution_positions_snapshot_v5",
    )
    _require_under_root(target_path, root=execution_truth_root, label="TARGET_PATH")

    action = "WRITE"
    if target_path.exists():
        existing = target_path.read_bytes()
        if existing == data:
            action = "SKIP_IDENTICAL"
        else:
            raise SystemExit(f"FAIL: TARGET_POSITIONS_SNAPSHOT_V5_CONFLICT:{target_path}")
    else:
        _atomic_write(target_path, data)

    print(
        "OK: EXECUTION_POSITIONS_SNAPSHOT_V5_BRIDGE_V1 "
        f"action={action} "
        f"day_utc={day} "
        f"source_path={source_path} "
        f"target_path={target_path} "
        f"sha256={_sha256_bytes(data)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
