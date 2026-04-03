from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from constellation_2.phaseF.accounting.lib.immut_write_v1 import (
    ImmutableWriteError,
    WriteResultV1,
    write_file_immutable_v1,
)


@dataclass(frozen=True)
class RefreshWriteResultV1:
    path: str
    sha256: str
    action: str  # WROTE | SKIP_IDENTICAL | EXISTS | REFRESHED
    quarantined_path: str
    prior_sha256: str


def _sha256_bytes(b: bytes) -> str:
    import hashlib

    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _read_json_obj(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _fsync_dir(path: Path) -> None:
    dir_fd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _version_matches(existing: Any, expected: Any) -> bool:
    return str(existing).strip() == str(expected).strip()


def _quarantine_existing_file(path: Path, existing_bytes: bytes, existing_sha: str) -> Path:
    quarantine_dir = (path.parent / "__quarantine__").resolve()
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    quarantine = (quarantine_dir / f"{path.name}.INVALID_{existing_sha}.json").resolve()
    if quarantine.exists():
        if not quarantine.is_file():
            raise SystemExit(f"FAIL: QUARANTINE_PATH_NOT_FILE: {quarantine}")
        q_sha = _sha256_file(quarantine)
        if q_sha != existing_sha:
            raise SystemExit(
                f"FAIL: QUARANTINE_SHA_MISMATCH path={quarantine} expected_sha={existing_sha} actual_sha={q_sha}"
            )
        return quarantine

    tmp_q = quarantine.with_name(f".{quarantine.name}.tmp.{os.getpid()}")
    tmp_q.write_bytes(existing_bytes)
    fd = os.open(str(tmp_q), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(str(tmp_q), str(quarantine))
    _fsync_dir(quarantine)
    return quarantine


def write_day_artifact_refreshable_v1(
    *,
    path: Path,
    data: bytes,
    expected_day_utc: str,
    expected_schema_id: str,
    expected_schema_version: Any,
    preserve_statuses: Tuple[str, ...] = ("PASS",),
) -> RefreshWriteResultV1:
    try:
        wr: WriteResultV1 = write_file_immutable_v1(path=path, data=data, create_dirs=True)
        return RefreshWriteResultV1(
            path=wr.path,
            sha256=wr.sha256,
            action=str(wr.action),
            quarantined_path="",
            prior_sha256="",
        )
    except ImmutableWriteError as exc:
        msg = str(exc)
        if not msg.startswith("ATTEMPTED_REWRITE:"):
            raise

    existing_bytes = path.read_bytes()
    existing_sha = _sha256_bytes(existing_bytes)
    candidate_sha = _sha256_bytes(data)
    if existing_sha == candidate_sha:
        return RefreshWriteResultV1(
            path=str(path),
            sha256=candidate_sha,
            action="SKIP_IDENTICAL",
            quarantined_path="",
            prior_sha256=existing_sha,
        )

    existing = _read_json_obj(path)
    schema_id = str(existing.get("schema_id") or "").strip()
    schema_version = existing.get("schema_version")
    day_utc = str(existing.get("day_utc") or "").strip()
    if schema_id != expected_schema_id:
        raise SystemExit(
            f"FAIL: EXISTING_SCHEMA_MISMATCH path={path} schema_id={schema_id!r} expected={expected_schema_id!r}"
        )
    if not _version_matches(schema_version, expected_schema_version):
        raise SystemExit(
            f"FAIL: EXISTING_SCHEMA_VERSION_MISMATCH path={path} schema_version={schema_version!r} expected={expected_schema_version!r}"
        )
    if day_utc != expected_day_utc:
        raise SystemExit(
            f"FAIL: EXISTING_DAY_MISMATCH path={path} day_utc={day_utc!r} expected={expected_day_utc!r}"
        )

    existing_status = str(existing.get("status") or "").strip().upper()
    if existing_status and existing_status in {s.strip().upper() for s in preserve_statuses}:
        return RefreshWriteResultV1(
            path=str(path),
            sha256=existing_sha,
            action="EXISTS",
            quarantined_path="",
            prior_sha256=existing_sha,
        )

    quarantine = _quarantine_existing_file(path, existing_bytes, existing_sha)
    path.unlink()
    wr2: WriteResultV1 = write_file_immutable_v1(path=path, data=data, create_dirs=True)
    return RefreshWriteResultV1(
        path=wr2.path,
        sha256=wr2.sha256,
        action="REFRESHED",
        quarantined_path=str(quarantine),
        prior_sha256=existing_sha,
    )
