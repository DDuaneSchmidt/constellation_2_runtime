from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


CODE_ROOT = Path(__file__).resolve().parents[2]
REPO_ROOT = Path("/home/node/constellation").resolve()
RUNTIME_DATA_ROOT = Path("/home/node/constellation_runtime_data").resolve()
CURRENT_RELEASE_MANIFEST = RUNTIME_DATA_ROOT / "truth/releases/current_release.v1.json"
ACTIVE_POINTER = Path("/home/node/constellation_active")
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_release_integrity_status.v1.schema.json"


def now_utc_iso_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def aegis_release_integrity_status_path_v1(*, truth_root: Path) -> Path:
    return Path(truth_root).resolve() / "reports/aegis_release_integrity_status_v1/current/aegis_release_integrity_status.v1.json"


def build_aegis_release_integrity_status_v1(
    *,
    generated_at_utc: str,
    repo_root: Path = REPO_ROOT,
    runtime_data_root: Path = RUNTIME_DATA_ROOT,
    current_release_manifest: Path = CURRENT_RELEASE_MANIFEST,
    active_pointer: Path = ACTIVE_POINTER,
    current_runtime_mode: str = "PAPER",
) -> dict[str, Any]:
    repo_root = Path(repo_root).resolve()
    manifest = _read_json(current_release_manifest)
    repo_head = _git_stdout(repo_root, ["rev-parse", "HEAD"])
    active_commit = str(manifest.get("commit") or "")
    release_match = "MATCH" if repo_head and active_commit and repo_head == active_commit else "MISMATCH"
    dirty = _repo_dirty_status(repo_root)
    active_release_path = str(manifest.get("release_path") or (active_pointer.resolve() if active_pointer.exists() else ""))
    reason_codes: list[str] = []
    if release_match != "MATCH":
        reason_codes.append("ACTIVE_RELEASE_REPO_MISMATCH")
    if dirty != "CLEAN":
        reason_codes.append(f"REPO_DIRTY_STATUS_{dirty}")
    payload = {
        "schema_id": "aegis_release_integrity_status",
        "schema_version": "v1",
        "artifact_id": "aegis_release_integrity_status_v1",
        "generated_at_utc": generated_at_utc,
        "repo_head_commit": repo_head,
        "active_release_commit": active_commit,
        "release_match_status": release_match,
        "active_release_id": str(manifest.get("release_id") or ""),
        "repo_dirty_status": dirty,
        "runtime_path": str(Path(runtime_data_root).resolve()),
        "active_release_path": active_release_path,
        "operator_warning_required": bool(reason_codes),
        "current_runtime_mode": current_runtime_mode,
        "reason_codes": reason_codes,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def validate_aegis_release_integrity_status_v1(payload: dict[str, Any]) -> None:
    validate_against_repo_schema_v1(payload, CODE_ROOT, SCHEMA_RELPATH)


def write_aegis_release_integrity_status_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_aegis_release_integrity_status_v1(payload)
    path = aegis_release_integrity_status_path_v1(truth_root=truth_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _git_stdout(repo_root: Path, args: list[str]) -> str:
    try:
        proc = subprocess.run(["git", "-C", str(repo_root), *args], check=False, capture_output=True, text=True)
    except OSError:
        return ""
    return proc.stdout.strip() if proc.returncode == 0 else ""


def _repo_dirty_status(repo_root: Path) -> str:
    out = _git_stdout(repo_root, ["status", "--porcelain"])
    if out:
        return "DIRTY"
    if _git_stdout(repo_root, ["rev-parse", "--is-inside-work-tree"]) != "true":
        return "UNKNOWN"
    return "CLEAN"
