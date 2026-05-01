from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from constellation_2.common.runtime_contract_v1 import resolve_runtime_data_root


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_VERSION = "producer_contract.v1"


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_bytes(payload: Any) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _sha256_file(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_stdout(args: list[str]) -> str:
    proc = subprocess.run(
        ["git", "-C", str(REPO_ROOT), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    return str(proc.stdout or "").strip() if proc.returncode == 0 else ""


def git_commit_v1() -> str:
    return _git_stdout(["rev-parse", "HEAD"])


def git_dirty_status_v1() -> str:
    return "DIRTY" if _git_stdout(["status", "--short"]) else "CLEAN"


def runtime_contract_path_v1() -> str:
    return str((resolve_runtime_data_root() / "runtime_contract_v1" / "active_runtime_contract.v1.json").resolve())


def artifact_ref_v1(path: str | Path, *, logical_name: str = "") -> dict[str, Any]:
    resolved = Path(path).expanduser().resolve()
    return {
        "logical_name": logical_name or resolved.name,
        "path": str(resolved),
        "exists": resolved.exists(),
        "sha256": _sha256_file(resolved),
    }


def _artifact_refs(paths: Iterable[str | Path]) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in paths:
        if not str(item or "").strip():
            continue
        ref = artifact_ref_v1(item)
        if ref["path"] in seen:
            continue
        seen.add(ref["path"])
        refs.append(ref)
    return refs


def producer_contract_v1(
    *,
    producer_name: str,
    producer_command: str,
    input_artifacts: Iterable[str | Path] = (),
    output_artifacts: Iterable[str | Path] = (),
    schema_versions: dict[str, Any] | None = None,
    runtime_contract_path: str | Path | None = None,
) -> dict[str, Any]:
    runtime_path = str(Path(runtime_contract_path or runtime_contract_path_v1()).expanduser().resolve())
    code_path = (REPO_ROOT / producer_name).resolve()
    stable = {
        "schema_version": SCHEMA_VERSION,
        "producer_name": producer_name,
        "producer_command": producer_command,
        "code_version_git_commit": git_commit_v1(),
        "producer_code_sha256": _sha256_file(code_path),
        "source_dirty_status": git_dirty_status_v1(),
        "runtime_contract_path": runtime_path,
        "runtime_contract_sha256": _sha256_file(Path(runtime_path)),
        "input_artifacts": _artifact_refs(input_artifacts),
        "output_artifacts": [{"path": str(Path(path).expanduser().resolve())} for path in output_artifacts if str(path or "").strip()],
        "schema_versions": schema_versions or {},
    }
    fingerprint = hashlib.sha256(_canonical_bytes(stable)).hexdigest()
    return {
        **stable,
        "generated_at_utc": _now_iso(),
        "deterministic_fingerprint": fingerprint,
    }


def attach_producer_contract_v1(
    payload: dict[str, Any],
    *,
    producer_name: str,
    producer_command: str,
    input_artifacts: Iterable[str | Path] = (),
    output_artifacts: Iterable[str | Path] = (),
    schema_versions: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload["producer_contract_v1"] = producer_contract_v1(
        producer_name=producer_name,
        producer_command=producer_command,
        input_artifacts=input_artifacts,
        output_artifacts=output_artifacts,
        schema_versions=schema_versions,
    )
    return payload
