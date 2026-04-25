#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import stat
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


CANONICAL_REPO_ROOT = Path("/home/node/constellation").resolve()
AGENT_WORKSPACE_ROOT = Path("/home/node/constellation_agent_workspace").resolve()
PATCH_INBOX_ROOT = Path("/home/node/constellation_patch_inbox").resolve()
RUNTIME_DATA_ROOT = Path("/home/node/constellation_runtime_data").resolve()
RELEASES_ROOT = Path("/home/node/constellation_releases").resolve()
PROTECTION_ROOT = (RUNTIME_DATA_ROOT / "repo_protection_v1").resolve()
PROTECTION_STATUS_PATH = (PROTECTION_ROOT / "status.json").resolve()
PROTECTION_AUDIT_PATH = (PROTECTION_ROOT / "audit.jsonl").resolve()


def utc_now_iso_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def run_git_v1(*, repo_root: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repo_root), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def git_status_porcelain_paths_v1(repo_root: Path) -> list[str]:
    proc = run_git_v1(repo_root=repo_root, args=["status", "--porcelain"])
    if proc.returncode != 0:
        raise RuntimeError(
            f"git status failed rc={proc.returncode} stderr={proc.stderr.strip()!r}"
        )
    paths: list[str] = []
    for line in proc.stdout.splitlines():
        row = line.rstrip()
        if not row:
            continue
        if len(row) < 4:
            continue
        paths.append(row[3:])
    return paths


def require_canonical_repo_root_v1(repo_root: Path) -> None:
    resolved = Path(repo_root).resolve()
    if resolved != CANONICAL_REPO_ROOT:
        raise SystemExit(
            f"FAIL: expected canonical repo root {CANONICAL_REPO_ROOT}, got {resolved}"
        )


def read_protection_status_v1() -> dict[str, Any]:
    if not PROTECTION_STATUS_PATH.exists() or not PROTECTION_STATUS_PATH.is_file():
        return {
            "status": "UNKNOWN",
            "protected": False,
            "canonical_repo_root": str(CANONICAL_REPO_ROOT),
            "reason": "status_file_missing",
        }
    try:
        payload = json.loads(PROTECTION_STATUS_PATH.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - defensive decode guard
        return {
            "status": "UNKNOWN",
            "protected": False,
            "canonical_repo_root": str(CANONICAL_REPO_ROOT),
            "reason": f"status_file_invalid:{type(exc).__name__}",
        }
    if not isinstance(payload, dict):
        return {
            "status": "UNKNOWN",
            "protected": False,
            "canonical_repo_root": str(CANONICAL_REPO_ROOT),
            "reason": "status_file_not_object",
        }
    return payload


def write_protection_status_v1(
    *,
    protected: bool,
    actor: str,
    reason: str,
    changed_path_count: int,
) -> Path:
    PROTECTION_ROOT.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "canonical_repo_protection_status.v1",
        "status": "PROTECTED" if protected else "UNPROTECTED",
        "protected": bool(protected),
        "canonical_repo_root": str(CANONICAL_REPO_ROOT),
        "changed_path_count": int(changed_path_count),
        "updated_at_utc": utc_now_iso_v1(),
        "actor": actor,
        "reason": reason,
    }
    PROTECTION_STATUS_PATH.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return PROTECTION_STATUS_PATH


def append_protection_audit_v1(
    *,
    action: str,
    actor: str,
    reason: str,
    changed_path_count: int,
    status: str,
) -> Path:
    PROTECTION_ROOT.mkdir(parents=True, exist_ok=True)
    row = {
        "action": action,
        "actor": actor,
        "reason": reason,
        "changed_path_count": int(changed_path_count),
        "status": status,
        "recorded_at_utc": utc_now_iso_v1(),
        "canonical_repo_root": str(CANONICAL_REPO_ROOT),
    }
    with PROTECTION_AUDIT_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, sort_keys=True) + "\n")
    return PROTECTION_AUDIT_PATH


def _iter_protect_targets_v1(repo_root: Path) -> list[Path]:
    names = [
        "constellation_2",
        "ops",
        "governance",
        "docs",
        "runtime",
        "package.json",
        "repo_role.v1.json",
        ".gitignore",
    ]
    targets: list[Path] = []
    for name in names:
        candidate = (repo_root / name).resolve()
        if candidate.exists():
            targets.append(candidate)
    return targets


def _set_mode_v1(path: Path, *, protect: bool) -> bool:
    st = path.lstat()
    mode = st.st_mode
    if protect:
        new_mode = mode & ~stat.S_IWUSR & ~stat.S_IWGRP & ~stat.S_IWOTH
    else:
        new_mode = mode | stat.S_IWUSR
        if stat.S_ISDIR(mode):
            new_mode = new_mode | stat.S_IXUSR
    if new_mode == mode:
        return False
    os.chmod(path, new_mode)
    return True


def set_canonical_repo_protection_v1(*, protect: bool) -> int:
    changed = 0
    for target in _iter_protect_targets_v1(CANONICAL_REPO_ROOT):
        if target.is_file() or target.is_symlink():
            if _set_mode_v1(target, protect=protect):
                changed += 1
            continue
        for root, dirs, files in os.walk(target, topdown=True):
            root_path = Path(root).resolve()
            if root_path.name == ".git":
                dirs[:] = []
                continue
            if _set_mode_v1(root_path, protect=protect):
                changed += 1
            for name in files:
                p = (root_path / name).resolve()
                try:
                    if _set_mode_v1(p, protect=protect):
                        changed += 1
                except OSError:
                    continue
    return changed


def require_runtime_output_outside_repo_runtime_v1(path: Path) -> None:
    resolved = Path(path).resolve()
    forbidden_root = (CANONICAL_REPO_ROOT / "runtime").resolve()
    if resolved == forbidden_root or str(resolved).startswith(str(forbidden_root) + "/"):
        raise SystemExit(
            f"FAIL: runtime output path under canonical repo/runtime is forbidden: {resolved}"
        )
