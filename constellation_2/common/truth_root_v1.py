"""
truth_root_v1.py

Single-root authority resolver for C2.

Governance artifacts are sourced from the checked-out repo. Runtime artifacts are
resolved from one authoritative runtime root. `resolve_truth_root()` remains the
compatibility entrypoint for runtime truth callers and now fails closed unless
the runtime root and truth root are provable on disk.
"""

from __future__ import annotations

import os
from pathlib import Path

from constellation_2.common.runtime_contract_v1 import (
    require_truth_root_under_contract,
    resolve_runtime_data_root,
    resolve_truth_sleeves_root,
)


ENV_VAR = "C2_TRUTH_ROOT"
AUTHORITY_MODE_ENV_VAR = "C2_AUTHORITY_MODE"
AUTHORITY_MODE_RUNTIME_COMPAT = "runtime_compat"
AUTHORITY_MODE_GOVERNANCE_PRIMARY = "governance_primary"

REPO_ROOT = Path(__file__).resolve().parents[2]
GOVERNANCE_ROOT = (REPO_ROOT / "governance").resolve()


def _governance_truth_root(repo_root: Path) -> Path:
    _require_under_repo(repo_root, label="repo_root")
    return (resolve_truth_sleeves_root().resolve() / "PRIMARY" / "PAPER").resolve()


def _require_absolute_existing_dir(path: Path, *, label: str) -> Path:
    if not path.is_absolute():
        raise SystemExit(f"FAIL: {label} must be absolute: {path}")
    if (not path.exists()) or (not path.is_dir()):
        raise SystemExit(f"FAIL: {label} must exist and be a directory: {path}")
    return path


def _require_under_root(path: Path, *, root: Path, label: str) -> Path:
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise SystemExit(f"FAIL: {label} must remain under runtime root: {path}") from exc
    return path


def _require_under_repo(path: Path, *, label: str) -> Path:
    try:
        path.relative_to(REPO_ROOT)
    except ValueError as exc:
        raise SystemExit(f"FAIL: {label} must remain under active repo root: {path}") from exc
    return path


def resolve_authority_mode() -> str:
    raw = (os.environ.get(AUTHORITY_MODE_ENV_VAR) or AUTHORITY_MODE_RUNTIME_COMPAT).strip()
    if raw not in {AUTHORITY_MODE_RUNTIME_COMPAT, AUTHORITY_MODE_GOVERNANCE_PRIMARY}:
        raise SystemExit(
            f"FAIL: {AUTHORITY_MODE_ENV_VAR} must be one of "
            f"{AUTHORITY_MODE_RUNTIME_COMPAT},{AUTHORITY_MODE_GOVERNANCE_PRIMARY}: {raw}"
        )
    return raw


def resolve_governance_root() -> Path:
    return _require_absolute_existing_dir(GOVERNANCE_ROOT, label="GOVERNANCE_ROOT")


def resolve_runtime_root() -> Path:
    return _require_absolute_existing_dir(resolve_runtime_data_root().resolve(), label="RUNTIME_ROOT")


def resolve_governance_path(*parts: str) -> Path:
    p = resolve_governance_root()
    for part in parts:
        p = p / part
    return p.resolve()


def resolve_runtime_path(*parts: str) -> Path:
    p = resolve_runtime_root()
    for part in parts:
        p = p / part
    return p.resolve()


def resolve_runtime_truth_root() -> Path:
    default_root = _require_absolute_existing_dir(
        (resolve_truth_sleeves_root().resolve() / "PRIMARY" / "PAPER").resolve(),
        label="RUNTIME_TRUTH_ROOT",
    )

    raw = (os.environ.get(ENV_VAR) or "").strip()
    if not raw:
        return default_root

    return require_truth_root_under_contract(
        _require_absolute_existing_dir(Path(raw).expanduser().resolve(), label=ENV_VAR)
    )


def resolve_truth_root(*, repo_root: Path) -> Path:
    repo_root_resolved = Path(repo_root).resolve()
    _require_under_repo(repo_root_resolved, label="repo_root")
    authority_mode = resolve_authority_mode()
    if authority_mode == AUTHORITY_MODE_GOVERNANCE_PRIMARY:
        default_root = _require_absolute_existing_dir(_governance_truth_root(repo_root_resolved), label="GOVERNANCE_TRUTH_ROOT")
        raw = (os.environ.get(ENV_VAR) or "").strip()
        if not raw:
            return default_root
        return require_truth_root_under_contract(
            _require_absolute_existing_dir(Path(raw).expanduser().resolve(), label=ENV_VAR)
        )
    return resolve_runtime_truth_root()


def truth_subpath(repo_root: Path, *parts: str) -> Path:
    root = resolve_truth_root(repo_root=repo_root)
    p = root
    for part in parts:
        p = p / part
    return p.resolve()
