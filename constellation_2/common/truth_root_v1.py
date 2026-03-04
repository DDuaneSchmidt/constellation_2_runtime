"""
truth_root_v1.py

Canonical truth root resolver for C2 tools (sleeve partition routing).

Behavior:
- If env var C2_TRUTH_ROOT is set, it is authoritative (must be absolute + existing dir).
- Otherwise defaults to <repo_root>/constellation_2/runtime/truth.

Fail-closed when env is provided but invalid.
"""

from __future__ import annotations

import os
from pathlib import Path


ENV_VAR = "C2_TRUTH_ROOT"


def resolve_truth_root(*, repo_root: Path) -> Path:
    repo_root = Path(repo_root).resolve()
    default_root = (repo_root / "constellation_2/runtime/truth").resolve()

    raw = (os.environ.get(ENV_VAR) or "").strip()
    if not raw:
        return default_root

    pr = Path(raw).expanduser().resolve()
    if not pr.is_absolute():
        raise SystemExit(f"FAIL: {ENV_VAR} must be absolute: {pr}")
    if (not pr.exists()) or (not pr.is_dir()):
        raise SystemExit(f"FAIL: {ENV_VAR} must exist and be a directory: {pr}")
    return pr

def truth_subpath(repo_root: Path, *parts: str) -> Path:
    root = resolve_truth_root(repo_root=repo_root)
    p = root
    for part in parts:
        p = p / part
    return p.resolve()
