#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
from pathlib import Path

from ops.tools.repo_protection_common_v1 import AGENT_WORKSPACE_ROOT, CANONICAL_REPO_ROOT


def _run_git(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
    )


def verify_workspace_v1(*, cwd: Path, workspace_root: Path, canonical_root: Path) -> dict:
    if cwd == canonical_root:
        raise SystemExit(
            f"FAIL: workspace verifier rejects canonical root: {canonical_root}"
        )
    if not str(cwd).startswith(str(workspace_root) + "/"):
        raise SystemExit(
            f"FAIL: workspace must be under {workspace_root}; got {cwd}"
        )

    in_tree = _run_git(["rev-parse", "--is-inside-work-tree"], cwd)
    if in_tree.returncode != 0 or in_tree.stdout.strip() != "true":
        raise SystemExit(f"FAIL: not a git worktree: {cwd}")

    branch = _run_git(["branch", "--show-current"], cwd)
    commit = _run_git(["rev-parse", "HEAD"], cwd)
    status = _run_git(["status", "--short"], cwd)
    dirty_lines = [line for line in status.stdout.splitlines() if line.strip()]
    return {
        "status": "OK",
        "workspace_path": str(cwd),
        "branch": branch.stdout.strip(),
        "commit": commit.stdout.strip(),
        "dirty_status": "DIRTY" if dirty_lines else "CLEAN",
        "dirty_path_count": len(dirty_lines),
    }


def main() -> int:
    cwd = Path.cwd().resolve()
    payload = verify_workspace_v1(
        cwd=cwd,
        workspace_root=AGENT_WORKSPACE_ROOT,
        canonical_root=CANONICAL_REPO_ROOT,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
