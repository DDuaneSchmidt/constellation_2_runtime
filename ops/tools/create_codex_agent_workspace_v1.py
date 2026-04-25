#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path

from ops.tools.repo_protection_common_v1 import (
    AGENT_WORKSPACE_ROOT,
    CANONICAL_REPO_ROOT,
    git_status_porcelain_paths_v1,
)


def _sanitize_token(value: str) -> str:
    raw = str(value or "").strip().lower()
    token = re.sub(r"[^a-z0-9._-]+", "-", raw).strip("-")
    return token or "task"


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=str(CANONICAL_REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )


def main() -> int:
    ap = argparse.ArgumentParser(prog="create_codex_agent_workspace_v1")
    ap.add_argument("--task_id", required=True)
    ap.add_argument("--branch", default="")
    args = ap.parse_args()

    if Path.cwd().resolve() != CANONICAL_REPO_ROOT:
        raise SystemExit(
            f"FAIL: run this tool from canonical root only: {CANONICAL_REPO_ROOT}"
        )

    dirty_paths = git_status_porcelain_paths_v1(CANONICAL_REPO_ROOT)
    if dirty_paths:
        raise SystemExit(
            "FAIL: canonical repo is dirty; refuse workspace creation until clean"
        )

    task_id = _sanitize_token(args.task_id)
    branch = str(args.branch or "").strip() or f"agent/{task_id}"
    workspace_path = (AGENT_WORKSPACE_ROOT / task_id).resolve()
    AGENT_WORKSPACE_ROOT.mkdir(parents=True, exist_ok=True)

    if workspace_path.exists():
        raise SystemExit(f"FAIL: workspace path already exists: {workspace_path}")

    add_proc = _run(["git", "worktree", "add", "-b", branch, str(workspace_path)])
    if add_proc.returncode != 0:
        fallback = _run(["git", "worktree", "add", str(workspace_path), branch])
        if fallback.returncode != 0:
            raise SystemExit(
                "FAIL: git worktree add failed "
                f"primary={add_proc.stderr.strip()!r} "
                f"fallback={fallback.stderr.strip()!r}"
            )

    print(f"CODEX_EDIT_ROOT={workspace_path}")
    print(f"FORBIDDEN_CANONICAL_EDIT_ROOT={CANONICAL_REPO_ROOT}")
    print("DO NOT EDIT /home/node/constellation")
    print(
        json.dumps(
            {
                "status": "OK",
                "task_id": task_id,
                "branch": branch,
                "workspace_path": str(workspace_path),
                "canonical_repo_root": str(CANONICAL_REPO_ROOT),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

