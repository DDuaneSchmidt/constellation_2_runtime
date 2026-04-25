#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.repo_protection_common_v1 import (
    CANONICAL_REPO_ROOT,
    PATCH_INBOX_ROOT,
    git_status_porcelain_paths_v1,
)


FORBIDDEN_PATCH_PREFIXES = (
    "runtime/",
    "runtime/exports/",
    "runtime/event_log.jsonl",
    "/home/node/constellation_runtime_data",
    "/home/node/constellation_2_runtime",
)


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=str(CANONICAL_REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )


def _parse_patch_paths(patch_text: str) -> list[str]:
    paths: list[str] = []
    for line in patch_text.splitlines():
        if not line.startswith("diff --git "):
            continue
        parts = line.split()
        if len(parts) < 4:
            continue
        b_path = str(parts[3])
        if b_path.startswith("b/"):
            b_path = b_path[2:]
        paths.append(b_path.strip())
    return sorted(set(paths))


def _is_forbidden(path: str) -> bool:
    normalized = str(path or "").strip()
    return any(
        normalized == prefix.rstrip("/") or normalized.startswith(prefix)
        for prefix in FORBIDDEN_PATCH_PREFIXES
    )


def _unprotect_for_intake(task_id: str) -> None:
    reason = f"apply_codex_patch_bundle_v1:{task_id}:patch_intake_authorized_write"
    proc = _run(
        [sys.executable, "ops/tools/unprotect_canonical_repo_for_intake_v1.py", "--reason", reason]
    )
    if proc.returncode != 0:
        raise RuntimeError(f"unprotect failed: {proc.stderr.strip() or proc.stdout.strip()}")


def _protect_after_intake() -> None:
    proc = _run([sys.executable, "ops/tools/protect_canonical_repo_v1.py"])
    if proc.returncode != 0:
        raise RuntimeError(f"protect failed: {proc.stderr.strip() or proc.stdout.strip()}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="apply_codex_patch_bundle_v1")
    ap.add_argument("--task_id", required=True)
    ap.add_argument("--commit_message", default="")
    args = ap.parse_args(argv)

    if git_status_porcelain_paths_v1(CANONICAL_REPO_ROOT):
        raise SystemExit("FAIL: canonical repo must be clean before intake")

    task_id = str(args.task_id or "").strip()
    if not task_id:
        raise SystemExit("FAIL: task_id empty")

    bundle_root = (PATCH_INBOX_ROOT / task_id).resolve()
    patch_path = (bundle_root / "changes.patch").resolve()
    manifest_path = (bundle_root / "manifest.json").resolve()
    if not bundle_root.exists() or not bundle_root.is_dir():
        raise SystemExit(f"FAIL: patch bundle missing: {bundle_root}")
    if not manifest_path.exists() or not manifest_path.is_file():
        raise SystemExit(f"FAIL: manifest missing: {manifest_path}")
    if not patch_path.exists() or not patch_path.is_file():
        raise SystemExit(f"FAIL: patch missing: {patch_path}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise SystemExit("FAIL: manifest must be a JSON object")

    patch_text = patch_path.read_text(encoding="utf-8")
    touched_paths = _parse_patch_paths(patch_text)
    forbidden = [path for path in touched_paths if _is_forbidden(path)]
    if forbidden:
        raise SystemExit(
            "FAIL: patch touches forbidden runtime paths: "
            + json.dumps(sorted(forbidden), sort_keys=True)
        )

    start_commit = _run(["git", "rev-parse", "HEAD"]).stdout.strip()
    tests_to_run = manifest.get("tests", [])
    if tests_to_run is None:
        tests_to_run = []
    if not isinstance(tests_to_run, list):
        raise SystemExit("FAIL: manifest.tests must be a list when present")

    unprotected = False
    try:
        _unprotect_for_intake(task_id)
        unprotected = True
        apply_proc = _run(["git", "apply", "--index", str(patch_path)])
        if apply_proc.returncode != 0:
            raise RuntimeError(f"git apply failed: {apply_proc.stderr.strip()}")

        for command in tests_to_run:
            cmd = str(command or "").strip()
            if not cmd:
                continue
            test_proc = subprocess.run(
                cmd,
                shell=True,
                cwd=str(CANONICAL_REPO_ROOT),
                capture_output=True,
                text=True,
                check=False,
            )
            if test_proc.returncode != 0:
                raise RuntimeError(
                    "manifest test failed "
                    f"command={cmd!r} stdout={test_proc.stdout[-600:]!r} stderr={test_proc.stderr[-600:]!r}"
                )

        if _run(["git", "diff", "--cached", "--quiet"]).returncode == 0:
            raise RuntimeError("no staged changes after applying patch bundle")

        commit_message = str(args.commit_message or "").strip() or f"Apply Codex patch bundle: {task_id}"
        commit_proc = _run(["git", "commit", "-m", commit_message])
        if commit_proc.returncode != 0:
            raise RuntimeError(f"git commit failed: {commit_proc.stderr.strip()}")

        _protect_after_intake()
        unprotected = False
        end_commit = _run(["git", "rev-parse", "HEAD"]).stdout.strip()
        print(
            json.dumps(
                {
                    "status": "OK",
                    "task_id": task_id,
                    "bundle_root": str(bundle_root),
                    "start_commit": start_commit,
                    "end_commit": end_commit,
                    "touched_path_count": len(touched_paths),
                    "tests_run": tests_to_run,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except Exception as exc:
        _run(["git", "reset", "--hard", "HEAD"])
        _run(["git", "clean", "-fd"])
        if start_commit:
            _run(["git", "reset", "--hard", start_commit])
        if unprotected:
            try:
                _protect_after_intake()
            except Exception:
                pass
        raise SystemExit(f"FAIL: patch intake rolled back: {type(exc).__name__}: {exc}") from exc


if __name__ == "__main__":
    raise SystemExit(main())
