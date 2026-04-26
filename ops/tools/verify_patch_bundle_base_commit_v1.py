#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.repo_protection_common_v1 import (
    CANONICAL_REPO_ROOT,
    PATCH_INBOX_ROOT,
    run_git_v1,
)


def _canonical_head_commit_v1(*, canonical_repo_root: Path) -> str:
    proc = run_git_v1(repo_root=canonical_repo_root, args=["rev-parse", "HEAD"])
    if proc.returncode != 0:
        raise RuntimeError(
            f"git rev-parse HEAD failed rc={proc.returncode} stderr={proc.stderr.strip()!r}"
        )
    head = proc.stdout.strip()
    if not head:
        raise RuntimeError("git rev-parse HEAD returned empty output")
    return head


def _load_manifest_v1(*, manifest_path: Path) -> dict:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("manifest is not a JSON object")
    return payload


def evaluate_patch_bundle_base_commit_v1(
    *,
    task_id: str,
    canonical_repo_root: Path = CANONICAL_REPO_ROOT,
    patch_inbox_root: Path = PATCH_INBOX_ROOT,
) -> dict:
    task = str(task_id or "").strip()
    if not task:
        return {
            "status": "FAIL",
            "code": "PATCH_BUNDLE_TASK_ID_EMPTY",
            "task_id": "",
        }

    canonical_head = ""
    try:
        canonical_head = _canonical_head_commit_v1(canonical_repo_root=canonical_repo_root)
    except Exception as exc:
        return {
            "status": "FAIL",
            "code": "PATCH_BUNDLE_CANONICAL_HEAD_UNAVAILABLE",
            "task_id": task,
            "error": f"{type(exc).__name__}:{exc}",
        }

    bundle_root = (patch_inbox_root / task).resolve()
    manifest_path = (bundle_root / "manifest.json").resolve()
    if not manifest_path.exists() or not manifest_path.is_file():
        return {
            "status": "FAIL",
            "code": "PATCH_BUNDLE_MANIFEST_MISSING",
            "task_id": task,
            "bundle_root": str(bundle_root),
            "manifest_path": str(manifest_path),
            "canonical_head": canonical_head,
        }

    try:
        manifest = _load_manifest_v1(manifest_path=manifest_path)
    except Exception as exc:
        return {
            "status": "FAIL",
            "code": "PATCH_BUNDLE_MANIFEST_INVALID",
            "task_id": task,
            "manifest_path": str(manifest_path),
            "canonical_head": canonical_head,
            "error": f"{type(exc).__name__}:{exc}",
        }

    manifest_base_commit = str(manifest.get("base_commit") or "").strip()
    if not manifest_base_commit:
        return {
            "status": "FAIL",
            "code": "PATCH_BUNDLE_BASE_COMMIT_MISSING",
            "task_id": task,
            "manifest_path": str(manifest_path),
            "canonical_head": canonical_head,
        }

    if manifest_base_commit != canonical_head:
        return {
            "status": "FAIL",
            "code": "PATCH_BUNDLE_BASE_COMMIT_MISMATCH",
            "task_id": task,
            "manifest_path": str(manifest_path),
            "manifest_base_commit": manifest_base_commit,
            "canonical_head": canonical_head,
            "instruction": "regenerate bundle from current canonical HEAD",
        }

    return {
        "status": "PASS",
        "code": "PATCH_BUNDLE_BASE_COMMIT_MATCH",
        "task_id": task,
        "manifest_path": str(manifest_path),
        "manifest_base_commit": manifest_base_commit,
        "canonical_head": canonical_head,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="verify_patch_bundle_base_commit_v1")
    ap.add_argument("--task_id", required=True)
    args = ap.parse_args(argv)

    payload = evaluate_patch_bundle_base_commit_v1(task_id=args.task_id)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
