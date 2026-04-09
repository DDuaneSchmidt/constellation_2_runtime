#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Dict, Iterable, List

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2].resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
RELEASES_ROOT = Path("/home/node/constellation_releases").resolve()

INCLUDED_ROOTS = [
    "ops",
    "constellation_2",
    "governance",
]
OPTIONAL_FILES = [
    "repo_role.v1.json",
]
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RELEASES/release_manifest.v1.schema.json"


def _utc_now_compact() -> str:
    return datetime.now(UTC).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _git_sha_or_fail(repo_root: Path) -> str:
    from constellation_2.common.deployment_state_machine_v1 import git_sha_or_fail

    return git_sha_or_fail(repo_root)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _copy_tree_filtered(src_root: Path, dst_root: Path) -> List[str]:
    copied: List[str] = []
    for src_path in sorted(src_root.rglob("*"), key=lambda p: str(p)):
        if src_path.is_dir():
            continue
        rel_from_root = src_path.relative_to(REPO_ROOT)
        if rel_from_root.parts[:2] == ("constellation_2", "runtime"):
            continue
        if "__pycache__" in rel_from_root.parts:
            continue
        dst_path = (dst_root / rel_from_root).resolve()
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_path, dst_path)
        copied.append(str(rel_from_root))
    return copied


def _copy_required_release_files(*, repo_root: Path, release_root: Path) -> List[str]:
    copied: List[str] = []
    for rel in INCLUDED_ROOTS:
        src = (repo_root / rel).resolve()
        if not src.exists() or not src.is_dir():
            raise SystemExit(f"FAIL: required release root missing: {src}")
        copied.extend(_copy_tree_filtered(src, release_root))
    for rel in OPTIONAL_FILES:
        src = (repo_root / rel).resolve()
        if not src.exists() or not src.is_file():
            continue
        dst = (release_root / rel).resolve()
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        copied.append(rel)
    return sorted(set(copied))


def _included_file_hashes(*, release_root: Path, included_files: Iterable[str]) -> Dict[str, str]:
    hashes: Dict[str, str] = {}
    for rel in included_files:
        path = (release_root / rel).resolve()
        if not path.exists() or not path.is_file():
            raise SystemExit(f"FAIL: copied release file missing before manifest: {path}")
        hashes[str(rel)] = _sha256_file(path)
    return hashes


def main() -> int:
    ap = argparse.ArgumentParser(prog="build_constellation_release_v1")
    ap.add_argument("--release_id", default="", help="Optional explicit release id")
    args = ap.parse_args()

    from constellation_2.common.deployment_state_machine_v1 import require_clean_git_worktree_or_fail

    require_clean_git_worktree_or_fail(REPO_ROOT)
    git_sha = _git_sha_or_fail(REPO_ROOT)
    release_id = str(args.release_id or "").strip() or f"{_utc_now_compact()}__{git_sha[:12]}"
    release_root = (RELEASES_ROOT / release_id).resolve()
    if release_root.exists():
        raise SystemExit(f"FAIL: release root already exists: {release_root}")

    release_root.mkdir(parents=True, exist_ok=False)
    included_files = _copy_required_release_files(repo_root=REPO_ROOT, release_root=release_root)
    file_hashes = _included_file_hashes(release_root=release_root, included_files=included_files)

    manifest = {
        "schema_id": "release_manifest.v1",
        "schema_version": "v1",
        "release_id": release_id,
        "git_sha": git_sha,
        "source_root": str(REPO_ROOT),
        "release_root": str(release_root),
        "included_files": included_files,
        "included_file_hashes": file_hashes,
        "generated_at_utc": _utc_now_iso(),
    }

    if str(manifest["release_root"]) != str(release_root):
        raise SystemExit("FAIL: release manifest root mismatch before write")

    if str(manifest["source_root"]) != str(REPO_ROOT):
        raise SystemExit("FAIL: release manifest source mismatch before write")

    if not file_hashes:
        raise SystemExit("FAIL: release build copied no files")

    sys_path_insert = str(release_root)
    import sys

    if sys_path_insert not in sys.path:
        sys.path.insert(0, sys_path_insert)
    from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
    from constellation_2.phaseC.lib.canon_json_v1 import canonical_json_bytes_v1

    validate_against_repo_schema_v1(manifest, release_root, SCHEMA_RELPATH)
    (release_root / "release_manifest.v1.json").write_bytes(canonical_json_bytes_v1(manifest) + b"\n")

    print(
        json.dumps(
            {
                "release_id": release_id,
                "release_root": str(release_root),
                "release_manifest_path": str((release_root / "release_manifest.v1.json").resolve()),
                "included_file_count": len(included_files),
                "git_sha": git_sha,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
