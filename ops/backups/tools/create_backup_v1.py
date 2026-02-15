#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path("/home/node/constellation_2_runtime")
BACKUP_ROOT = Path("/home/node/constellation_2_backups")
LOCK_PATH = BACKUP_ROOT / ".backup_lock_v1.json"

DEFAULT_EXCLUDES = [
    "./.git",
    "./node_modules",
    "./.venv",
    "./runtime/truth",
    "*/__pycache__/*",
    "*.pyc",
]

def die(msg: str):
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(2)

def now_utc():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def sha256_file(p: Path):
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def run(cmd, cwd=None):
    return subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

def ensure_repo():
    if Path.cwd() != REPO_ROOT:
        die(f"Must run from {REPO_ROOT}")

def acquire_lock():
    if LOCK_PATH.exists():
        die("Backup lock exists. Use unlock tool.")
    LOCK_PATH.write_text(json.dumps({"created": now_utc()}))
    os.chmod(LOCK_PATH, 0o600)

def release_lock():
    if LOCK_PATH.exists():
        LOCK_PATH.unlink()

def main():
    os.umask(0o077)
    ensure_repo()
    acquire_lock()
    try:
        ts = now_utc()
        name = f"manual_operator_checkpoint_{ts}"
        out_dir = BACKUP_ROOT / "MANUAL" / name
        tmp_dir = BACKUP_ROOT / "_tmp" / f"tmp_{ts}"

        tmp_dir.mkdir(parents=True)
        os.chmod(tmp_dir, 0o700)

        tar_path = tmp_dir / "repo_snapshot.tar.gz"

        args = ["tar", "-czf", str(tar_path)]
        for e in DEFAULT_EXCLUDES:
            args.append(f"--exclude={e}")
        args.append(".")

        cp = run(args, cwd=REPO_ROOT)
        if cp.returncode != 0:
            die(cp.stderr)

        sha = sha256_file(tar_path)
        size = tar_path.stat().st_size

        listing = run(["tar", "-tzf", str(tar_path)])
        if listing.returncode != 0:
            die("tar verification failed")

        manifest = {
            "schema_version": "backup_manifest.v1",
            "created_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
            "repo_root": str(REPO_ROOT),
            "artifact": {
                "tar_gz_bytes": size,
                "tar_gz_sha256": sha,
                "sha_verified": True,
                "tar_list_verified": True,
            },
            "authoritative_truth": "governance + git + runtime/truth",
            "notes": "Disaster recovery only. Not authoritative."
        }

        (tmp_dir / "backup_manifest.json").write_text(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n"
        )

        out_dir.parent.mkdir(parents=True, exist_ok=True)
        tmp_dir.rename(out_dir)
        os.chmod(out_dir, 0o700)

        print("OK: BACKUP_CREATED")
        print(f"DIR: {out_dir}")
        print(f"SHA256: {sha}")

    finally:
        release_lock()

if __name__ == "__main__":
    main()
