#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
from pathlib import Path

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = REPO_ROOT / "constellation_2/runtime/truth"

def _json_bytes(obj):
    return (json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")

def _atomic_write(path: Path, content: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

def _immut_write(path: Path, content: bytes):
    if path.exists():
        if hashlib.sha256(path.read_bytes()).hexdigest() != hashlib.sha256(content).hexdigest():
            raise RuntimeError(f"ImmutableWriteError: ATTEMPTED_REWRITE path={path}")
        return
    _atomic_write(path, content)

def main():
    ap = argparse.ArgumentParser(prog="run_engine_linkage_snapshot_day_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--producer_git_sha", required=True)
    ap.add_argument("--producer_repo", required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()

    linkage = []

    out = {
        "schema_id": "C2_ENGINE_LINKAGE_V1",
        "schema_version": 1,
        "produced_utc": __import__("datetime").datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "day_utc": day,
        "producer": {
            "repo": args.producer_repo,
            "git_sha": args.producer_git_sha,
            "module": "ops/tools/run_engine_linkage_snapshot_day_v1.py"
        },
        "status": "NOT_AVAILABLE",
        "reason_codes": ["NO_EXECUTIONS_OR_LIFECYCLE_DATA"],
        "linkage": linkage
    }

    out_dir = TRUTH_ROOT / "engine_linkage_v1" / "snapshots" / day
    out_path = out_dir / "engine_linkage.v1.json"
    _immut_write(out_path, _json_bytes(out))

    print(f"OK: wrote {out_path}")

if __name__ == "__main__":
    raise SystemExit(main())
