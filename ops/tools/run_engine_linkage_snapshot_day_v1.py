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


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


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
        if _sha256_bytes(path.read_bytes()) != _sha256_bytes(content):
            raise RuntimeError(f"ImmutableWriteError: ATTEMPTED_REWRITE path={path}")
        return
    _atomic_write(path, content)


def _read_json_obj(p: Path) -> dict:
    obj = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: EXISTING_ENGINE_LINKAGE_NOT_OBJECT: {p}")
    return obj


def _return_if_existing(out_path: Path, expected_day_utc: str) -> int | None:
    """
    Immutable rerun safety:
    - If output already exists for the day, do NOT rewrite.
    - Treat existing artifact as authoritative for that day (after basic validation).
    """
    if not out_path.exists():
        return None

    existing_sha = _sha256_file(out_path)
    existing = _read_json_obj(out_path)

    schema_id = str(existing.get("schema_id") or "").strip()
    day_utc = str(existing.get("day_utc") or "").strip()

    if schema_id != "C2_ENGINE_LINKAGE_V1":
        raise SystemExit(f"FAIL: EXISTING_ENGINE_LINKAGE_SCHEMA_MISMATCH: schema_id={schema_id!r} path={out_path}")
    if day_utc != expected_day_utc:
        raise SystemExit(f"FAIL: EXISTING_ENGINE_LINKAGE_DAY_MISMATCH: day_utc={day_utc!r} expected={expected_day_utc!r} path={out_path}")

    print(f"OK: engine_linkage_exists day_utc={expected_day_utc} path={out_path} sha256={existing_sha} action=EXISTS")
    return 0


def main():
    ap = argparse.ArgumentParser(prog="run_engine_linkage_snapshot_day_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--producer_git_sha", required=True)
    ap.add_argument("--producer_repo", required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()

    linkage = []

    out_dir = TRUTH_ROOT / "engine_linkage_v1" / "snapshots" / day
    out_path = out_dir / "engine_linkage.v1.json"

    existing_rc = _return_if_existing(out_path=out_path, expected_day_utc=day)
    if existing_rc is not None:
        return int(existing_rc)

    out = {
        "schema_id": "C2_ENGINE_LINKAGE_V1",
        "schema_version": 1,
        "produced_utc": f"{day}T00:00:00Z",
        "day_utc": day,
        "producer": {
            "repo": args.producer_repo,
            "git_sha": args.producer_git_sha,
            "module": "ops/tools/run_engine_linkage_snapshot_day_v1.py",
        },
        "status": "NOT_AVAILABLE",
        "reason_codes": ["NO_EXECUTIONS_OR_LIFECYCLE_DATA"],
        "linkage": linkage,
    }

    _immut_write(out_path, _json_bytes(out))

    print(f"OK: wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
