#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

LOCK_NAME = ".canonical_pointer_index.v1.lock"
IDX_NAME = "canonical_pointer_index.v1.jsonl"

def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def _sha256_file(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()

def _json_line(obj: Dict[str, Any]) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(content)
    os.replace(tmp, path)

def _lock_acquire(lock_path: Path) -> int:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.write(fd, f"pid={os.getpid()}\n".encode("utf-8"))
    os.fsync(fd)
    return fd

def _lock_release(fd: int, lock_path: Path) -> None:
    try:
        os.close(fd)
    finally:
        try:
            os.unlink(str(lock_path))
        except FileNotFoundError:
            pass

def _read_last_seq(idx_path: Path) -> int:
    if not idx_path.exists():
        return 0
    last = 0
    for line in idx_path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s:
            continue
        try:
            o = json.loads(s)
        except Exception:
            raise SystemExit(f"FAIL: invalid JSONL pointer index: {idx_path}")
        if not isinstance(o, dict):
            continue
        try:
            seq = int(o.get("pointer_seq"))
        except Exception:
            continue
        if seq > last:
            last = seq
    return last

def _append_jsonl(idx_path: Path, obj: Dict[str, Any]) -> None:
    idx_path.parent.mkdir(parents=True, exist_ok=True)
    line = _json_line(obj)
    fd = os.open(str(idx_path), os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
    try:
        os.write(fd, line)
        os.fsync(fd)
    finally:
        os.close(fd)

def main() -> int:
    ap = argparse.ArgumentParser(prog="c2_baseline_readiness_publish_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--attempt_id", required=True)
    ap.add_argument("--ib_account", required=True)
    ap.add_argument("--nav_path", required=True)
    ap.add_argument("--producer_git_sha", required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    attempt_id = str(args.attempt_id).strip()
    acct = str(args.ib_account).strip()
    nav_path = Path(str(args.nav_path)).resolve()
    sha = str(args.producer_git_sha).strip()

    if acct != "DUO847203":
        raise SystemExit(f"FAIL: BASELINE_READY_ACCOUNT_MISMATCH: {acct}")

    if not nav_path.exists():
        raise SystemExit(f"FAIL: NAV_MISSING: {nav_path}")

    nav_obj = json.loads(nav_path.read_text(encoding="utf-8"))
    nav = nav_obj.get("nav") or {}
    nav_total = nav.get("nav_total")
    if not isinstance(nav_total, int) or nav_total <= 0:
        raise SystemExit(f"FAIL: NAV_TOTAL_NOT_POSITIVE: {nav_total!r}")

    root = (TRUTH_ROOT / "readiness_v1" / "baseline_ready" / day).resolve()
    attempts_dir = (root / "attempts" / attempt_id).resolve()
    attempts_dir.mkdir(parents=True, exist_ok=True)

    out_attempt = (attempts_dir / "baseline_ready.v1.json").resolve()
    out_canon = (root / "baseline_ready.v1.json").resolve()

    payload = {
        "schema_id": "C2_BASELINE_READY_V1",
        "schema_version": 1,
        "day_utc": day,
        "ib_account": acct,
        "produced_utc": f"{day}T00:00:00Z",
        "nav": {"path": str(nav_path), "sha256": _sha256_file(nav_path), "nav_total": int(nav_total)},
        "producer": {"repo": "constellation_2_runtime", "git_sha": sha, "module": "ops/run/c2_baseline_readiness_publish_v1.py"},
        "attempt_id": attempt_id,
    }
    b = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")

    # Attempt-scoped write (always)
    _atomic_write(out_attempt, b)
    # Canonical day write (overwrite-allowed because it is derived head; still deterministic bytes for same head)
    _atomic_write(out_canon, b)

    # Pointer append with lock
    idx_path = (root / IDX_NAME).resolve()
    lock_path = (root / LOCK_NAME).resolve()

    lock_fd = _lock_acquire(lock_path)
    try:
        last = _read_last_seq(idx_path)
        seq = last + 1
        entry = {
            "schema_id": "C2_BASELINE_READY_POINTER_INDEX_V1",
            "pointer_seq": int(seq),
            "day_utc": day,
            "attempt_id": attempt_id,
            "status": "PASS",
            "authoritative": True,
            "produced_utc": f"{day}T00:00:00Z",
            "producer_git_sha": sha,
            "points_to": str(out_attempt),
            "nav_sha256": payload["nav"]["sha256"],
        }
        _append_jsonl(idx_path, entry)
    finally:
        _lock_release(lock_fd, lock_path)

    print(f"OK: BASELINE_READY_PUBLISHED day_utc={day} attempt_id={attempt_id} pointer_seq={seq} path={out_attempt}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
