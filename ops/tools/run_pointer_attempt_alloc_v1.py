#!/usr/bin/env python3
"""
run_pointer_attempt_alloc_v1.py

Atomic attempt allocator for the per-repo run pointer spine.

Writes (append-only, atomic append):
  constellation_2/runtime/truth/run_pointer_v1/attempt_registry.v1.jsonl

Allocates:
  attempt_seq (monotonic int, per DAY_UTC)
  attempt_id  = <DAY>__A<zero_padded_seq>__<git_sha_short>__<config_hash_12>

No operator-supplied attempt_id allowed.
No directory scanning for canonical head.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()
RUNPTR_ROOT = (TRUTH_ROOT / "run_pointer_v1").resolve()
REG_PATH = (RUNPTR_ROOT / "attempt_registry.v1.jsonl").resolve()
LOCK_PATH = (RUNPTR_ROOT / ".attempt_registry.v1.lock").resolve()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _require_day(day: str) -> str:
    d = (day or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise SystemExit(f"FAIL: bad --day_utc: {d!r}")
    return d


def _require_mode(mode: str) -> str:
    m = (mode or "").strip().upper()
    if m not in ("PAPER", "LIVE"):
        raise SystemExit(f"FAIL: bad --mode (expected PAPER|LIVE): {m!r}")
    return m


def _require_hash12(h: str, field: str) -> str:
    s = (h or "").strip().lower()
    if len(s) < 12:
        raise SystemExit(f"FAIL: {field} must be >= 12 hex chars: {s!r}")
    # allow hex-only; fail if not
    for c in s[:12]:
        if c not in "0123456789abcdef":
            raise SystemExit(f"FAIL: {field} must be hex: {s!r}")
    return s


def _lock_acquire() -> int:
    RUNPTR_ROOT.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        raise SystemExit(f"FAIL: lock busy (concurrent allocator): {LOCK_PATH}")
    os.write(fd, f"pid={os.getpid()}\n".encode("utf-8"))
    os.fsync(fd)
    return fd


def _lock_release(fd: int) -> None:
    try:
        os.close(fd)
    finally:
        try:
            os.unlink(str(LOCK_PATH))
        except FileNotFoundError:
            pass


def _read_existing_lines(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            raise SystemExit(f"FAIL: attempt_registry contains invalid JSONL line: {path}")
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def _next_seq(rows: List[Dict[str, Any]], day: str) -> int:
    mx = 0
    for r in rows:
        if str(r.get("day_utc") or "") != day:
            continue
        try:
            v = int(r.get("attempt_seq"))
        except Exception:
            continue
        if v > mx:
            mx = v
    return mx + 1


def _attempt_id(day: str, seq: int, git_sha: str, cfg_hash: str) -> str:
    return f"{day}__A{seq:04d}__{git_sha[:7]}__{cfg_hash[:12]}"


def _atomic_append_jsonl(path: Path, obj: Dict[str, Any]) -> Tuple[str, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    line_sha = _sha256_bytes(line)

    fd = os.open(str(path), os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
    try:
        os.write(fd, line)
        os.fsync(fd)
    finally:
        os.close(fd)

    # fsync directory to harden append visibility
    dfd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)

    return (line_sha, str(path))


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_pointer_attempt_alloc_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--mode", required=True, choices=["PAPER", "LIVE"])
    ap.add_argument("--orchestrator_config_hash", required=True, help="Hex config hash (>=12 chars)")
    ap.add_argument("--git_sha", default="", help="Optional override; defaults to HEAD")
    args = ap.parse_args()

    day = _require_day(args.day_utc)
    mode = _require_mode(args.mode)
    cfg_hash = _require_hash12(args.orchestrator_config_hash, "orchestrator_config_hash")
    git_sha = (str(args.git_sha) or "").strip() or _git_sha()

    lock_fd = _lock_acquire()
    try:
        rows = _read_existing_lines(REG_PATH)
        seq = _next_seq(rows, day)
        aid = _attempt_id(day, seq, git_sha, cfg_hash)

        entry = {
            "schema_id": "C2_RUN_POINTER_ATTEMPT_REGISTRY_V1",
            "day_utc": day,
            "attempt_seq": seq,
            "attempt_id": aid,
            "mode": mode,
            "git_sha": git_sha,
            "orchestrator_config_hash": cfg_hash,
        }

        line_sha, path_s = _atomic_append_jsonl(REG_PATH, entry)

    finally:
        _lock_release(lock_fd)

    # machine-readable single-line output
    out = {
        "ok": True,
        "day_utc": day,
        "attempt_seq": seq,
        "attempt_id": aid,
        "mode": mode,
        "git_sha": git_sha,
        "orchestrator_config_hash": cfg_hash,
        "append_line_sha256": line_sha,
        "path": path_s,
    }
    print(json.dumps(out, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
