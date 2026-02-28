#!/usr/bin/env python3
"""
run_pointer_append_v1.py

Append-only canonical pointer index for the per-repo run pointer spine.

Writes (append-only, atomic append):
  constellation_2/runtime/truth/run_pointer_v1/canonical_pointer_index.v1.jsonl

Dual-head semantics:
- Display head: highest pointer_seq (any status)
- Authority head: highest pointer_seq where authoritative=true and status=PASS

This tool only appends entries; head derivation is performed by consumers.
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
IDX_PATH = (RUNPTR_ROOT / "canonical_pointer_index.v1.jsonl").resolve()
LOCK_PATH = (RUNPTR_ROOT / ".canonical_pointer_index.v1.lock").resolve()


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


def _require_status(st: str) -> str:
    s = (st or "").strip().upper()
    if s not in ("PASS", "FAIL", "OK_WITH_SOFT_FAILS"):
        raise SystemExit(f"FAIL: bad --status (PASS|FAIL|OK_WITH_SOFT_FAILS): {s!r}")
    return s


def _require_bool_flag(v: str, field: str) -> bool:
    s = (v or "").strip().upper()
    if s not in ("YES", "NO"):
        raise SystemExit(f"FAIL: {field} must be YES|NO: {s!r}")
    return s == "YES"


def _require_hash(h: str, field: str, n: int) -> str:
    s = (h or "").strip().lower()
    if len(s) < n:
        raise SystemExit(f"FAIL: {field} must be >= {n} hex chars: {s!r}")
    for c in s[:n]:
        if c not in "0123456789abcdef":
            raise SystemExit(f"FAIL: {field} must be hex: {s!r}")
    return s


def _require_produced_utc_day0(day: str, produced_utc: str) -> str:
    s = (produced_utc or "").strip()
    expected = f"{day}T00:00:00Z"
    if s != expected:
        raise SystemExit(f"FAIL: produced_utc must be deterministic day-stamp: expected={expected!r} got={s!r}")
    return s


def _lock_acquire() -> int:
    RUNPTR_ROOT.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        raise SystemExit(f"FAIL: lock busy (concurrent pointer append): {LOCK_PATH}")
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


def _read_last_pointer_seq(path: Path) -> int:
    if not path.exists():
        return 0
    last = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            raise SystemExit(f"FAIL: canonical_pointer_index has invalid JSONL line: {path}")
        if isinstance(obj, dict):
            try:
                ps = int(obj.get("pointer_seq"))
            except Exception:
                continue
            if ps > last:
                last = ps
    return last


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

    dfd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)

    return (line_sha, str(path))


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_pointer_append_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--attempt_id", required=True)
    ap.add_argument("--attempt_seq", required=True, type=int)
    ap.add_argument("--mode", required=True, choices=["PAPER", "LIVE"])
    ap.add_argument("--status", required=True, help="PASS|FAIL|OK_WITH_SOFT_FAILS")
    ap.add_argument("--authoritative", required=True, help="YES|NO")
    ap.add_argument("--policy_hash", required=True, help="Hex policy hash (>=12 chars)")
    ap.add_argument("--orchestrator_config_hash", required=True, help="Hex config hash (>=12 chars)")
    ap.add_argument("--produced_utc", required=True, help="Must equal <DAY>T00:00:00Z")
    ap.add_argument("--points_to", required=True, help="Path to gate_stack_verdict_v1 artifact (must contain gate_stack_verdict_v1)")
    ap.add_argument("--git_sha", default="", help="Optional override; defaults to HEAD")
    args = ap.parse_args()

    day = _require_day(args.day_utc)
    mode = _require_mode(args.mode)
    status = _require_status(args.status)
    authoritative = _require_bool_flag(args.authoritative, "authoritative")
    policy_hash = _require_hash(args.policy_hash, "policy_hash", 12)
    cfg_hash = _require_hash(args.orchestrator_config_hash, "orchestrator_config_hash", 12)
    produced_utc = _require_produced_utc_day0(day, args.produced_utc)
    git_sha = (str(args.git_sha) or "").strip() or _git_sha()

    points_to = str(args.points_to).strip()
    if "gate_stack_verdict_v1" not in points_to:
        raise SystemExit("FAIL: points_to must reference gate_stack_verdict_v1 (path must contain token)")

    lock_fd = _lock_acquire()
    try:
        last_seq = _read_last_pointer_seq(IDX_PATH)
        pointer_seq = last_seq + 1

        entry = {
            "schema_id": "C2_RUN_POINTER_CANONICAL_POINTER_INDEX_V1",
            "pointer_seq": pointer_seq,
            "day_utc": day,
            "attempt_id": str(args.attempt_id).strip(),
            "attempt_seq": int(args.attempt_seq),
            "mode": mode,
            "status": status,
            "authoritative": bool(authoritative),
            "policy_hash": policy_hash,
            "orchestrator_config_hash": cfg_hash,
            "produced_utc": produced_utc,
            "producer_git_sha": git_sha,
            "points_to": points_to,
        }

        line_sha, path_s = _atomic_append_jsonl(IDX_PATH, entry)

    finally:
        _lock_release(lock_fd)

    out = {
        "ok": True,
        "pointer_seq": pointer_seq,
        "day_utc": day,
        "attempt_id": entry["attempt_id"],
        "status": status,
        "authoritative": bool(authoritative),
        "append_line_sha256": line_sha,
        "path": path_s,
    }
    print(json.dumps(out, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
