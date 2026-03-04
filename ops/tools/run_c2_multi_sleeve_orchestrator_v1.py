#!/usr/bin/env python3
"""
run_c2_multi_sleeve_orchestrator_v1.py

C2 Multi-Sleeve Orchestrator V1 (fail-closed topology driver)

Reads governed sleeve registry:
  governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json

For each enabled sleeve:
  - resolves sleeve truth root (absolute)
  - runs orchestrator v2 with --truth_root pointing at the sleeve partition
  - records per-sleeve exit code + summary

Always emits a global rollup verdict under canonical truth root:
  constellation_2/runtime/truth/reports/sleeve_rollup_v1/<day>/...

Verdict policy:
  PASS      if all enabled sleeves PASS (orchestrator rc=0 and status != ABORTED)
  DEGRADED  if any sleeve returns non-fatal but run completed (rc=0) with degraded/fail OR sleeve has NO_ACTIVITY (inferred by orchestrator reason codes)
  FAIL      if any enabled sleeve returns rc=0 but status=FAIL (non-aborted failure)
  ABORTED   only for safety breach / topology breach (rc != 0) OR registry invalid OR truth partition invalid

Non-bricking:
  Even if a sleeve fails, this tool must still emit the global rollup artifact.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
REGISTRY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").resolve()

CANONICAL_TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()
ROLLOUP_ROOT = (CANONICAL_TRUTH_ROOT / "reports" / "sleeve_rollup_v1").resolve()

POINTER_INDEX_NAME = "canonical_pointer_index.v1.jsonl"
POINTER_LOCK_NAME = ".canonical_pointer_index.v1.lock"

ORCH_V2 = (REPO_ROOT / "ops/tools/run_c2_paper_day_orchestrator_v2.py").resolve()


def die(msg: str, code: int = 2) -> None:
    print(f"ABORT: {msg}", file=sys.stderr)
    sys.exit(code)


def utc_now_isoz() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def require_day(day: str) -> str:
    d = (day or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        die(f"bad_day expected=YYYY-MM-DD got={d!r}")
    return d


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        die(f"missing_file path={path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        die(f"json_parse_failed path={path} err={type(e).__name__}:{e}")


def require_registry(reg: Dict[str, Any]) -> List[Dict[str, Any]]:
    if reg.get("schema_id") != "c2_sleeve_registry" or reg.get("schema_version") != "v1":
        die(f"registry_schema_mismatch got=({reg.get('schema_id')},{reg.get('schema_version')})")
    sleeves = reg.get("sleeves")
    if not isinstance(sleeves, list):
        die("registry_invalid sleeves must be list")
    return [s for s in sleeves if isinstance(s, dict)]


def canonical_partition(sleeve_id: str, mode: str) -> str:
    return f"truth_sleeves/{sleeve_id}/{mode}"


def resolve_sleeve_truth_root(sleeve: Dict[str, Any]) -> Tuple[str, str, str, Path]:
    sleeve_id = str(sleeve.get("sleeve_id") or "").strip()
    if not sleeve_id:
        die("registry_invalid sleeve_id empty")
    enabled = sleeve.get("enabled")
    if not isinstance(enabled, bool):
        die(f"registry_invalid enabled must be bool sleeve_id={sleeve_id}")
    if not enabled:
        return (sleeve_id, "", "", Path("/dev/null"))

    mode = str(sleeve.get("mode") or "").strip().upper()
    if mode not in ("PAPER", "LIVE"):
        die(f"registry_invalid mode sleeve_id={sleeve_id} got={mode!r}")

    ib_account = str(sleeve.get("ib_account") or "").strip()
    if not ib_account:
        die(f"registry_invalid ib_account empty sleeve_id={sleeve_id}")

    truth_partition = str(sleeve.get("truth_partition") or "").strip()
    exp = canonical_partition(sleeve_id, mode)
    if truth_partition != exp:
        die(f"truth_partition_mismatch sleeve_id={sleeve_id} expected={exp} got={truth_partition}")

    abs_root = (REPO_ROOT / "constellation_2/runtime" / truth_partition).resolve()
    if not abs_root.exists() or (not abs_root.is_dir()):
        die(f"truth_partition_path_missing sleeve_id={sleeve_id} path={abs_root}")

    return (sleeve_id, mode, ib_account, abs_root)

def sha256_file(p: Path) -> str:
    import hashlib
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json_obj(p: Path) -> Dict[str, Any]:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        die(f"json_parse_failed path={p} err={type(e).__name__}:{e}")


def resolve_latest_verdict_pointer(*, verdict_root: Path, day: str, mode: str) -> Tuple[Path, Path, int]:
    """
    Returns:
      (pointer_index_path, points_to_path, pointer_seq)

    Fail-closed if pointer index missing or invalid.
    """
    idx = (verdict_root / day / "canonical_pointer_index.v1.jsonl").resolve()
    if not idx.exists():
        die(f"missing_pointer_index path={idx}")

    best_seq = -1
    best_points_to: Path | None = None

    for line in idx.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s:
            continue
        try:
            o = json.loads(s)
        except Exception:
            die(f"invalid_pointer_index_jsonl path={idx}")
        if not isinstance(o, dict):
            continue
        if str(o.get("mode") or "").strip().upper() != mode:
            continue
        try:
            ps = int(o.get("pointer_seq"))
        except Exception:
            continue
        if ps <= best_seq:
            continue
        pt = str(o.get("points_to") or "").strip()
        if not pt:
            continue
        best_seq = ps
        best_points_to = Path(pt).resolve()

    if best_points_to is None or best_seq < 0:
        die(f"no_pointer_for_mode path={idx} mode={mode}")

    if not best_points_to.exists():
        die(f"pointer_points_to_missing points_to={best_points_to} idx={idx}")

    return idx, best_points_to, best_seq

def run_orchestrator_v2(*, day: str, input_day: str, mode: str, symbol: str, ib_account: str, produced_utc: str, truth_root: Path) -> Tuple[int, str]:
    cmd = [
        "python3",
        str(ORCH_V2),
        "--day_utc",
        day,
        "--input_day_utc",
        input_day,
        "--mode",
        mode,
        "--symbol",
        symbol,
        "--ib_account",
        ib_account,
        "--produced_utc",
        produced_utc,
        "--truth_root",
        str(truth_root),
    ]
    rc = subprocess.call(cmd, cwd=str(REPO_ROOT))
    return (int(rc), " ".join(cmd))

def lock_acquire(lock_path: Path) -> int:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        die(f"pointer_lock_busy path={lock_path}")
    os.write(fd, f"pid={os.getpid()}\n".encode("utf-8"))
    os.fsync(fd)
    return fd


def lock_release(fd: int, lock_path: Path) -> None:
    try:
        os.close(fd)
    finally:
        try:
            os.unlink(str(lock_path))
        except FileNotFoundError:
            pass


def read_last_pointer_seq(idx_path: Path) -> int:
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
            die(f"invalid_pointer_index_jsonl path={idx_path}")
        if not isinstance(o, dict):
            continue
        try:
            ps = int(o.get("pointer_seq"))
        except Exception:
            continue
        if ps > last:
            last = ps
    return last


def atomic_append_jsonl(idx_path: Path, obj: Dict[str, Any]) -> str:
    idx_path.parent.mkdir(parents=True, exist_ok=True)
    line = (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    line_sha = sha256_file_bytes(line)

    fd = os.open(str(idx_path), os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
    try:
        os.write(fd, line)
        os.fsync(fd)
    finally:
        os.close(fd)

    dfd = os.open(str(idx_path.parent), os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)

    return line_sha


def sha256_file_bytes(b: bytes) -> str:
    import hashlib
    return hashlib.sha256(b).hexdigest()

def write_rollup(day: str, payload: Dict[str, Any]) -> Path:
    out_dir = (ROLLOUP_ROOT / day).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = (out_dir / "sleeve_rollup.v1.json").resolve()
    out_path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return out_path


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_c2_multi_sleeve_orchestrator_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--input_day_utc", default="", help="Optional input day key (defaults to day_utc)")
    ap.add_argument("--symbol", default="SPY", help="Default symbol (sleeves may override in future; v1 uses this)")
    args = ap.parse_args()

    day = require_day(args.day_utc)
    input_day = require_day((args.input_day_utc or "").strip() or day)
    symbol = str(args.symbol or "").strip().upper() or "SPY"

    # Load registry
    reg = load_json(REGISTRY_PATH)
    sleeves = require_registry(reg)

    produced_utc = utc_now_isoz()

    per_sleeve: List[Dict[str, Any]] = []
    any_abort = False
    any_fail = False
    any_degraded = False

    # Drive each enabled sleeve deterministically in listed order
    for s in sleeves:
        sleeve_id = str(s.get("sleeve_id") or "").strip() or "UNKNOWN"
        enabled = s.get("enabled")
        if enabled is False:
            per_sleeve.append({"sleeve_id": sleeve_id, "enabled": False, "status": "SKIP_DISABLED"})
            continue

        sleeve_id, mode, ib_account, truth_root = resolve_sleeve_truth_root(s)

        rc, cmd_str = run_orchestrator_v2(
            day=day,
            input_day=input_day,
            mode=mode,
            symbol=symbol,
            ib_account=ib_account,
            produced_utc=produced_utc,
            truth_root=truth_root,
        )

        # Orchestrator v2 policy: rc!=0 implies ABORTED (safety breach)
        # rc == 0 => orchestrator v2 completed and wrote verdict + pointer index in the sleeve truth root.
        verdict_root = (truth_root / "reports" / "orchestrator_run_verdict_v2").resolve()
        idx_path, points_to_path, pointer_seq = resolve_latest_verdict_pointer(
            verdict_root=verdict_root, day=day, mode=mode
        )

        verdict_obj = read_json_obj(points_to_path)
        v_status = str(verdict_obj.get("status") or "").strip().upper()
        v_reasons = verdict_obj.get("reason_codes") if isinstance(verdict_obj.get("reason_codes"), list) else []
        v_breaches = verdict_obj.get("safety_breaches") if isinstance(verdict_obj.get("safety_breaches"), list) else []

        # Fail-closed if status missing/unknown
        if v_status not in ("PASS", "DEGRADED", "FAIL", "ABORTED"):
            die(f"verdict_status_invalid sleeve_id={sleeve_id} status={v_status!r} points_to={points_to_path}")

        # Aggregate global status
        if v_status == "ABORTED":
            any_abort = True
        elif v_status == "FAIL":
            any_fail = True
        elif v_status == "DEGRADED":
            any_degraded = True

        per_sleeve.append(
            {
                "sleeve_id": sleeve_id,
                "enabled": True,
                "mode": mode,
                "ib_account": ib_account,
                "truth_root": str(truth_root),
                "orchestrator_rc": int(rc),
                "verdict_status": v_status,
                "verdict_reason_codes": [str(x) for x in v_reasons],
                "verdict_safety_breaches": [str(x) for x in v_breaches],
                "verdict_pointer_seq": int(pointer_seq),
                "verdict_pointer_index_path": str(idx_path),
                "verdict_points_to": str(points_to_path),
                "verdict_points_to_sha256": sha256_file(points_to_path),
                "cmd": cmd_str,
            }
        )

    # Global verdict (fail-closed conservative)
    if any_abort:
        global_status = "ABORTED"
    elif any_fail:
        global_status = "FAIL"
    elif any_degraded:
        global_status = "DEGRADED"
    else:
        global_status = "PASS"

    rollup = {
        "schema_id": "C2_SLEEVE_ROLLUP_V1",
        "day_utc": day,
        "input_day_utc": input_day,
        "produced_utc": produced_utc,
        "status": global_status,
        "registry_path": str(REGISTRY_PATH),
        "sleeves": per_sleeve,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_c2_multi_sleeve_orchestrator_v1.py"},
    }

    out_path = write_rollup(day, rollup)
    print(f"OK: wrote_rollup path={out_path}")

    # Append canonical pointer index for the rollup (day-scoped, append-only)
    day_dir = (ROLLOUP_ROOT / day).resolve()
    idx_path = (day_dir / POINTER_INDEX_NAME).resolve()
    lock_path = (day_dir / POINTER_LOCK_NAME).resolve()

    lock_fd = lock_acquire(lock_path)
    try:
        last_seq = read_last_pointer_seq(idx_path)
        pointer_seq = last_seq + 1
        entry = {
            "schema_id": "C2_SLEEVE_ROLLUP_POINTER_INDEX_V1",
            "pointer_seq": int(pointer_seq),
            "day_utc": day,
            "status": global_status,
            "produced_utc": produced_utc,
            "points_to": str(out_path),
            "points_to_sha256": sha256_file(out_path),
            "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_c2_multi_sleeve_orchestrator_v1.py"},
        }
        line_sha = atomic_append_jsonl(idx_path, entry)
    finally:
        lock_release(lock_fd, lock_path)

    print(f"OK: rollup_pointer_appended seq={pointer_seq} line_sha256={line_sha} idx={idx_path}")
    # Always exit 0 except on ABORTED (safety breach)
    if global_status == "ABORTED":
        return 9
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
