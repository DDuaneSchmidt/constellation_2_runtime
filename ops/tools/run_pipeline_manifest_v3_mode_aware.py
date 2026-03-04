#!/usr/bin/env python3
"""
run_pipeline_manifest_v3_mode_aware.py

Writes a mode-aware pipeline manifest derived from Orchestrator V2 attempt manifest.

Output (attempt-scoped, immutable-safe):
  constellation_2/runtime/truth/reports/pipeline_manifest_v3/<DAY>/attempts/<ATTEMPT_ID>/pipeline_manifest.v3.json

Pointer index (append-only, per-day):
  constellation_2/runtime/truth/reports/pipeline_manifest_v3/<DAY>/canonical_pointer_index.v1.jsonl

Determinism:
- produced_utc is day-scoped: <DAY>T00:00:00Z
- schema validation required (repo-local)
- no dependency on legacy v1/v2 pipeline manifests
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Tuple
from constellation_2.common.truth_root_v1 import resolve_truth_root
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = resolve_truth_root(repo_root=REPO_ROOT)

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/pipeline_manifest.v3.schema.json"
OUT_ROOT = (TRUTH_ROOT / "reports" / "pipeline_manifest_v3").resolve()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _json_bytes(obj: Any) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _require_day(day: str) -> str:
    d = (day or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise SystemExit(f"FAIL: bad --day_utc: {d!r}")
    return d


def _require_mode(mode: str) -> str:
    m = (mode or "").strip().upper()
    if m not in ("PAPER", "LIVE"):
        raise SystemExit(f"FAIL: bad --mode (PAPER|LIVE): {m!r}")
    return m


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    s = out.decode("utf-8").strip()
    if len(s) != 40:
        raise SystemExit(f"FAIL: bad git sha: {s!r}")
    return s


def _lock_acquire(lock_path: Path) -> int:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        raise SystemExit(f"FAIL: lock busy: {lock_path}")
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


def _read_last_pointer_seq(idx_path: Path, mode: str) -> int:
    if not idx_path.exists():
        return 0
    last = 0
    for line in idx_path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            raise SystemExit(f"FAIL: invalid JSONL: {idx_path}")
        if not isinstance(obj, dict):
            continue
        if str(obj.get("mode") or "").strip().upper() != mode:
            continue
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
    ap = argparse.ArgumentParser(prog="run_pipeline_manifest_v3_mode_aware")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--mode", required=True, choices=["PAPER", "LIVE"])
    ap.add_argument("--attempt_id", required=True)
    ap.add_argument("--attempt_seq", required=True, type=int)
    ap.add_argument("--attempt_manifest_path", required=True)
    args = ap.parse_args()

    day = _require_day(args.day_utc)
    mode = _require_mode(args.mode)
    attempt_id = str(args.attempt_id).strip()
    attempt_seq = int(args.attempt_seq)
    if not attempt_id:
        raise SystemExit("FAIL: empty attempt_id")
    if attempt_seq <= 0:
        raise SystemExit("FAIL: attempt_seq must be > 0")

    man_path = Path(str(args.attempt_manifest_path)).resolve()
    if not man_path.exists() or not man_path.is_file():
        raise SystemExit(f"FAIL: attempt_manifest_path missing: {man_path}")

    man_bytes = man_path.read_bytes()
    man_sha = _sha256_bytes(man_bytes)
    try:
        man_obj = json.loads(man_bytes.decode("utf-8"))
    except Exception as e:
        raise SystemExit(f"FAIL: attempt manifest invalid JSON: {e!r}") from e
    if not isinstance(man_obj, dict):
        raise SystemExit("FAIL: attempt manifest not object")

    stages_in = man_obj.get("stages")
    if not isinstance(stages_in, list):
        raise SystemExit("FAIL: attempt manifest missing stages list")

    produced_utc = f"{day}T00:00:00Z"

    blocking_failures = 0
    required_failures = 0
    nonblocking_degradations = 0

    out_stages: List[Dict[str, Any]] = []
    for s in stages_in:
        if not isinstance(s, dict):
            continue

        stage_id = str(s.get("stage_id") or "").strip()
        status = str(s.get("status") or "").strip().upper()
        executed = bool(s.get("executed"))
        rc = int(s.get("rc") or 0)

        outputs_present = s.get("outputs_present") or []
        if not isinstance(outputs_present, list):
            outputs_present = []

        cls = s.get("classification") or {}
        if not isinstance(cls, dict):
            cls = {}

        eff_req = bool(cls.get("effective_required"))
        eff_blk = bool(cls.get("effective_blocking"))

        reason_codes = s.get("reason_codes") or []
        if not isinstance(reason_codes, list):
            reason_codes = []

        if status == "FAIL" and eff_blk:
            blocking_failures += 1
        elif status == "FAIL" and eff_req:
            required_failures += 1
        elif status == "FAIL" and (not eff_req):
            nonblocking_degradations += 1

        out_stages.append(
            {
                "stage_id": stage_id,
                "status": status if status in ("OK", "SKIP", "FAIL") else "FAIL",
                "executed": bool(executed),
                "effective_required": bool(eff_req),
                "effective_blocking": bool(eff_blk),
                "rc": int(rc),
                "reason_codes": list(reason_codes),
                "outputs_present": [str(x) for x in outputs_present if isinstance(x, str)],
            }
        )

    act = man_obj.get("activity") or {}
    activity = bool(isinstance(act, dict) and act.get("activity"))

    reason_codes_top: List[str] = []
    if not activity:
        reason_codes_top.append("NO_ACTIVITY_DAY")

    if blocking_failures > 0:
        status_top = "ABORTED"
        reason_codes_top.append("BLOCKING_FAILURES_PRESENT")
    elif required_failures > 0:
        status_top = "FAIL"
        reason_codes_top.append("REQUIRED_FAILURES_PRESENT")
    else:
        status_top = "DEGRADED" if (nonblocking_degradations > 0 or not activity) else "PASS"
        if nonblocking_degradations > 0:
            reason_codes_top.append("NONBLOCKING_DEGRADATIONS_PRESENT")

    out_dir = (OUT_ROOT / day / "attempts" / attempt_id).resolve()
    out_path = (out_dir / "pipeline_manifest.v3.json").resolve()

    out = {
        "schema_id": "pipeline_manifest.v3",
        "day_utc": day,
        "mode": mode,
        "attempt_id": attempt_id,
        "attempt_seq": int(attempt_seq),
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_pipeline_manifest_v3_mode_aware.py", "git_sha": _git_sha()},
        "status": status_top,
        "summary": {
            "blocking_failures": int(blocking_failures),
            "required_failures": int(required_failures),
            "nonblocking_degradations": int(nonblocking_degradations),
            "stages_total": int(len(out_stages)),
        },
        "reason_codes": list(reason_codes_top),
        "inputs": [{"type": "orchestrator_attempt_manifest_v2", "path": str(man_path), "sha256": str(man_sha)}],
        "stages": out_stages,
        "artifacts": {"attempt_manifest_path": str(man_path), "path": str(out_path)},
    }

    validate_against_repo_schema_v1(out, REPO_ROOT, SCHEMA_RELPATH)

    try:
        write_file_immutable_v1(path=out_path, data=_json_bytes(out), create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL_IMMUTABLE_WRITE: {e}") from e

    idx_path = (OUT_ROOT / day / "canonical_pointer_index.v1.jsonl").resolve()
    lock_path = (OUT_ROOT / day / ".canonical_pointer_index.v1.lock").resolve()

    lock_fd = _lock_acquire(lock_path)
    try:
        last_seq = _read_last_pointer_seq(idx_path, mode)
        pointer_seq = last_seq + 1

        entry = {
            "schema_id": "C2_PIPELINE_MANIFEST_V3_POINTER_INDEX_V1",
            "pointer_seq": int(pointer_seq),
            "day_utc": day,
            "mode": mode,
            "attempt_id": attempt_id,
            "attempt_seq": int(attempt_seq),
            "status": status_top,
            "authoritative": bool(status_top == "PASS"),
            "produced_utc": produced_utc,
            "producer_git_sha": _git_sha(),
            "points_to": str(out_path),
        }
        line_sha, _ = _atomic_append_jsonl(idx_path, entry)
    finally:
        _lock_release(lock_fd, lock_path)

    print(
        json.dumps(
            {
                "ok": True,
                "day_utc": day,
                "mode": mode,
                "attempt_id": attempt_id,
                "status": status_top,
                "path": str(out_path),
                "pointer_index_path": str(idx_path),
                "append_line_sha256": line_sha,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
