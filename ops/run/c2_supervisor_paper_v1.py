#!/usr/bin/env python3
"""
C2 Paper Supervisor v1 (always-on, deterministic, fail-closed).

Goal:
- Keep C2 always running without daily manual ops.
- Maintain truth spines derived from existing evidence/snapshots.
- Avoid churn: only run exec_evidence day writer when submissions dir changes.

This supervisor does NOT submit trades.
"""

from __future__ import annotations

import hashlib
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
VENV_PY = (REPO_ROOT / ".venv_c2/bin/python").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

# Local state (NOT truth, not git-tracked)
STATE_ROOT = (Path.home() / ".local/state/constellation_2").resolve()

POLL_SECONDS = 30  # polling interval


def _utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    print(f"{ts} {msg}", flush=True)


def _exists(path: Path) -> bool:
    try:
        return path.exists()
    except Exception:
        return False


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _dir_fingerprint_sha256(root: Path) -> str:
    """
    Deterministic fingerprint for an immutable submissions day directory.
    We hash: relative path + sha256(file) for every file, sorted by path.
    """
    if not root.exists() or not root.is_dir():
        return ""
    items: List[Tuple[str, str]] = []
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        rel = str(p.relative_to(root)).replace("\\", "/")
        items.append((rel, _sha256_file(p)))
    items.sort(key=lambda x: x[0])
    h = hashlib.sha256()
    for rel, fsha in items:
        h.update(rel.encode("utf-8"))
        h.update(b"\n")
        h.update(fsha.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def _read_text(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8").strip()
    except Exception:
        return ""


def _write_text_atomic(p: Path, s: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(s + "\n", encoding="utf-8")
    tmp.replace(p)


def _run(args: List[str]) -> int:
    p = subprocess.run(args, cwd=str(REPO_ROOT))
    return int(p.returncode)


def main() -> int:
    if not VENV_PY.exists():
        _log(f"FATAL: missing venv python: {VENV_PY}")
        return 2
    if not (REPO_ROOT / "constellation_2").is_dir():
        _log(f"FATAL: missing constellation_2 package dir under {REPO_ROOT}")
        return 2

    STATE_ROOT.mkdir(parents=True, exist_ok=True)

    _log("C2_SUPERVISOR_PAPER_V1_START")
    last_day = None

    while True:
        day_utc = _utc_day()
        producer_sha = _git_sha()

        if last_day != day_utc:
            _log(f"DAY_ROLLOVER day_utc={day_utc}")
            last_day = day_utc

        # --- Exec evidence: run only when submissions dir exists AND fingerprint changes ---
        submissions_dir = TRUTH / "execution_evidence_v1/submissions" / day_utc
        fp_path = STATE_ROOT / f"exec_evidence_fp_{day_utc}.sha256"

        if _exists(submissions_dir):
            fp = _dir_fingerprint_sha256(submissions_dir)
            prev = _read_text(fp_path)
            if fp and fp != prev:
                rc = _run(
                    [
                        str(VENV_PY),
                        "-m",
                        "constellation_2.phaseF.execution_evidence.run.run_execution_evidence_truth_day_v1",
                        "--day_utc",
                        day_utc,
                        "--producer_git_sha",
                        producer_sha,
                        "--producer_repo",
                        "constellation_2_runtime",
                    ]
                )
                if rc != 0:
                    _log(f"FAIL: exec_evidence rc={rc}")
                    return 1
                _write_text_atomic(fp_path, fp)
                _log("OK: exec_evidence (updated)")
            else:
                _log("SKIP: exec_evidence (no change)")
        else:
            _log("SKIP: exec_evidence (no submissions dir)")

        # --- Positions effective pointer: run only if snapshot exists; never rewrite ---
        eff_ptr = TRUTH / "positions_v1/effective_v1/days" / day_utc / "positions_effective_pointer.v1.json"
        eff_fail = TRUTH / "positions_v1/effective_v1/failures" / day_utc / "failure.json"

        snap_root = TRUTH / "positions_v1/snapshots" / day_utc
        has_snapshot = _exists(snap_root) and any(snap_root.glob("positions_snapshot.v*.json"))

        if _exists(eff_ptr) or _exists(eff_fail):
            _log("SKIP: positions_effective (already has pointer or failure)")
        elif not has_snapshot:
            _log("SKIP: positions_effective (no positions snapshot yet)")
        else:
            rc = _run(
                [
                    str(VENV_PY),
                    "-m",
                    "constellation_2.phaseF.positions.run.run_positions_effective_pointer_day_v1",
                    "--day_utc",
                    day_utc,
                    "--producer_git_sha",
                    producer_sha,
                    "--producer_repo",
                    "constellation_2_runtime",
                ]
            )
            if rc != 0:
                _log(f"FAIL: positions_effective rc={rc}")
                return 1
            _log("OK: positions_effective")

        # --- Position lifecycle: run only if prerequisites exist; never rewrite ---
        life_snap = TRUTH / "position_lifecycle_v1/snapshots" / day_utc / "position_lifecycle_snapshot.v1.json"
        life_fail = TRUTH / "position_lifecycle_v1/failures" / day_utc / "failure.json"

        if _exists(life_snap) or _exists(life_fail):
            _log("SKIP: lifecycle (already has snapshot or failure)")
        elif not _exists(eff_ptr):
            _log("SKIP: lifecycle (missing positions effective pointer)")
        elif not _exists(submissions_dir):
            _log("SKIP: lifecycle (missing execution evidence submissions dir)")
        else:
            rc = _run(
                [
                    str(VENV_PY),
                    "-m",
                    "constellation_2.phaseF.position_lifecycle.run.run_position_lifecycle_day_v1",
                    "--day_utc",
                    day_utc,
                    "--producer_git_sha",
                    producer_sha,
                    "--producer_repo",
                    "constellation_2_runtime",
                ]
            )
            if rc != 0:
                _log(f"FAIL: lifecycle rc={rc}")
                return 1
            _log("OK: lifecycle")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
