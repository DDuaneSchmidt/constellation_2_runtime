#!/usr/bin/env python3
"""
resolve_paper_day_utc_v1.py

Deterministic PAPER day-key resolver for systemd.

Goal:
- Select a DAY_UTC such that the PAPER day orchestrator can run without violating immutability,
  and without being forced onto a previously-poisoned day.

Policy (fail-closed):
- Search forward window: today_utc .. today_utc + 10 days (UTC).
- For each candidate day:
  1) If any "rerun-unsafe" outputs already exist for that day, skip it.
  2) Ensure Engine Model Registry Gate v1 for that day is PASS.
     - If artifact exists and is PASS -> ok.
     - If artifact exists and is not PASS -> skip (poisoned).
     - If artifact missing -> run the gate strictly; require PASS.

Outputs:
- Prints shell-safe lines:
    DAY_UTC=YYYY-MM-DD
    PRODUCED_UTC=YYYY-MM-DDT00:00:00Z
    PRODUCER_GIT_SHA=<git sha>
- Exit 0 on success, else exit 2.
"""

from __future__ import annotations

import datetime as _dt
import json
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

ENGINE_GATE_ROOT = (TRUTH / "reports" / "engine_model_registry_gate_v1").resolve()
ENGINE_GATE_TOOL = (REPO_ROOT / "ops" / "tools" / "run_engine_model_registry_gate_v1.py").resolve()


def _today_utc_day() -> str:
    return _dt.datetime.now(_dt.timezone.utc).date().isoformat()


def _git_head_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT), text=True).strip()
    if not out:
        raise SystemExit("FATAL: GIT_HEAD_SHA_EMPTY")
    return out


def _run_engine_gate(day_utc: str, current_sha: str) -> None:
    # Run the gate deterministically; it writes immutable output.
    p = subprocess.run(
        ["python3", str(ENGINE_GATE_TOOL), "--day_utc", day_utc, "--current_git_sha", current_sha],
        cwd=str(REPO_ROOT),
        stdout=sys.stdout,
        stderr=sys.stderr,
        text=True,
    )
    if p.returncode != 0:
        raise SystemExit(p.returncode)


def _read_json(path: Path) -> dict:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise SystemExit(f"FAIL: cannot read json: {path}: {e!r}") from e
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: json not object: {path}")
    return obj


def _rerun_unsafe_outputs(day_utc: str) -> List[Tuple[str, Path]]:
    """
    If any of these exist, we treat the day as unsafe for a fresh paper-day orchestrator run.
    This mirrors the immutability guard approach used elsewhere in ops tooling.

    We intentionally include the engine registry gate artifact itself: if it exists and is FAIL,
    that day is poisoned for paper-day runs.
    """
    checks: List[Tuple[str, Path]] = [
        ("positions_snapshot_v2", TRUTH / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json"),
        ("cash_ledger_snapshot_v1", TRUTH / "cash_ledger_v1" / "snapshots" / day_utc / "cash_ledger_snapshot.v1.json"),
        ("engine_linkage_v1", TRUTH / "engine_linkage_v1" / "snapshots" / day_utc / "engine_linkage.v1.json"),
        ("broker_baseline_snapshot_v1", TRUTH / "execution_evidence_v1" / "broker_baseline_snapshot_v1" / day_utc / "broker_baseline_snapshot.v1.json"),
        ("engine_model_registry_gate_v1", ENGINE_GATE_ROOT / day_utc / "engine_model_registry_gate.v1.json"),
    ]
    present: List[Tuple[str, Path]] = []
    for label, p in checks:
        if p.exists() and p.is_file():
            present.append((label, p))
    return present


def _engine_gate_passes_or_can_pass(day_utc: str, current_sha: str) -> bool:
    """
    Returns True iff the gate artifact exists and is PASS, or if it can be produced and is PASS.
    Never overwrites immutable files.
    """
    gate_path = (ENGINE_GATE_ROOT / day_utc / "engine_model_registry_gate.v1.json").resolve()
    if gate_path.exists():
        obj = _read_json(gate_path)
        st = str(obj.get("status") or "").strip().upper()
        return st == "PASS"

    # If missing, run it. This will write immutably for that day.
    _run_engine_gate(day_utc, current_sha)
    if not gate_path.exists():
        return False
    obj2 = _read_json(gate_path)
    st2 = str(obj2.get("status") or "").strip().upper()
    return st2 == "PASS"


def main() -> int:
    if not REPO_ROOT.exists() or not REPO_ROOT.is_dir():
        print(f"FAIL: repo root missing: {REPO_ROOT}", file=sys.stderr)
        return 2
    if not TRUTH.exists() or not TRUTH.is_dir():
        print(f"FAIL: truth root missing: {TRUTH}", file=sys.stderr)
        return 2
    if not ENGINE_GATE_TOOL.exists():
        print(f"FAIL: missing engine gate tool: {ENGINE_GATE_TOOL}", file=sys.stderr)
        return 2

    today = _today_utc_day()
    sha = _git_head_sha()

    # forward-only window (bounded)
    base = _dt.date.fromisoformat(today)
    for i in range(0, 11):
        d = (base + _dt.timedelta(days=i)).isoformat()

        present = _rerun_unsafe_outputs(d)

        # If only the gate artifact exists, we still treat it as unsafe unless it's PASS.
        if present:
            # Allow a special case: only engine_model_registry_gate_v1 exists AND it is PASS.
            if len(present) == 1 and present[0][0] == "engine_model_registry_gate_v1":
                if _engine_gate_passes_or_can_pass(d, sha):
                    print(f"DAY_UTC={d}")
                    print(f"PRODUCED_UTC={d}T00:00:00Z")
                    print(f"PRODUCER_GIT_SHA={sha}")
                    return 0
            continue

        # No outputs exist -> require gate PASS (may create the gate artifact)
        if not _engine_gate_passes_or_can_pass(d, sha):
            continue

        print(f"DAY_UTC={d}")
        print(f"PRODUCED_UTC={d}T00:00:00Z")
        print(f"PRODUCER_GIT_SHA={sha}")
        return 0

    print("FAIL: no safe DAY_UTC found in forward window (today..today+10d).", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
