from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import List, Tuple


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

EXEC_SUB_DAYS = (TRUTH / "execution_evidence_v1" / "submissions").resolve()

ORCH_MOD = "constellation_2.phaseG.bundles.run.run_bundle_f_to_g_day_v1"


def _git_head_sha() -> str:
    out = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT), text=True).strip()
    if not out:
        raise ValueError("GIT_HEAD_SHA_EMPTY")
    return out


def _newest_day_dir() -> str:
    if not EXEC_SUB_DAYS.exists() or not EXEC_SUB_DAYS.is_dir():
        raise FileNotFoundError(str(EXEC_SUB_DAYS))
    days = sorted([p.name for p in EXEC_SUB_DAYS.iterdir() if p.is_dir()])
    if not days:
        raise ValueError("NO_EXECUTION_EVIDENCE_DAYS_FOUND")
    return days[-1]


def _existing_day_outputs(day_utc: str) -> List[Tuple[str, Path]]:
    """
    Returns list of (label, path) that exist for day_utc.
    Any presence means re-running orchestrator is unsafe (immutability + sha locks).
    """
    checks: List[Tuple[str, Path]] = [
        ("cash_ledger_snapshot", TRUTH / "cash_ledger_v1" / "snapshots" / day_utc / "cash_ledger_snapshot.v1.json"),
        ("positions_snapshot_v2", TRUTH / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json"),
        ("positions_snapshot_v3", TRUTH / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v3.json"),
        ("positions_effective_pointer", TRUTH / "positions_v1" / "effective_v1" / "days" / day_utc / "positions_effective_pointer.v1.json"),
        ("defined_risk_snapshot", TRUTH / "defined_risk_v1" / "snapshots" / day_utc / "defined_risk_snapshot.v1.json"),
        ("lifecycle_snapshot", TRUTH / "position_lifecycle_v1" / "snapshots" / day_utc / "position_lifecycle_snapshot.v1.json"),
        ("accounting_nav", TRUTH / "accounting_v1" / "nav" / day_utc / "nav.json"),
        ("accounting_exposure", TRUTH / "accounting_v1" / "exposure" / day_utc / "exposure.json"),
        ("accounting_attr", TRUTH / "accounting_v1" / "attribution" / day_utc / "engine_attribution.json"),
        ("allocation_summary", TRUTH / "allocation_v1" / "summary" / day_utc / "summary.json"),
    ]
    present: List[Tuple[str, Path]] = []
    for label, p in checks:
        if p.exists() and p.is_file():
            present.append((label, p))
    return present


def main() -> int:
    try:
        day = _newest_day_dir()
    except Exception as e:
        print(f"FAIL: cannot determine newest DAY_UTC from {str(EXEC_SUB_DAYS)}: {e}", file=sys.stderr)
        return 2

    present = _existing_day_outputs(day)
    if present:
        print("BLOCKED: newest execution-evidence day already has truth outputs (immutability + sha locks make orchestrator rerun unsafe).", file=sys.stderr)
        print(f"DAY_UTC={day}", file=sys.stderr)
        print("EXISTING_OUTPUTS:", file=sys.stderr)
        for label, p in present:
            print(f"- {label}: {str(p)}", file=sys.stderr)
        print("", file=sys.stderr)
        print("NEXT ACTION:", file=sys.stderr)
        print("- Produce a new execution-evidence day (new submissions with a new UTC day key), then rerun this helper.", file=sys.stderr)
        return 2

    try:
        sha = _git_head_sha()
    except Exception as e:
        print(f"FAIL: cannot determine git HEAD sha: {e}", file=sys.stderr)
        return 2

    print("== NEXT ORCHESTRATOR RUN (SAFE) ==")
    print(f"DAY_UTC={day}")
    print(f"PRODUCER_SHA={sha}  (git HEAD)")
    print("")
    print("COMMAND:")
    print(f'python3 -m {ORCH_MOD} --day_utc "{day}" --producer_git_sha "{sha}"')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
