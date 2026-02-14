from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Optional, Tuple


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

EXEC_SUB_DAYS = (TRUTH / "execution_evidence_v1" / "submissions").resolve()

# Orchestrator module
ORCH_MOD = "constellation_2.phaseG.bundles.run.run_bundle_f_to_g_day_v1"


def _read_json_obj(p: Path) -> dict:
    with p.open("r", encoding="utf-8") as f:
        o = json.load(f)
    if not isinstance(o, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(p)}")
    return o


def _producer_sha_from_artifact(p: Path) -> Optional[str]:
    if not p.exists() or not p.is_file():
        return None
    o = _read_json_obj(p)
    prod = o.get("producer")
    if isinstance(prod, dict):
        sha = prod.get("git_sha")
        if isinstance(sha, str) and sha.strip():
            return sha.strip()
    return None


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


def _find_locked_sha_if_any(day_utc: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (sha, source_path) if any relevant artifact exists for day or global latest pointer exists.
    Prefers day-scoped artifacts; uses first found in deterministic order.
    """
    candidates = []

    # Day-scoped artifacts (prefer these)
    candidates.append(TRUTH / "cash_ledger_v1" / "snapshots" / day_utc / "cash_ledger_snapshot.v1.json")
    candidates.append(TRUTH / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v3.json")
    candidates.append(TRUTH / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json")
    candidates.append(TRUTH / "defined_risk_v1" / "snapshots" / day_utc / "defined_risk_snapshot.v1.json")
    candidates.append(TRUTH / "position_lifecycle_v1" / "snapshots" / day_utc / "position_lifecycle_snapshot.v1.json")
    candidates.append(TRUTH / "accounting_v1" / "nav" / day_utc / "nav.json")
    candidates.append(TRUTH / "allocation_v1" / "summary" / day_utc / "summary.json")

    # Global latest pointers (less ideal but still indicative of locking)
    candidates.append(TRUTH / "cash_ledger_v1" / "latest.json")
    candidates.append(TRUTH / "positions_v1" / "latest_v3.json")
    candidates.append(TRUTH / "defined_risk_v1" / "latest.json")
    candidates.append(TRUTH / "position_lifecycle_v1" / "latest.json")
    candidates.append(TRUTH / "accounting_v1" / "latest.json")
    candidates.append(TRUTH / "allocation_v1" / "latest.json")
    candidates.append(TRUTH / "execution_evidence_v1" / "latest.json")

    for p in candidates:
        sha = _producer_sha_from_artifact(p)
        if sha:
            return (sha, str(p))
    return (None, None)


def main() -> int:
    try:
        day = _newest_day_dir()
    except Exception as e:
        print(f"FAIL: cannot determine newest DAY_UTC from {str(EXEC_SUB_DAYS)}: {e}", file=sys.stderr)
        return 2

    try:
        locked_sha, locked_src = _find_locked_sha_if_any(day)
    except Exception as e:
        print(f"FAIL: cannot inspect existing artifacts for producer sha: {e}", file=sys.stderr)
        return 2

    try:
        sha = locked_sha if locked_sha else _git_head_sha()
    except Exception as e:
        print(f"FAIL: cannot determine producer git sha: {e}", file=sys.stderr)
        return 2

    print("== NEXT ORCHESTRATOR RUN ==")
    print(f"DAY_UTC={day}")
    if locked_sha:
        print(f"PRODUCER_SHA={sha}  (LOCKED from {locked_src})")
        print("NOTE: day appears to have existing outputs; orchestrator may attempt rewrites if rerun.")
    else:
        print(f"PRODUCER_SHA={sha}  (git HEAD)")

    print("")
    print("COMMAND:")
    print(f'python3 -m {ORCH_MOD} --day_utc "{day}" --producer_git_sha "{sha}"')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
