from __future__ import annotations

import argparse
import subprocess
import sys
from typing import List, Tuple


def _run(cmd: List[str]) -> int:
    p = subprocess.run(cmd)
    return int(p.returncode)


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_bundle_f_to_g_day_v1",
        description="C2 Orchestrator v1: cash -> positions -> effective -> defined_risk -> lifecycle -> accounting -> allocation (immutable, fail-closed).",
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--producer_git_sha", required=True, help="Producing git sha (explicit; passed to all runners)")
    ap.add_argument("--producer_repo", default="constellation_2_runtime", help="Producer repo id")
    args = ap.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    producer_sha = str(args.producer_git_sha).strip()
    producer_repo = str(args.producer_repo).strip()

    steps: List[Tuple[List[str], str]] = [
        (
            [
                "python3",
                "-m",
                "constellation_2.phaseF.cash_ledger.run.run_cash_ledger_snapshot_day_v1",
                "--day_utc",
                day_utc,
                "--producer_git_sha",
                producer_sha,
                "--producer_repo",
                producer_repo,
            ],
            "cash_ledger_v1",
        ),
        (
            [
                "python3",
                "-m",
                "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v3",
                "--day_utc",
                day_utc,
                "--producer_git_sha",
                producer_sha,
                "--producer_repo",
                producer_repo,
            ],
            "positions_v3",
        ),
        (
            [
                "python3",
                "-m",
                "constellation_2.phaseF.positions.run.run_positions_effective_pointer_day_v1",
                "--day_utc",
                day_utc,
                "--producer_git_sha",
                producer_sha,
                "--producer_repo",
                producer_repo,
            ],
            "positions_effective_v1",
        ),
        (
            [
                "python3",
                "-m",
                "constellation_2.phaseF.defined_risk.run.run_defined_risk_day_v1",
                "--day_utc",
                day_utc,
                "--producer_git_sha",
                producer_sha,
                "--producer_repo",
                producer_repo,
            ],
            "defined_risk_v1",
        ),
        (
            [
                "python3",
                "-m",
                "constellation_2.phaseF.position_lifecycle.run.run_position_lifecycle_day_v1",
                "--day_utc",
                day_utc,
                "--producer_git_sha",
                producer_sha,
                "--producer_repo",
                producer_repo,
            ],
            "position_lifecycle_v1",
        ),
        (
            [
                "python3",
                "-m",
                "constellation_2.phaseF.accounting.run.run_accounting_day_v1",
                "--day_utc",
                day_utc,
                "--producer_git_sha",
                producer_sha,
                "--producer_repo",
                producer_repo,
            ],
            "accounting_v1",
        ),
        (
            [
                "python3",
                "-m",
                "constellation_2.phaseG.allocation.run.run_allocation_day_v1",
                "--day_utc",
                day_utc,
                "--producer_git_sha",
                producer_sha,
                "--producer_repo",
                producer_repo,
            ],
            "allocation_v1",
        ),
    ]

    for cmd, name in steps:
        rc = _run(cmd)
        if rc != 0:
            print(f"FAIL: STEP_FAILED name={name} rc={rc}", file=sys.stderr)
            return rc

    print("OK: BUNDLE_F_TO_G_DAY_V1_COMPLETE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
