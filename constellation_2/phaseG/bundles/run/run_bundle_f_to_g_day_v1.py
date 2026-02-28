from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
DEFAULT_TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()


def _resolve_truth_root() -> Path:
    env = (str(__import__("os").environ.get("C2_TRUTH_ROOT", "")) or "").strip()
    if env:
        p = Path(env).resolve()
        if not p.exists() or not p.is_dir():
            raise SystemExit(f"FATAL: C2_TRUTH_ROOT invalid: {p}")
        return p
    return DEFAULT_TRUTH_ROOT


def _positions_effective_pointer_path(truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "positions_v1"
        / "effective_v1"
        / "days"
        / day_utc
        / "positions_effective_pointer.v1.json"
    ).resolve()


def _positions_effective_pointer_exists(truth_root: Path, day_utc: str) -> bool:
    p = _positions_effective_pointer_path(truth_root, day_utc)
    return p.exists() and p.is_file()


def _run(cmd: List[str]) -> int:
    p = subprocess.run(cmd)
    return int(p.returncode)


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="run_bundle_f_to_g_day_v1",
        description=(
            "C2 Orchestrator v1: cash -> positions -> effective -> defined_risk -> lifecycle -> accounting -> allocation "
            "(immutable, fail-closed, idempotent)"
        ),
    )
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--producer_git_sha", required=True, help="Producing git sha (explicit; passed to all runners)")
    ap.add_argument("--producer_repo", default="constellation_2_runtime", help="Producer repo id")
    ap.add_argument("--operator_statement_json", required=True, help="Operator statement JSON for cash ledger bootstrap")
    args = ap.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    producer_sha = str(args.producer_git_sha).strip()
    producer_repo = str(args.producer_repo).strip()
    operator_statement_json = str(args.operator_statement_json).strip()

    truth_root = _resolve_truth_root()

    steps: List[Tuple[List[str], str]] = []

    # 1) cash_ledger_v1
    steps.append(
        (
            [
                "python3",
                "-m",
                "constellation_2.phaseF.cash_ledger.run.run_cash_ledger_snapshot_day_v1",
                "--day_utc",
                day_utc,
                "--operator_statement_json",
                operator_statement_json,
                "--producer_git_sha",
                producer_sha,
                "--producer_repo",
                producer_repo,
            ],
            "cash_ledger_v1",
        )
    )

    # 2) positions_snapshot.v3 + positions_effective_pointer
    #
    # IMMUTABILITY / IDEMPOTENCY INVARIANT:
    # If positions_effective_pointer already exists, we MUST NOT create a new "better" snapshot (v3),
    # because the pointer would need to change => immutable rewrite attempt.
    #
    # Therefore:
    # - If pointer exists: SKIP both v3 snapshot and pointer writer.
    # - If pointer missing: produce v3 snapshot, then produce pointer using the schema-correct v2 writer.
    if _positions_effective_pointer_exists(truth_root, day_utc):
        print(f"OK: POSITIONS_EFFECTIVE_POINTER_EXISTS -> SKIP positions_v3 + positions_effective_v1 day_utc={day_utc}")
    else:
        steps.append(
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
            )
        )
        steps.append(
            (
                [
                    "python3",
                    "-m",
                    "constellation_2.phaseF.positions.run.run_positions_effective_pointer_day_v2",
                    "--day_utc",
                    day_utc,
                    "--producer_git_sha",
                    producer_sha,
                    "--producer_repo",
                    producer_repo,
                ],
                "positions_effective_v1",
            )
        )

    # 3) defined_risk_v1
    steps.append(
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
        )
    )

    # 4) position_lifecycle_v1
    steps.append(
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
        )
    )

    # 5) accounting_v1
    steps.append(
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
        )
    )

    # 6) allocation_v1
    steps.append(
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
        )
    )

    for cmd, name in steps:
        rc = _run(cmd)
        if rc != 0:
            print(f"FAIL: STEP_FAILED name={name} rc={rc}", file=sys.stderr)
            return rc

    print("OK: BUNDLE_F_TO_G_DAY_V1_COMPLETE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
