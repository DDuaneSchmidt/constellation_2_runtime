#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
GLOBAL_TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()


def _run_step(name: str, cmd: list[str]) -> dict[str, Any]:
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
    )
    return {
        "name": name,
        "cmd": cmd,
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _artifact_ok(rc: int) -> bool:
    # These governed producers return 0 on PASS and 2 when a FAIL artifact is
    # still emitted. Both are acceptable for refresh/diagnostics purposes.
    return rc in {0, 2}


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_c2_global_monitoring_refresh_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--with-diagnostics", action="store_true")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    truth_root = str(GLOBAL_TRUTH_ROOT)

    steps = [
        (
            "position_lifecycle_v2",
            [
                "python3",
                "ops/tools/run_position_lifecycle_snapshot_v2.py",
                "--day_utc",
                day,
                "--truth_root",
                truth_root,
            ],
        ),
        (
            "exit_obligations_v1",
            [
                "python3",
                "ops/tools/run_exit_obligations_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                truth_root,
            ],
        ),
        (
            "exposure_reconciliation_v2",
            [
                "python3",
                "ops/tools/run_exposure_reconciliation_v2.py",
                "--day_utc",
                day,
                "--truth_root",
                truth_root,
            ],
        ),
        (
            "lifecycle_monitor",
            [
                "python3",
                "ops/tools/run_lifecycle_monitor_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                truth_root,
            ],
        ),
        (
            "paper_readiness",
            [
                "python3",
                "ops/tools/run_paper_readiness_monitor_v2.py",
                "--day_utc",
                day,
                "--truth_root",
                truth_root,
            ],
        ),
        (
            "capital_authority_monitoring_attestation",
            [
                "python3",
                "ops/tools/run_capital_monitoring_attestation_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                truth_root,
                "--sleeve_id",
                "PRIMARY",
                "--mode",
                "PAPER",
            ],
        ),
    ]

    if args.with_diagnostics:
        steps.extend(
            [
                ("runtime_state_snapshot", ["python3", "ops/tools/run_constellation_runtime_state_snapshot_v1.py"]),
                ("root_cause_classifier", ["python3", "ops/tools/run_constellation_root_cause_classifier_v1.py"]),
                ("repair_plan", ["python3", "ops/tools/run_constellation_repair_plan_v1.py"]),
            ]
        )

    results: list[dict[str, Any]] = []
    hard_failures: list[str] = []
    for name, cmd in steps:
        result = _run_step(name, cmd)
        results.append(result)
        if not _artifact_ok(int(result["returncode"])):
            hard_failures.append(name)

    out = {
        "day_utc": day,
        "truth_root": truth_root,
        "with_diagnostics": bool(args.with_diagnostics),
        "results": results,
        "hard_failures": hard_failures,
    }
    print(json.dumps(out, indent=2, sort_keys=False))
    if hard_failures:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
