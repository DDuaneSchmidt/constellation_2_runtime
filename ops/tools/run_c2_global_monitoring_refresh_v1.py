#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.runtime_path_authority_v1 import (
    require_authoritative_repo_runtime_v1,
    resolve_decision_truth_root_v1,
)

SKIP_SESSION_AUTHORITY_REENTRY_ENV = "C2_SKIP_SESSION_AUTHORITY_REENTRY"


def _run_step(name: str, cmd: list[str]) -> dict[str, Any]:
    tool_path = (REPO_ROOT / cmd[1]).resolve() if len(cmd) > 1 and cmd[1].endswith(".py") else None
    if tool_path is not None and not tool_path.exists():
        return {
            "name": name,
            "cmd": cmd,
            "returncode": 2,
            "status": "DEGRADED",
            "stdout": "",
            "stderr": f"SKIPPED_OPTIONAL_TOOL_MISSING:{tool_path}",
        }
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
        "status": _status_for_rc(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _status_for_rc(rc: int) -> str:
    if rc == 0:
        return "OK"
    if rc == 2:
        return "DEGRADED"
    return "FAIL"


def main(argv: list[str] | None = None) -> int:
    require_authoritative_repo_runtime_v1(REPO_ROOT)
    ap = argparse.ArgumentParser(prog="run_c2_global_monitoring_refresh_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--with-diagnostics", action="store_true")
    args = ap.parse_args(argv)

    day = str(args.day_utc).strip()
    truth_root = str(resolve_decision_truth_root_v1(args.truth_root, repo_root=REPO_ROOT))
    skip_session_authority_reentry = str(os.environ.get(SKIP_SESSION_AUTHORITY_REENTRY_ENV) or "").strip().upper() == "YES"

    steps = []
    if not skip_session_authority_reentry:
        steps.append(
            (
                "session_authority_reentry",
                [
                    "python3",
                    "ops/tools/run_session_authority_v1.py",
                    "--phase",
                    "all",
                    "--truth_root",
                    truth_root,
                    "--environment",
                    "PAPER",
                    "--target_day",
                    day,
                ],
            )
        )
    steps.extend(
        [
            (
            "day_open_trigger",
            [
                "python3",
                "ops/tools/run_day_open_trigger_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                truth_root,
                "--environment",
                "PAPER",
            ],
        ),
        (
            "day_open_attempt",
            [
                "python3",
                "ops/tools/run_day_open_attempt_v1.py",
                "--day_utc",
                day,
                "--truth_root",
                truth_root,
                "--actor_name",
                "c2-global-monitoring-refresh.service",
                "--actor_path",
                "ops/tools/run_c2_global_monitoring_refresh_v1.py",
                "--environment",
                "PAPER",
            ],
        ),
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
    )

    if args.with_diagnostics:
        steps.extend(
            [
                ("runtime_state_snapshot", ["python3", "ops/tools/run_constellation_runtime_state_snapshot_v1.py"]),
                ("root_cause_classifier", ["python3", "ops/tools/run_constellation_root_cause_classifier_v1.py"]),
                ("repair_plan", ["python3", "ops/tools/run_constellation_repair_plan_v1.py"]),
            ]
        )

    results: list[dict[str, Any]] = []
    degraded_steps: list[str] = []
    hard_failures: list[str] = []
    for name, cmd in steps:
        result = _run_step(name, cmd)
        results.append(result)
        if str(result["status"]) == "DEGRADED":
            degraded_steps.append(name)
        elif str(result["status"]) == "FAIL":
            hard_failures.append(name)

    status = "FAIL" if hard_failures else "DEGRADED" if degraded_steps else "OK"

    out = {
        "day_utc": day,
        "truth_root": truth_root,
        "with_diagnostics": bool(args.with_diagnostics),
        "session_authority_reentry_skipped": skip_session_authority_reentry,
        "status": status,
        "results": results,
        "degraded_steps": degraded_steps,
        "hard_failures": hard_failures,
    }
    print(json.dumps(out, indent=2, sort_keys=False))
    if hard_failures or degraded_steps:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
