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


def _stdout_json(result: dict[str, Any]) -> dict[str, Any]:
    raw = str(result.get("stdout") or "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _detect_session_day_blocker(*, day_utc: str, session_reentry_result: dict[str, Any] | None) -> str:
    if not isinstance(session_reentry_result, dict):
        return ""
    if str(session_reentry_result.get("status") or "").strip().upper() == "FAIL":
        return "SESSION_AUTHORITY_MISSING"
    payload = _stdout_json(session_reentry_result)
    if not payload:
        return ""
    target_day_admission = payload.get("target_day_admission") if isinstance(payload.get("target_day_admission"), dict) else {}
    active_session = payload.get("active_session") if isinstance(payload.get("active_session"), dict) else {}
    active_day = str(active_session.get("active_day") or "").strip()
    admission_status = str(
        target_day_admission.get("admission_status")
        or active_session.get("target_day_admission_status")
        or ""
    ).strip().upper()
    blocking_codes = (
        [str(item or "").strip().upper() for item in (target_day_admission.get("blocking_reason_codes") or [])]
        if isinstance(target_day_admission.get("blocking_reason_codes"), list)
        else []
    )
    if "NON_TRADING_DAY" in blocking_codes:
        return "NON_TRADING_DAY"
    if active_day and active_day != day_utc:
        return "NO_ACTIVE_PAPER_SESSION"
    if admission_status and admission_status != "ADMIT":
        return "NO_ACTIVE_PAPER_SESSION"
    return ""


def _skipped_step(name: str, cmd: list[str], blocker_code: str) -> dict[str, Any]:
    reason = str(blocker_code or "NO_ACTIVE_PAPER_SESSION").strip()
    return {
        "name": name,
        "cmd": cmd,
        "returncode": 2,
        "status": "DEGRADED",
        "stdout": "",
        "stderr": f"SKIPPED_NO_ACTIVE_PAPER_SESSION:{reason}",
    }


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
    open_phase_steps = [
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
    ]
    trading_only_steps = [
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

    diagnostic_steps: list[tuple[str, list[str]]] = []
    if args.with_diagnostics:
        diagnostic_steps = [
            ("runtime_state_snapshot", ["python3", "ops/tools/run_constellation_runtime_state_snapshot_v1.py"]),
            ("root_cause_classifier", ["python3", "ops/tools/run_constellation_root_cause_classifier_v1.py"]),
            ("repair_plan", ["python3", "ops/tools/run_constellation_repair_plan_v1.py"]),
        ]

    results: list[dict[str, Any]] = []
    degraded_steps: list[str] = []
    hard_failures: list[str] = []

    def _append_result(result: dict[str, Any]) -> None:
        results.append(result)
        if str(result["status"]) == "DEGRADED":
            degraded_steps.append(str(result["name"]))
        elif str(result["status"]) == "FAIL":
            hard_failures.append(str(result["name"]))

    session_reentry_result: dict[str, Any] | None = None
    if steps:
        name, cmd = steps[0]
        session_reentry_result = _run_step(name, cmd)
        _append_result(session_reentry_result)

    session_day_blocker = _detect_session_day_blocker(day_utc=day, session_reentry_result=session_reentry_result)

    for name, cmd in open_phase_steps:
        result = _run_step(name, cmd)
        _append_result(result)

    if session_day_blocker:
        for name, cmd in trading_only_steps:
            _append_result(_skipped_step(name, cmd, session_day_blocker))
    else:
        for name, cmd in trading_only_steps:
            result = _run_step(name, cmd)
            _append_result(result)

    for name, cmd in diagnostic_steps:
        result = _run_step(name, cmd)
        _append_result(result)

    status = "FAIL" if hard_failures else "DEGRADED" if degraded_steps else "OK"

    out = {
        "day_utc": day,
        "truth_root": truth_root,
        "with_diagnostics": bool(args.with_diagnostics),
        "session_authority_reentry_skipped": skip_session_authority_reentry,
        "session_day_blocker": session_day_blocker,
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
