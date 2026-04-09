#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2].resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import (
    read_paper_session_ledger_ref_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.runtime_contract_v1 import resolve_release_provenance
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_current_system_projection_path,
    resolve_deployment_state_machine_path,
    resolve_operator_statement_path,
    resolve_execution_journal_path,
    resolve_paper_session_ledger_path,
    resolve_startup_proof_validation_path,
    resolve_trading_day_state_machine_path,
)
from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry


STARTUP_TOOL = (REPO_ROOT / "ops/tools/run_startup_materialization_v1.py").resolve()
POSTURE_TOOL = (REPO_ROOT / "ops/tools/run_paper_trading_posture_v1.py").resolve()
BOUNDARY_TOOL = (REPO_ROOT / "ops/tools/run_submit_boundary_status_v1.py").resolve()
LEDGER_TOOL = (REPO_ROOT / "ops/tools/run_paper_session_ledger_v1.py").resolve()
OPERATOR_STATEMENT_TOOL = (REPO_ROOT / "ops/tools/ensure_cash_ledger_operator_statement_v1.py").resolve()
POSITIONS_TOOL = "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v2"
CASH_LEDGER_TOOL = "constellation_2.phaseF.cash_ledger.run.run_cash_ledger_snapshot_day_v1"
ACCOUNTING_NAV_TOOL = (REPO_ROOT / "ops/tools/run_accounting_nav_v2_day_v1.py").resolve()
REGIME_SNAPSHOT_TOOL = (REPO_ROOT / "ops/tools/run_regime_snapshot_v2.py").resolve()
DEFENSIVE_TAIL_INPUTS_BRIDGE_TOOL = (
    REPO_ROOT / "constellation_2/phaseJ/tools/build_defensive_tail_required_inputs_day_v1.py"
).resolve()
ENGINE_CORRELATION_MATRIX_TOOL = (
    REPO_ROOT / "constellation_2/phaseJ/monitoring/run/run_engine_correlation_matrix_day_v1.py"
).resolve()
STARTUP_PROOF_TOOL = (REPO_ROOT / "ops/tools/run_startup_proof_validation_v1.py").resolve()
DEPLOYMENT_TOOL = (REPO_ROOT / "ops/tools/run_deployment_state_machine_v1.py").resolve()
TRADING_DAY_STATE_MACHINE_TOOL = (REPO_ROOT / "ops/tools/run_trading_day_state_machine_v1.py").resolve()
EXECUTION_JOURNAL_TOOL = (REPO_ROOT / "ops/tools/run_execution_journal_v1.py").resolve()
CURRENT_SYSTEM_PROJECTION_TOOL = (REPO_ROOT / "ops/tools/run_current_system_projection_v1.py").resolve()
EXECUTION_TOOL = (REPO_ROOT / "ops/tools/run_c2_multi_sleeve_orchestrator_v1.py").resolve()


def _print_payload(payload: dict[str, object]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def _run(cmd: List[str], *, truth_root: Path) -> Dict[str, object]:
    env = dict(os.environ)
    env["C2_AUTHORITY_MODE"] = "governance_primary"
    env["C2_TRUTH_ROOT"] = str(truth_root)
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False, env=env)
    return {
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _producer_git_sha() -> str:
    try:
        return str(resolve_release_provenance().get("git_sha") or "").strip() or ("0" * 40)
    except Exception:
        return "0" * 40


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_paper_session_admission_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--input_day_utc", default="")
    ap.add_argument("--mode", default="PAPER", choices=["PAPER"])
    ap.add_argument("--symbol", default="SPY")
    ap.add_argument("--execute", default="NO", choices=["YES", "NO"])
    ap.add_argument("--truth_root", default=str((REPO_ROOT / "constellation_2/runtime/truth").resolve()))
    args = ap.parse_args()

    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    day_utc = str(args.day_utc).strip()
    input_day_utc = str((args.input_day_utc or "").strip() or day_utc)
    producer_git_sha = _producer_git_sha()
    producer_repo = REPO_ROOT.name
    ib_account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    operator_statement_path = resolve_operator_statement_path(operator_input_root=truth_root, day_utc=day_utc)

    runs = {
        "startup_materialization": _run(
            [sys.executable, str(STARTUP_TOOL), "--day_utc", day_utc, "--truth_root", str(truth_root)],
            truth_root=truth_root,
        ),
        "paper_trading_posture": _run(
            [sys.executable, str(POSTURE_TOOL), "--day_utc", day_utc, "--truth_root", str(truth_root)],
            truth_root=truth_root,
        ),
        "submit_boundary_status": _run(
            [sys.executable, str(BOUNDARY_TOOL), "--day_utc", day_utc, "--truth_root", str(truth_root)],
            truth_root=truth_root,
        ),
        "paper_session_ledger": _run(
            [sys.executable, str(LEDGER_TOOL), "--day_utc", day_utc, "--truth_root", str(truth_root)],
            truth_root=truth_root,
        ),
        "ensure_cash_ledger_operator_statement": _run(
            [
                sys.executable,
                str(OPERATOR_STATEMENT_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--ib_account",
                ib_account,
                "--mode",
                "SEED_100K",
                "--allow_create",
                "YES",
            ],
            truth_root=truth_root,
        ),
        "positions_snapshot_v2": _run(
            [
                sys.executable,
                "-m",
                POSITIONS_TOOL,
                "--day_utc",
                day_utc,
                "--producer_git_sha",
                producer_git_sha,
                "--producer_repo",
                producer_repo,
            ],
            truth_root=truth_root,
        ),
        "cash_ledger_snapshot_v1": _run(
            [
                sys.executable,
                "-m",
                CASH_LEDGER_TOOL,
                "--day_utc",
                day_utc,
                "--operator_statement_json",
                str(operator_statement_path),
                "--producer_repo",
                producer_repo,
                "--producer_git_sha",
                producer_git_sha,
            ],
            truth_root=truth_root,
        ),
        "accounting_nav_v2": _run(
            [
                sys.executable,
                str(ACCOUNTING_NAV_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--producer_repo",
                producer_repo,
                "--producer_git_sha",
                producer_git_sha,
            ],
            truth_root=truth_root,
        ),
        "regime_snapshot_v2": _run(
            [sys.executable, str(REGIME_SNAPSHOT_TOOL), "--day_utc", day_utc, "--truth_root", str(truth_root)],
            truth_root=truth_root,
        ),
        "defensive_tail_required_inputs_bridge_v1": _run(
            [
                sys.executable,
                str(DEFENSIVE_TAIL_INPUTS_BRIDGE_TOOL),
                "--day_utc",
                day_utc,
                "--symbol",
                str(args.symbol),
            ],
            truth_root=truth_root,
        ),
        "engine_correlation_matrix_v1": _run(
            [sys.executable, str(ENGINE_CORRELATION_MATRIX_TOOL), "--day_utc", day_utc, "--truth_root", str(truth_root)],
            truth_root=truth_root,
        ),
    }

    ledger_path = resolve_paper_session_ledger_path(truth_root=truth_root, day_utc=day_utc)
    try:
        ledger_ref = read_paper_session_ledger_ref_v1(truth_root=truth_root, day_utc=day_utc)
    except Exception as exc:
        _print_payload(
            {
                "status": "LEDGER_UNAVAILABLE",
                "day_utc": day_utc,
                "ledger_path": str(ledger_path),
                "runs": runs,
                "error": f"{type(exc).__name__}:{exc}",
            }
        )
        return 2

    ledger = ledger_ref.payload
    control_state = ledger.get("control_state") if isinstance(ledger.get("control_state"), dict) else {}
    authority_status = str(control_state.get("authority_status") or "").strip().upper()
    control_plane_runs = {
        "startup_proof_validation": _run(
            [sys.executable, str(STARTUP_PROOF_TOOL), "--day_utc", day_utc, "--truth_root", str(truth_root)],
            truth_root=truth_root,
        ),
        "deployment_state_machine": _run(
            [sys.executable, str(DEPLOYMENT_TOOL), "--day_utc", day_utc, "--truth_root", str(truth_root)],
            truth_root=truth_root,
        ),
        "trading_day_state_machine": _run(
            [sys.executable, str(TRADING_DAY_STATE_MACHINE_TOOL), "--day_utc", day_utc, "--truth_root", str(truth_root)],
            truth_root=truth_root,
        ),
        "execution_journal": _run(
            [sys.executable, str(EXECUTION_JOURNAL_TOOL), "--day_utc", day_utc, "--truth_root", str(truth_root)],
            truth_root=truth_root,
        ),
        "current_system_projection": _run(
            [sys.executable, str(CURRENT_SYSTEM_PROJECTION_TOOL), "--day_utc", day_utc, "--truth_root", str(truth_root)],
            truth_root=truth_root,
        ),
    }
    runs.update(control_plane_runs)
    control_plane_artifacts = {
        "startup_proof_validation_v1": str(resolve_startup_proof_validation_path(truth_root=truth_root, day_utc=day_utc)),
        "deployment_state_machine_v1": str(resolve_deployment_state_machine_path(truth_root=truth_root, day_utc=day_utc)),
        "trading_day_state_machine_v1": str(resolve_trading_day_state_machine_path(truth_root=truth_root, day_utc=day_utc)),
        "execution_journal_v1": str(resolve_execution_journal_path(truth_root=truth_root, day_utc=day_utc)),
        "current_system_projection_v1": str(
            resolve_current_system_projection_path(truth_root=truth_root, day_utc=day_utc)
        ),
    }
    missing_control_plane_artifacts = [
        logical_name
        for logical_name, path_text in control_plane_artifacts.items()
        if not Path(path_text).exists()
    ]
    summary = {
        "day_utc": day_utc,
        "input_day_utc": input_day_utc,
        "ledger_path": str(ledger_path),
        "ledger_id": str(ledger.get("ledger_id") or "").strip(),
        "authority_status": authority_status,
        "system_ready": bool(control_state.get("system_ready") is True),
        "submission_authorized": bool(control_state.get("submission_authorized") is True),
        "current_state": str(control_state.get("current_state") or "").strip(),
        "runs": runs,
        "control_plane_artifacts": control_plane_artifacts,
        "missing_control_plane_artifacts": missing_control_plane_artifacts,
    }
    if missing_control_plane_artifacts:
        summary["status"] = "CONTROL_PLANE_INCOMPLETE"
        summary["next_action"] = "inspect_missing_current_day_control_plane_artifacts"
        _print_payload(summary)
        return 4
    if authority_status != "GRANTED":
        summary["status"] = "NOT_AUTHORIZED"
        summary["next_action"] = "resolve_kernel_blocking_codes_and_rerun"
        _print_payload(summary)
        return 2

    summary["status"] = "AUTHORIZED"
    if str(args.execute) == "YES":
        cmd = [
            sys.executable,
            str(EXECUTION_TOOL),
            "--day_utc",
            day_utc,
            "--input_day_utc",
            input_day_utc,
            "--symbol",
            str(args.symbol),
            "--paper_session_ledger_path",
            str(ledger_path),
        ]
        summary["execution_command"] = " ".join(cmd)
        _print_payload(summary)
        return int(subprocess.call(cmd, cwd=str(REPO_ROOT)))

    summary["next_action"] = "execution_permitted_via_kernel_gated_entrypoint"
    _print_payload(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
