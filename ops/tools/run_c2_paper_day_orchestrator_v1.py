#!/usr/bin/env python3
"""
run_c2_paper_day_orchestrator_v1.py

Constellation 2.0 — Bundle 2 (+ Paper Readiness Enhancements)
Institutional-grade PAPER day orchestrator.

USAGE (authoritative, proven safe):
  cd /home/node/constellation_2_runtime
  python3 ops/tools/run_c2_paper_day_orchestrator_v1.py --day_utc YYYY-MM-DD --mode PAPER --ib_account DUXXXXXXX

NON-NEGOTIABLE PROPERTIES:
- Deterministic stage order
- PAPER mode only
- No implicit deletion or mutation
- Structured audit logging
- Fail-closed for submission (PhaseD blocked on prereq failure)
- Fail-closed for paper readiness (broker evidence + linkage + attribution + monitor)
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()


def _require_repo_root_cwd() -> None:
    cwd = Path.cwd().resolve()
    if cwd != REPO_ROOT:
        raise SystemExit(f"FATAL: must run from repo root cwd={cwd} expected={REPO_ROOT}")


def _run_stage_strict(name: str, cmd: List[str]) -> None:
    print(f"STAGE_START {name}")
    p = subprocess.run(cmd, stdout=sys.stdout, stderr=sys.stderr, text=True)
    if p.returncode != 0:
        print(f"STAGE_FAIL {name} rc={p.returncode}", file=sys.stderr)
        raise SystemExit(p.returncode)
    print(f"STAGE_OK {name}")


def _run_stage_soft(name: str, cmd: List[str]) -> Tuple[bool, int]:
    print(f"STAGE_START {name}")
    p = subprocess.run(cmd, stdout=sys.stdout, stderr=sys.stderr, text=True)
    if p.returncode != 0:
        print(f"STAGE_FAIL {name} rc={p.returncode}", file=sys.stderr)
        return (False, int(p.returncode))
    print(f"STAGE_OK {name}")
    return (True, 0)


def main() -> int:
    _require_repo_root_cwd()

    ap = argparse.ArgumentParser(prog="run_c2_paper_day_orchestrator_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--input_day_utc", default="", help="Optional input day key")
    ap.add_argument("--mode", required=True, choices=["PAPER", "LIVE"])
    ap.add_argument("--symbol", default="SPY")
    ap.add_argument("--ib_account", required=True, help="IB PAPER account id (must be DU* per adapter policy)")
    args = ap.parse_args()

    day = args.day_utc.strip()
    input_day = (args.input_day_utc or "").strip() or day
    mode = args.mode.strip().upper()
    symbol = str(args.symbol).strip().upper()
    ib_account = str(args.ib_account).strip()

    if mode != "PAPER":
        print("FATAL: Orchestrator v1 supports PAPER mode only.", file=sys.stderr)
        return 2

    import datetime as _dt
    produced_utc = _dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    import subprocess as _sp
    current_git_sha = (
        _sp.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(Path.cwd()))
        .decode("utf-8")
        .strip()
    )

    # --- Bundle A2 (Fail-Closed) ---
    # Require broker-of-record reconciliation PASS for this day before any trading stages.
    _run_stage_strict(
        "A2_BROKER_RECONCILIATION_GATE_V1",
        [
            "python3",
            "ops/tools/run_broker_reconciliation_day_v1.py",
            "--day_utc",
            day,
        ],
    )


    prereq_failed = False

    # --- Stage 0 ---
    _run_stage_strict(
        "BUNDLEB_ENGINE_MODEL_REGISTRY_GATE",
        [
            "python3",
            "ops/tools/run_engine_model_registry_gate_v1.py",
            "--day_utc",
            day,
            "--current_git_sha",
            current_git_sha,
        ],
    )

    # --- Stage 1 Engines ---
    for stage_name, module in [
        ("ENGINE_MEAN_REVERSION", "constellation_2.phaseI.mean_reversion.run.run_mean_reversion_intents_day_v1"),
        ("ENGINE_TREND_EQ_PRIMARY", "constellation_2.phaseI.trend_eq_primary.run.run_trend_eq_primary_intents_day_v1"),
        ("ENGINE_VOL_INCOME_DEFINED", "constellation_2.phaseI.vol_income_defined_risk.run.run_vol_income_defined_risk_intents_day_v1"),
    ]:
        ok, _rc = _run_stage_soft(
            stage_name,
            ["python3", "-m", module, "--day_utc", day, "--mode", mode, "--symbol", symbol],
        )
        if not ok:
            prereq_failed = True

    # --- PhaseC ---
    ok, _rc = _run_stage_soft(
        "PHASEC_PREFLIGHT",
        [
            "python3",
            "-m",
            "constellation_2.phaseC.tools.run_phaseC_preflight_day_v1",
            "--day_utc",
            input_day,
            "--eval_time_utc",
            produced_utc,
        ],
    )
    if not ok:
        prereq_failed = True

    # --- PhaseH ---
    ok, _rc = _run_stage_soft(
        "PHASEH_OMS",
        [
            "python3",
            "-m",
            "constellation_2.phaseH.tools.run_oms_decisions_day_v1",
            "--day_utc",
            input_day,
            "--producer_git_sha",
            current_git_sha,
        ],
    )
    if not ok:
        prereq_failed = True

    # --- Bundle B.2 ---
    ok, _rc = _run_stage_soft(
        "BUNDLEB2_CAPITAL_RISK_ENVELOPE_GATE",
        [
            "python3",
            "ops/tools/run_c2_capital_risk_envelope_gate_v1.py",
            "--out_day_utc",
            day,
            "--input_day_utc",
            input_day,
            "--produced_utc",
            produced_utc,
        ],
    )
    if not ok:
        prereq_failed = True

    if prereq_failed:
        print("FATAL: prerequisite stage failure; submission blocked.", file=sys.stderr)
        return 2

    # --- PhaseD (v2) ---
    _run_stage_strict(
        "PHASED_PAPER_SUBMIT_V2",
        [
            "python3",
            "-m",
            "constellation_2.phaseD.tools.c2_submit_paper_v2",
            "--day_utc",
            day,
            "--ib_account",
            ib_account,
        ],
    )

    # --- A+2 Broker Day Manifest (strict) ---
    _run_stage_strict(
        "A2_BROKER_EVENT_DAY_MANIFEST",
        [
            "python3",
            "ops/ib/run_broker_event_day_manifest_v1.py",
            "--day_utc",
            day,
        ],
    )

    # --- A+2 Execution Event Linker (strict) ---
    _run_stage_strict(
        "A2_EXECUTION_EVENT_LINKER",
        [
            "python3",
            "ops/tools/run_execution_event_linker_v1.py",
            "--day_utc",
            day,
            "--environment",
            "PAPER",
        ],
    )

    # --- B+1 Positions Snapshot v5 (strict) ---
    _run_stage_strict(
        "B1_POSITIONS_SNAPSHOT_V5",
        [
            "python3",
            "-m",
            "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v5",
            "--day_utc",
            day,
            "--producer_git_sha",
            current_git_sha,
        ],
    )

    # --- Bundle F++: Position Lifecycle Snapshot (strict) ---
    _run_stage_strict(
        "BUNDLEF_LIFECYCLE_SNAPSHOT_V2",
        [
            "python3",
            "ops/tools/run_position_lifecycle_snapshot_v2.py",
            "--day_utc",
            day,
        ],
    )

    # --- Bundle F++: Exit Obligations (strict) ---
    _run_stage_strict(
        "BUNDLEF_EXIT_OBLIGATIONS_V1",
        [
            "python3",
            "ops/tools/run_exit_obligations_v1.py",
            "--day_utc",
            day,
        ],
    )

    # --- Bundle F++: Exposure Reconciliation (strict) ---
    _run_stage_strict(
        "BUNDLEF_EXPOSURE_RECONCILIATION_V2",
        [
            "python3",
            "ops/tools/run_exposure_reconciliation_v2.py",
            "--day_utc",
            day,
        ],
    )

    # --- Bundle F++: Lifecycle Monitor (strict) ---
    _run_stage_strict(
        "BUNDLEF_LIFECYCLE_MONITOR_V1",
        [
            "python3",
            "ops/tools/run_lifecycle_monitor_v1.py",
            "--day_utc",
            day,
        ],
    )

    # --- B+2 Positions Effective Pointer (strict; immutable) ---
    _run_stage_strict(
        "B2_POSITIONS_EFFECTIVE_POINTER_V1",
        [
            "python3",
            "-m",
            "constellation_2.phaseF.positions.run.run_positions_effective_pointer_day_v2",
            "--day_utc",
            day,
            "--producer_git_sha",
            current_git_sha,
        ],
    )

    # --- A+3 Paper Readiness Monitor (strict) ---
    _run_stage_strict(
        "A3_PAPER_READINESS_MONITOR_V2",
        [
            "python3",
            "ops/tools/run_paper_readiness_monitor_v2.py",
            "--day_utc",
            day,
        ],
    )

    # --- PhaseF ---
    _run_stage_strict(
        "PHASEF_EXEC_EVIDENCE",
        [
            "python3",
            "-m",
            "constellation_2.phaseF.execution_evidence.run.run_execution_evidence_truth_day_v1",
            "--day_utc",
            day,
        ],
    )

    _run_stage_strict(
        "PHASEF_SUBMISSION_INDEX",
        [
            "python3",
            "-m",
            "constellation_2.phaseF.execution_evidence.run.run_submission_index_day_v1",
            "--day_utc",
            day,
        ],
    )

    # --- PhaseG ---
    _run_stage_strict(
        "PHASEG_BUNDLE_F_TO_G",
        [
            "python3",
            "-m",
            "constellation_2.phaseG.bundles.run.run_bundle_f_to_g_day_v1",
            "--day_utc",
            day,
        ],
    )

    # --- Economic NAV + Drawdown Truth Spine (soft stages) ---
    for stage_name, cmd in [
        ("ECON_NAV_SNAPSHOT_V1", ["python3", "ops/tools/gen_nav_snapshot_v1.py", "--day_utc", day]),
        ("ECON_NAV_HISTORY_LEDGER_V1", ["python3", "ops/tools/gen_nav_history_ledger_v1.py", "--day_utc", day]),
        ("ECON_DRAWDOWN_WINDOW_PACK_V1", ["python3", "ops/tools/gen_drawdown_window_pack_v1.py", "--day_utc", day]),
        ("ECON_TRUTH_AVAIL_CERT_V1", ["python3", "ops/tools/gen_economic_truth_availability_certificate_v1.py", "--day_utc", day]),
        ("ECON_NAV_DRAWDOWN_BUNDLE_VALIDATE_V1", ["python3", "ops/tools/validate_economic_nav_drawdown_bundle_v1.py", "--day_utc", day]),
    ]:
        _run_stage_soft(stage_name, cmd)

    # --- PhaseJ ---
    _run_stage_strict(
        "PHASEJ_DAILY_SNAPSHOT",
        [
            "python3",
            "-m",
            "constellation_2.phaseJ.reporting.daily_snapshot_v1",
            "--day_utc",
            day,
        ],
    )

    # --- Bundle A ---
    _run_stage_strict(
        "BUNDLEA_PIPELINE_MANIFEST",
        [
            "python3",
            "ops/tools/run_pipeline_manifest_v1.py",
            "--day_utc",
            day,
        ],
    )

    print("ORCHESTRATOR_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
