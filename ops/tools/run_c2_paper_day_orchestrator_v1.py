"""
run_c2_paper_day_orchestrator_v1.py

Constellation 2.0 — Bundle 2
Institutional-grade PAPER day orchestrator.

USAGE (authoritative, proven safe):
  cd /home/node/constellation_2_runtime
  python3 ops/tools/run_c2_paper_day_orchestrator_v1.py --day_utc YYYY-MM-DD --mode PAPER

Rationale:
- ops/ is not a Python package in this repo (no __init__.py; repo root not on sys.path).
- Therefore, python -m ops.tools... is forbidden and will fail.

NON-NEGOTIABLE PROPERTIES:
- Deterministic stage order
- PAPER mode only (hard guard)
- No implicit deletion or mutation
- Structured audit logging (STAGE_START / STAGE_OK / STAGE_FAIL)
- Refuse execution if not launched from repo root (audit reproducibility)
- Fail-closed for submission: never reach PhaseD if any prerequisite stage failed

Important refinement (audit-grade):
- Engine stages may fail (e.g., missing market data). We still write required gate/report artifacts.
- Overall run still returns non-zero if any stage fails.
- PhaseD submission is blocked unless all prerequisites succeed.

Stages:
0. Bundle B Engine Model Registry Gate (fail-closed)
1. Engines (MR, Trend, Vol Income) (continue-on-failure, but recorded)
2. PhaseC submit preflight
3. PhaseH OMS decisions
3.5 Bundle B.2 Capital Risk Envelope Gate (pre-PhaseD)
4. PhaseD paper submission (only if all prior stages succeeded AND B.2 PASS)
5. PhaseF execution evidence truth
6. PhaseF submission index
7. PhaseG bundle F→G (accounting + allocation)
8. PhaseJ daily snapshot
9. Bundle A pipeline manifest (final completeness gate)

This orchestrator performs no network operations itself.
It delegates to existing modules via subprocess with strict return-code checks.
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
    """
    Strict stage runner: fail-closed immediately on non-zero return.
    """
    print(f"STAGE_START {name}")
    p = subprocess.run(cmd, stdout=sys.stdout, stderr=sys.stderr, text=True)
    if p.returncode != 0:
        print(f"STAGE_FAIL {name} rc={p.returncode}", file=sys.stderr)
        raise SystemExit(p.returncode)
    print(f"STAGE_OK {name}")


def _run_stage_soft(name: str, cmd: List[str]) -> Tuple[bool, int]:
    """
    Soft stage runner: records failure but does not abort.
    Returns (ok, rc).
    """
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
    ap.add_argument(
        "--input_day_utc",
        default="",
        help="Optional: day key to read input truth from (default: same as --day_utc)",
    )
    ap.add_argument("--mode", required=True, choices=["PAPER", "LIVE"])
    ap.add_argument("--symbol", default="SPY", help="Underlying symbol (default: SPY)")

    args = ap.parse_args()

    day = args.day_utc.strip()
    input_day = (args.input_day_utc or "").strip() or day
    mode = args.mode.strip().upper()
    symbol = str(args.symbol).strip().upper()

    # PAPER-only guard
    if mode != "PAPER":
        print("FATAL: Orchestrator v1 supports PAPER mode only.", file=sys.stderr)
        return 2

    # Deterministic produced_utc captured once for this orchestrator run
    import datetime as _dt
    produced_utc = _dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    # Capture current git sha once (passed to stage 0; audit-only mismatch permitted by gate contract)
    import subprocess as _sp
    current_git_sha = _sp.check_output(
        ["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(Path.cwd())
    ).decode("utf-8").strip()

    # Track whether any prerequisite stage failed (blocks PhaseD).
    prereq_failed = False

    # --- Stage 0: Bundle B Engine Model Registry Gate (strict fail-closed) ---
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

    # --- Stage 1: Engines (soft: record failures, continue to gates) ---
    ok, _rc = _run_stage_soft(
        "ENGINE_MEAN_REVERSION",
        [
            "python3",
            "-m",
            "constellation_2.phaseI.mean_reversion.run.run_mean_reversion_intents_day_v1",
            "--day_utc",
            day,
            "--mode",
            mode,
            "--symbol",
            symbol,
        ],
    )
    if not ok:
        prereq_failed = True

    ok, _rc = _run_stage_soft(
        "ENGINE_TREND_EQ_PRIMARY",
        [
            "python3",
            "-m",
            "constellation_2.phaseI.trend_eq_primary.run.run_trend_eq_primary_intents_day_v1",
            "--day_utc",
            day,
            "--mode",
            mode,
            "--symbol",
            symbol,
        ],
    )
    if not ok:
        prereq_failed = True

    ok, _rc = _run_stage_soft(
        "ENGINE_VOL_INCOME_DEFINED",
        [
            "python3",
            "-m",
            "constellation_2.phaseI.vol_income_defined_risk.run.run_vol_income_defined_risk_intents_day_v1",
            "--day_utc",
            day,
            "--mode",
            mode,
            "--symbol",
            symbol,
        ],
    )
    if not ok:
        prereq_failed = True

    # --- Stage 2: PhaseC Preflight (strict; uses input_day to read truth if needed) ---
    _run_stage_strict(
        "PHASEC_PREFLIGHT",
        [
            "python3",
            "-m",
            "constellation_2.phaseC.tools.c2_submit_preflight_offline_v1",
            "--day_utc",
            input_day,
        ],
    )

    # --- Stage 3: PhaseH OMS (strict; uses input_day) ---
    _run_stage_strict(
        "PHASEH_OMS",
        [
            "python3",
            "-m",
            "constellation_2.phaseH.tools.run_oms_decisions_day_v1",
            "--day_utc",
            input_day,
        ],
    )

    # --- Stage 3.5: Bundle B.2 Capital-at-Risk Envelope Gate (strict; always run, uses input_day) ---
    _run_stage_strict(
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

    # If any engine failed, block submissions after writing gates/reports.
    if prereq_failed:
        print("FATAL: one or more engine stages failed; submission blocked (gates written).", file=sys.stderr)
        return 2

    # --- Stage 4+: Only run submission pipeline if all prerequisites succeeded ---
    _run_stage_strict(
        "PHASED_PAPER_SUBMIT",
        [
            "python3",
            "-m",
            "constellation_2.phaseD.tools.c2_submit_paper_v1",
            "--day_utc",
            day,
        ],
    )

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
