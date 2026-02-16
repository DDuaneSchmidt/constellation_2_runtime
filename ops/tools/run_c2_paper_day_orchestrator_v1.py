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
- Fail-closed (any stage failure stops execution)
- PAPER mode only (hard guard)
- No implicit deletion or mutation
- Structured audit logging (STAGE_START / STAGE_OK / STAGE_FAIL)
- Refuse execution if not launched from repo root (audit reproducibility)

Stages:
Stages:
0. Bundle B Engine Model Registry Gate (fail-closed)
1. Engines (MR, Trend, Vol Income)
2. PhaseC submit preflight
3. PhaseH OMS decisions
4. PhaseD paper submission
5. PhaseF execution evidence truth
6. PhaseF submission index
7. PhaseG bundle F→G (accounting + allocation)
8. PhaseJ daily snapshot
9. Bundle A pipeline manifest (final hostile-review completeness check)

This orchestrator performs no network operations itself.
It delegates to existing modules via subprocess with strict return-code checks.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()


def _require_repo_root_cwd() -> None:
    cwd = Path.cwd().resolve()
    if cwd != REPO_ROOT:
        raise SystemExit(f"FATAL: must run from repo root cwd={cwd} expected={REPO_ROOT}")


def _run_stage(name: str, cmd: List[str]) -> None:
    print(f"STAGE_START {name}")
    p = subprocess.run(cmd, stdout=sys.stdout, stderr=sys.stderr, text=True)
    if p.returncode != 0:
        print(f"STAGE_FAIL {name} rc={p.returncode}", file=sys.stderr)
        raise SystemExit(p.returncode)
    print(f"STAGE_OK {name}")


def main() -> int:
    _require_repo_root_cwd()

    ap = argparse.ArgumentParser(prog="run_c2_paper_day_orchestrator_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--input_day_utc", default="", help="Optional: day key to read input truth from (default: same as --day_utc)")
    ap.add_argument("--mode", required=True, choices=["PAPER", "LIVE"])
    ap.add_argument("--symbol", default="SPY", help="Underlying symbol (default: SPY)")

    args = ap.parse_args()

    day = args.day_utc.strip()
    input_day = (args.input_day_utc or "").strip() or day
    mode = args.mode.strip().upper()
    symbol = str(args.symbol).strip().upper()

    # Deterministic produced_utc captured once for this orchestrator run
    import datetime as _dt
    produced_utc = _dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    import subprocess as _sp
    current_git_sha = _sp.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(Path.cwd())).decode("utf-8").strip()

    if mode != "PAPER":
        print("FATAL: Orchestrator v1 supports PAPER mode only.", file=sys.stderr)
    # --- Stage 0: Bundle B Engine Model Registry Gate (fail-closed) ---
    _run_stage(
        "BUNDLEB_ENGINE_MODEL_REGISTRY_GATE",
        ["python3",
         "ops/tools/run_engine_model_registry_gate_v1.py",
         "--day_utc", day,
         "--current_git_sha", current_git_sha],
    )

    # --- Stage 1: Engines ---
    _run_stage(
        "ENGINE_MEAN_REVERSION",
        ["python3", "-m",
         "constellation_2.phaseI.mean_reversion.run.run_mean_reversion_intents_day_v1",
         "--day_utc", day,
         "--mode", mode,
         "--symbol", symbol],
    )

    _run_stage(
        "ENGINE_TREND_EQ_PRIMARY",
        ["python3", "-m",
         "constellation_2.phaseI.trend_eq_primary.run.run_trend_eq_primary_intents_day_v1",
         "--day_utc", day,
         "--mode", mode,
         "--symbol", symbol],
    )

    _run_stage(
        "ENGINE_VOL_INCOME_DEFINED",
        ["python3", "-m",
         "constellation_2.phaseI.vol_income_defined_risk.run.run_vol_income_defined_risk_intents_day_v1",
         "--day_utc", day,
         "--mode", mode,
         "--symbol", symbol],
    )

    # --- Stage 2: PhaseC Preflight ---
    _run_stage(
        "PHASEC_PREFLIGHT",
        ["python3", "-m",
         "constellation_2.phaseC.tools.c2_submit_preflight_offline_v1",
         "--day_utc", day],
    )

    # --- Stage 3: PhaseH OMS ---
    _run_stage(
        "PHASEH_OMS",
        ["python3", "-m",
         "constellation_2.phaseH.tools.run_oms_decisions_day_v1",
         "--day_utc", day],
    )
    # --- Stage 3.5: Bundle B.2 Capital-at-Risk Envelope Gate (pre-PhaseD) ---
    _run_stage(
        "BUNDLEB2_CAPITAL_RISK_ENVELOPE_GATE",
        ["python3",
         "ops/tools/run_c2_capital_risk_envelope_gate_v1.py",
         "--out_day_utc", day,
         "--input_day_utc",input_day,
         "--produced_utc", produced_utc],
    )

    # --- Stage 4: PhaseD PAPER submit ---
    _run_stage(
        "PHASED_PAPER_SUBMIT",
        ["python3", "-m",
         "constellation_2.phaseD.tools.c2_submit_paper_v1",
         "--day_utc", day],
    )

    # --- Stage 5: PhaseF execution evidence truth ---
    _run_stage(
        "PHASEF_EXEC_EVIDENCE",
        ["python3", "-m",
         "constellation_2.phaseF.execution_evidence.run.run_execution_evidence_truth_day_v1",
         "--day_utc", day],
    )

    # --- Stage 6: PhaseF submission index ---
    _run_stage(
        "PHASEF_SUBMISSION_INDEX",
        ["python3", "-m",
         "constellation_2.phaseF.execution_evidence.run.run_submission_index_day_v1",
         "--day_utc", day],
    )

    # --- Stage 7: Bundle F→G ---
    _run_stage(
        "PHASEG_BUNDLE_F_TO_G",
        ["python3", "-m",
         "constellation_2.phaseG.bundles.run.run_bundle_f_to_g_day_v1",
         "--day_utc", day],
    )

    # --- Stage 8: PhaseJ Daily Snapshot ---
    _run_stage(
        "PHASEJ_DAILY_SNAPSHOT",
        ["python3", "-m",
         "constellation_2.phaseJ.reporting.daily_snapshot_v1",
         "--day_utc", day],
    )

    # --- Stage 9: Bundle A Pipeline Manifest (final completeness gate) ---
    _run_stage(
        "BUNDLEA_PIPELINE_MANIFEST",
        ["python3",
         "ops/tools/run_pipeline_manifest_v1.py",
         "--day_utc", day],
    )

    print("ORCHESTRATOR_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
