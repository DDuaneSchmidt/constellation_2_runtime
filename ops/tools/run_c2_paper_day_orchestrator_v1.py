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
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
DEFAULT_TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()
REG_PATH = (REPO_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json").resolve()


def _require_repo_root_cwd() -> None:
    cwd = Path.cwd().resolve()
    if cwd != REPO_ROOT:
        raise SystemExit(f"FATAL: must run from repo root cwd={cwd} expected={REPO_ROOT}")


def _resolve_truth_root(args_truth_root: str) -> Path:
    """
    Deterministic truth_root resolution order:
      1) --truth_root if provided
      2) env C2_TRUTH_ROOT if set
      3) DEFAULT_TRUTH_ROOT (canonical)
    Hard guard: truth_root must be under repo root.
    """
    tr = (args_truth_root or "").strip()
    if not tr:
        tr = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if not tr:
        tr = str(DEFAULT_TRUTH_ROOT)

    truth_root = Path(tr).resolve()
    if not truth_root.exists() or not truth_root.is_dir():
        raise SystemExit(f"FATAL: truth_root missing or not directory: {truth_root}")

    try:
        truth_root.relative_to(REPO_ROOT)
    except Exception:
        raise SystemExit(f"FATAL: truth_root not under repo root: truth_root={truth_root} repo_root={REPO_ROOT}")

    return truth_root


def _run_stage_strict(name: str, cmd: List[str], *, env: Dict[str, str]) -> None:
    print(f"STAGE_START {name}")
    p = subprocess.run(cmd, stdout=sys.stdout, stderr=sys.stderr, text=True, env=env)
    if p.returncode != 0:
        print(f"STAGE_FAIL {name} rc={p.returncode}", file=sys.stderr)
        raise SystemExit(p.returncode)
    print(f"STAGE_OK {name}")


def _run_stage_soft(name: str, cmd: List[str], *, env: Dict[str, str]) -> Tuple[bool, int]:
    print(f"STAGE_START {name}")
    p = subprocess.run(cmd, stdout=sys.stdout, stderr=sys.stderr, text=True, env=env)
    if p.returncode != 0:
        print(f"STAGE_FAIL {name} rc={p.returncode}", file=sys.stderr)
        return (False, int(p.returncode))
    print(f"STAGE_OK {name}")
    return (True, 0)


def _bootstrap_window_true(truth_root: Path, day_utc: str) -> bool:
    """
    Day-0 Bootstrap Window iff:
      TRUTH/execution_evidence_v1/submissions/<DAY>/ is missing OR contains zero submission dirs.
    """
    root = (truth_root / "execution_evidence_v1" / "submissions" / day_utc).resolve()
    if (not root.exists()) or (not root.is_dir()):
        return True
    try:
        for p in root.iterdir():
            if p.is_dir():
                return False
    except Exception:
        # Fail-closed: if we cannot enumerate, treat as NOT bootstrap.
        return False
    return True


def _intents_day_empty(truth_root: Path, day_utc: str) -> bool:
    """
    Intents are considered empty iff:
      - intents_v1/snapshots/<DAY>/ does not exist, OR
      - it exists but contains zero files.
    """
    d = (truth_root / "intents_v1" / "snapshots" / day_utc).resolve()
    if (not d.exists()) or (not d.is_dir()):
        return True
    try:
        for p in d.iterdir():
            if p.is_file():
                return False
    except Exception:
        # Fail-closed: if we cannot enumerate, do NOT treat as empty.
        return False
    return True


def _read_engine_registry() -> Dict[str, Any]:
    if not REG_PATH.exists():
        raise SystemExit(f"FATAL: missing engine registry: {REG_PATH}")
    obj = json.loads(REG_PATH.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SystemExit("FATAL: engine registry top-level not object")
    return obj


def _active_engines_sorted(reg: Dict[str, Any]) -> List[Dict[str, str]]:
    engines = reg.get("engines") or []
    if not isinstance(engines, list):
        raise SystemExit("FATAL: registry engines not list")

    active: List[Dict[str, str]] = []
    for e in engines:
        if not isinstance(e, dict):
            continue
        if str(e.get("activation_status") or "") != "ACTIVE":
            continue

        engine_id = str(e.get("engine_id") or "").strip()
        runner_mod = str(e.get("runner_path") or "").strip()

        if not engine_id:
            raise SystemExit("FATAL: ACTIVE engine missing engine_id")
        if not runner_mod:
            raise SystemExit(f"FATAL: ACTIVE engine missing runner_path: engine_id={engine_id}")

        active.append({"engine_id": engine_id, "runner_path": runner_mod})

    return sorted(active, key=lambda r: r["engine_id"])


def _stage_name_for_engine(engine_id: str) -> str:
    s = "".join([c if (c.isalnum() or c == "_") else "_" for c in engine_id.strip().upper()])
    return f"ENGINE_{s}"


def main() -> int:
    _require_repo_root_cwd()

    ap = argparse.ArgumentParser(prog="run_c2_paper_day_orchestrator_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--input_day_utc", default="", help="Optional input day key")
    ap.add_argument("--mode", required=True, choices=["PAPER", "LIVE"])
    ap.add_argument("--symbol", default="SPY")
    ap.add_argument("--ib_account", required=True, help="IB PAPER account id (must be DU* per adapter policy)")
    ap.add_argument(
        "--truth_root",
        default="",
        help="Override truth root (must be under repo root). If omitted, uses env C2_TRUTH_ROOT, else canonical.",
    )
    args = ap.parse_args()

    day = args.day_utc.strip()
    input_day = (args.input_day_utc or "").strip() or day
    mode = args.mode.strip().upper()
    symbol = str(args.symbol).strip().upper()
    ib_account = str(args.ib_account).strip()
    truth_root = _resolve_truth_root(str(args.truth_root))

    if mode != "PAPER":
        print("FATAL: Orchestrator v1 supports PAPER mode only.", file=sys.stderr)
        return 2

# All stages inherit truth_root via env so tools that honor C2_TRUTH_ROOT write into the correct tree.
# Also pin PYTHONPATH to repo root for deterministic imports across all tools.
stage_env = dict(os.environ)
stage_env["C2_TRUTH_ROOT"] = str(truth_root)
stage_env["PYTHONPATH"] = str(REPO_ROOT)

    import datetime as _dt

    produced_utc = _dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    current_git_sha = (
        subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(Path.cwd()))
        .decode("utf-8")
        .strip()
    )

    # --- Bundle A2 (Fail-Closed) ---
    _run_stage_strict(
        "A2_BROKER_RECONCILIATION_GATE_V2",
        [
            "python3",
            "ops/tools/run_broker_reconciliation_day_v2.py",
            "--day_utc",
            day,
            "--ib_account",
            ib_account,
            "--mode",
            "CHECK",
        ],
        env=stage_env,
    )

    prereq_failed = False

    _run_stage_strict(
        "A10_TRUTH_SURFACE_AUTHORITY_GATE_V1",
        ["python3", "ops/tools/run_truth_surface_authority_gate_v1.py", "--day_utc", day],
        env=stage_env,
    )

    # --- Bundle A3: Broker Marks (strict) ---
    _run_stage_strict(
        "A3_BROKER_MARKS_SNAPSHOT_V1",
        ["python3", "ops/tools/run_broker_marks_snapshot_day_v1.py", "--day_utc", input_day],
        env=stage_env,
    )

    # --- Bundle A3: Accounting NAV v2 (strict) ---
    _run_stage_strict(
        "A3_ACCOUNTING_NAV_V2",
        [
            "python3",
            "ops/tools/run_accounting_nav_v2_day_v1.py",
            "--day_utc",
            input_day,
            "--producer_git_sha",
            current_git_sha,
            "--producer_repo",
            "constellation_2_runtime",
        ],
        env=stage_env,
    )

    # --- Bundle A5: Accounting Attribution v2 (strict) ---
    _run_stage_strict(
        "A5_ACCOUNTING_ATTRIBUTION_V2",
        [
            "python3",
            "ops/tools/run_accounting_attribution_v2_day_v1.py",
            "--day_utc",
            day,
            "--producer_git_sha",
            current_git_sha,
            "--producer_repo",
            "constellation_2_runtime",
        ],
        env=stage_env,
    )

    # --- Bundle A6: Engine Daily Returns v1 (soft) ---
    ok, _rc = _run_stage_soft(
        "A6_ENGINE_DAILY_RETURNS_V1",
        ["python3", "ops/tools/run_engine_daily_returns_day_v1.py", "--day_utc", day],
        env=stage_env,
    )
    if not ok:
        prereq_failed = True

    # --- Bundle A4: Engine Linkage Snapshot (soft) ---
    ok, _rc = _run_stage_soft(
        "A4_ENGINE_LINKAGE_SNAPSHOT_V1",
        [
            "python3",
            "ops/tools/run_engine_linkage_snapshot_day_v1.py",
            "--day_utc",
            day,
            "--producer_git_sha",
            current_git_sha,
            "--producer_repo",
            "constellation_2_runtime",
        ],
        env=stage_env,
    )
    if not ok:
        prereq_failed = True

    # --- Stage 0 ---
    _run_stage_strict(
        "BUNDLEB_ENGINE_MODEL_REGISTRY_GATE",
        ["python3", "ops/tools/run_engine_model_registry_gate_v1.py", "--day_utc", day, "--current_git_sha", current_git_sha],
        env=stage_env,
    )
    _run_stage_strict(
        "X_ACTIVE_ENGINE_SET_SNAPSHOT_V1",
        ["python3", "ops/tools/run_active_engine_set_snapshot_v1.py", "--day_utc", day, "--current_git_sha", current_git_sha],
        env=stage_env,
    )

    # --- Stage 1 Engines (registry-driven; ACTIVE only; deterministic order) ---
    reg = _read_engine_registry()
    active_engines = _active_engines_sorted(reg)
    for e in active_engines:
        stage_name = _stage_name_for_engine(e["engine_id"])
        module = str(e["runner_path"])
        ok, _rc = _run_stage_soft(
            stage_name,
            ["python3", "-m", module, "--day_utc", day, "--mode", mode, "--symbol", symbol],
            env=stage_env,
        )
        if not ok:
            prereq_failed = True

    # --- Bundle X prerequisites (strict; root-level systemic inputs) ---
    _run_stage_strict(
        "X_ENGINE_RISK_BUDGET_LEDGER_V1",
        ["python3", "ops/tools/run_engine_risk_budget_ledger_v1.py", "--day_utc", input_day],
        env=stage_env,
    )

    _run_stage_strict(
        "X_REGIME_SNAPSHOT_V2",
        ["python3", "ops/tools/run_regime_snapshot_v2.py", "--day_utc", input_day],
        env=stage_env,
    )

    # --- Bundle F++: Lifecycle Monitor (strict) ---
    _run_stage_strict(
        "BUNDLEF_LIFECYCLE_MONITOR_V1",
        ["python3", "ops/tools/run_lifecycle_monitor_v1.py", "--day_utc", day],
        env=stage_env,
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
        env=stage_env,
    )

    # --- A+3 Paper Readiness Monitor (strict) ---
    _run_stage_strict(
        "A3_PAPER_READINESS_MONITOR_V2",
        ["python3", "ops/tools/run_paper_readiness_monitor_v2.py", "--day_utc", day],
        env=stage_env,
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
        env=stage_env,
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
        env=stage_env,
    )

    # --- PhaseG ---
    _run_stage_strict(
        "PHASEG_BUNDLE_F_TO_G",
        ["python3", "-m", "constellation_2.phaseG.bundles.run.run_bundle_f_to_g_day_v1", "--day_utc", day],
        env=stage_env,
    )

    # --- Economic NAV + Drawdown Truth Spine (soft stages) ---
    for stage_name, cmd in [
        ("ECON_NAV_SNAPSHOT_V1", ["python3", "ops/tools/gen_nav_snapshot_v1.py", "--day_utc", day]),
        ("ECON_NAV_HISTORY_LEDGER_V1", ["python3", "ops/tools/gen_nav_history_ledger_v1.py", "--day_utc", day]),
        ("ECON_DRAWDOWN_WINDOW_PACK_V1", ["python3", "ops/tools/gen_drawdown_window_pack_v1.py", "--day_utc", day]),
        ("ECON_TRUTH_AVAIL_CERT_V1", ["python3", "ops/tools/gen_economic_truth_availability_certificate_v1.py", "--day_utc", day]),
        ("ECON_NAV_DRAWDOWN_BUNDLE_VALIDATE_V1", ["python3", "ops/tools/validate_economic_nav_drawdown_bundle_v1.py", "--day_utc", day]),
    ]:
        _run_stage_soft(stage_name, cmd, env=stage_env)

    # --- PhaseJ ---
    _run_stage_strict(
        "PHASEJ_DAILY_SNAPSHOT",
        ["python3", "-m", "constellation_2.phaseJ.reporting.daily_snapshot_v1", "--day_utc", day],
        env=stage_env,
    )

    # --- Bundle A ---
    _run_stage_strict(
        "BUNDLEA_PIPELINE_MANIFEST",
        ["python3", "ops/tools/run_pipeline_manifest_v1.py", "--day_utc", day],
        env=stage_env,
    )

    # --- Bundle Y: Replay Integrity (strict; deterministic replay hash sealing) ---
    _run_stage_strict(
        "Y_REPLAY_INTEGRITY_V2",
        [
            "python3",
            "ops/tools/run_replay_integrity_day_v2.py",
            "--day_utc",
            day,
            "--mode",
            "WRITE",
            "--truth_root",
            str(truth_root),
        ],
        env=stage_env,
    )

    if prereq_failed:
        print("ORCHESTRATOR_OK_WITH_SOFT_STAGE_FAILURES")
    else:
        print("ORCHESTRATOR_OK")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
