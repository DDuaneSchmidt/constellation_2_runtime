#!/usr/bin/env python3
"""
run_c2_paper_day_orchestrator_v1.py

Constellation 2.0 — Bundle 2 (+ Paper Readiness Enhancements)
Institutional-grade PAPER day orchestrator.

USAGE (authoritative, proven safe):
  cd /home/node/constellation_2_runtime
  python3 ops/tools/run_c2_paper_day_orchestrator_v1.py \
    --day_utc YYYY-MM-DD \
    --mode PAPER \
    --ib_account DUXXXXXXX \
    --produced_utc YYYY-MM-DDTHH:MM:SSZ

NON-NEGOTIABLE PROPERTIES:
- Deterministic stage order
- PAPER mode only
- No implicit deletion or mutation
- Structured audit logging
- Deterministic produced_utc (operator supplied; orchestrator MUST NOT synthesize time)
- Fail-closed for submission (PhaseD blocked on prereq failure)
- Fail-closed for paper readiness (broker evidence + linkage + attribution + monitor)
- Bundle C: Emit per-engine heartbeat artifacts (authoritative) after each engine runner attempt
"""

from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
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


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


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


def _validate_produced_utc(produced_utc: str) -> str:
    """
    Fail-closed deterministic produced_utc validation.
    Required format: ISO-8601 UTC with 'Z' suffix, e.g. 2026-02-26T04:54:33Z
    Orchestrator MUST NOT synthesize time.
    """
    s = (produced_utc or "").strip()
    if not s:
        raise SystemExit("FATAL: produced_utc required")
    if not s.endswith("Z"):
        raise SystemExit(f"FATAL: produced_utc must end with 'Z' (UTC): {s}")
    if "T" not in s:
        raise SystemExit(f"FATAL: produced_utc must be ISO-8601 with 'T': {s}")

    # Strict parse: Python's fromisoformat does not accept 'Z', so normalize to +00:00.
    try:
        dt = _dt.datetime.fromisoformat(s[:-1] + "+00:00")
    except Exception:
        raise SystemExit(f"FATAL: produced_utc not parseable ISO-8601 UTC: {s}")

    if dt.tzinfo is None:
        raise SystemExit(f"FATAL: produced_utc must be timezone-aware UTC: {s}")

    # Normalize back to canonical Z form (no microseconds).
    dt = dt.astimezone(_dt.timezone.utc).replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


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


def _locked_git_sha_for_positions_day(truth_root: Path, day_utc: str, fallback_git_sha: str) -> str:
    """
    Day-SHA lock: if the day already has a positions snapshot, reuse its producer.git_sha
    so immutable/idempotent downstream stages do not fail on reruns.
    """
    p = (truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json").resolve()
    if not p.exists() or not p.is_file():
        return fallback_git_sha
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
        sha = str(((obj.get("producer") or {}).get("git_sha") or "")).strip()
        return sha if sha else fallback_git_sha
    except Exception:
        return fallback_git_sha


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
        runner_file = str(e.get("engine_runner_path") or "").strip()
        runner_sha = str(e.get("engine_runner_sha256") or "").strip()

        if not engine_id:
            raise SystemExit("FATAL: ACTIVE engine missing engine_id")
        if not runner_mod:
            raise SystemExit(f"FATAL: ACTIVE engine missing runner_path: engine_id={engine_id}")
        if not runner_file:
            raise SystemExit(f"FATAL: ACTIVE engine missing engine_runner_path: engine_id={engine_id}")
        if (len(runner_sha) != 64) or any(c not in "0123456789abcdef" for c in runner_sha.lower()):
            raise SystemExit(f"FATAL: ACTIVE engine missing/invalid engine_runner_sha256: engine_id={engine_id} sha={runner_sha!r}")

        active.append(
            {
                "engine_id": engine_id,
                "runner_path": runner_mod,
                "engine_runner_path": runner_file,
                "engine_runner_sha256": runner_sha.lower(),
            }
        )

    return sorted(active, key=lambda r: r["engine_id"])


def _stage_name_for_engine(engine_id: str) -> str:
    s = "".join([c if (c.isalnum() or c == "_") else "_" for c in engine_id.strip().upper()])
    return f"ENGINE_{s}"


def _stage_name_for_heartbeat(engine_id: str) -> str:
    s = "".join([c if (c.isalnum() or c == "_") else "_" for c in engine_id.strip().upper()])
    return f"HB_{s}"


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
    ap.add_argument(
        "--produced_utc",
        required=True,
        help="UTC ISO-8601 Z timestamp (deterministic; operator/orchestrator provided). Example: 2026-02-26T04:54:33Z",
    )
    args = ap.parse_args()

    day = args.day_utc.strip()
    input_day = (args.input_day_utc or "").strip() or day
    mode = args.mode.strip().upper()
    symbol = str(args.symbol).strip().upper()
    ib_account = str(args.ib_account).strip()
    truth_root = _resolve_truth_root(str(args.truth_root))
    produced_utc = _validate_produced_utc(str(args.produced_utc))

    if mode != "PAPER":
        print("FATAL: Orchestrator v1 supports PAPER mode only.", file=sys.stderr)
        return 2

    # All stages inherit truth_root via env so tools that honor C2_TRUTH_ROOT write into the correct tree.
    # Also pin PYTHONPATH to repo root for deterministic imports across all tools.
    stage_env = dict(os.environ)
    stage_env["C2_TRUTH_ROOT"] = str(truth_root)
    stage_env["PYTHONPATH"] = str(REPO_ROOT)
    # Deterministic produced_utc is operator-supplied; orchestrator does not synthesize time.
    stage_env["C2_PRODUCED_UTC"] = produced_utc

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
        ["python3", "ops/tools/run_broker_marks_snapshot_day_v1.py", "--day_utc", day],
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
    registry_sha = _sha256_file(REG_PATH)

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

        # Bundle C heartbeat (strict; must be emitted even if the engine failed)
        hb_status = "OK" if ok else "FAIL"
        hb_reason = "ENGINE_RUN_OK" if ok else "ENGINE_RUN_FAIL"

        hb_stage = _stage_name_for_heartbeat(e["engine_id"])
        _run_stage_strict(
            hb_stage,
            [
                "python3",
                "ops/tools/run_engine_heartbeat_emit_v1.py",
                "--day_utc",
                day,
                "--engine_id",
                e["engine_id"],
                "--status",
                hb_status,
                "--reason_code",
                hb_reason,
                "--last_run_utc",
                produced_utc,
                "--expected_period_seconds",
                "86400",
                "--stale_after_seconds",
                "172800",
                "--fingerprint",
                f"engine_registry|governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json|{registry_sha}|true",
                "--fingerprint",
                f"engine_runner|{e['engine_runner_path']}|{e['engine_runner_sha256']}|true",
                "--producer_repo",
                "constellation_2_runtime",
                "--producer_module",
                "ops/tools/run_engine_heartbeat_emit_v1.py",
                "--producer_git_sha",
                current_git_sha,
            ],
            env=stage_env,
        )

    # --- Bundle X prerequisites (strict; root-level systemic inputs) ---
    _run_stage_strict(
        "X_ENGINE_RISK_BUDGET_LEDGER_V1",
        ["python3", "ops/tools/run_engine_risk_budget_ledger_v1.py", "--day_utc", input_day],
        env=stage_env,
    )

    _run_stage_strict(
        "C_HEARTBEAT_GATE_V1",
        [
            "python3",
            "ops/tools/run_heartbeat_gate_v1.py",
            "--day_utc",
            day,
            "--truth_root",
            str(truth_root),
            "--expected_period_seconds",
            "86400",
            "--stale_after_seconds",
            "172800",
        ],
        env=stage_env,
    )
    _run_stage_strict(
        "X_REGIME_SNAPSHOT_V2",
        ["python3", "ops/tools/run_regime_snapshot_v2.py", "--day_utc", input_day],
        env=stage_env,
    )

    # --- Bundle F++: Lifecycle Monitor (strict) ---
    # --- Bundle F prereqs for lifecycle monitor (strict; must exist before monitor) ---
    _run_stage_strict(
        "F_POSITION_LIFECYCLE_SNAPSHOT_V2",
        [
            "python3",
            "ops/tools/run_position_lifecycle_snapshot_v2.py",
            "--day_utc",
            day,
        ],
        env=stage_env,
    )
    _run_stage_strict(
        "F_EXIT_OBLIGATIONS_V1",
        [
            "python3",
            "ops/tools/run_exit_obligations_v1.py",
            "--day_utc",
            day,
        ],
        env=stage_env,
    )
    _run_stage_strict(
        "F_EXPOSURE_RECONCILIATION_V2",
        [
            "python3",
            "ops/tools/run_exposure_reconciliation_v2.py",
            "--day_utc",
            day,
        ],
        env=stage_env,
    )

    _run_stage_strict(
        "BUNDLEF_LIFECYCLE_MONITOR_V1",
        ["python3", "ops/tools/run_lifecycle_monitor_v1.py", "--day_utc", day],
        env=stage_env,
    )

    # --- B+2 Positions Effective Pointer (strict; immutable) ---
    positions_day_git_sha = _locked_git_sha_for_positions_day(truth_root, day, current_git_sha)

    _run_stage_strict(
        "B2_POSITIONS_EFFECTIVE_POINTER_V1",
        [
            "python3",
            "-m",
            "constellation_2.phaseF.positions.run.run_positions_effective_pointer_day_v2",
            "--day_utc",
            day,
            "--producer_git_sha",
            positions_day_git_sha,
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
            "--producer_git_sha",
            current_git_sha,
            "--producer_repo",
            "constellation_2_runtime",
        ],
        env=stage_env,
    )

    _run_stage_strict(
        "PHASEF_SUBMISSION_INDEX",
        [
            "python3",
            "-m",
            "constellation_2.phaseF.execution_evidence.run.run_submission_index_day_v1",
            "--day",
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
        ],
        env=stage_env,
    )

    _run_stage_strict(
        "A8_GATE_STACK_VERDICT_V1",
        ["python3", "ops/tools/run_gate_stack_verdict_v1.py", "--day_utc", day],
        env=stage_env,
    )
    _run_stage_strict(
        "C_GLOBAL_KILL_SWITCH_V1",
        ["python3", "ops/tools/run_global_kill_switch_v1.py", "--day_utc", day],
        env=stage_env,
    )
    # Fail-closed posture: if any prereq failed, exit 2 (systemd may treat 2 as degraded if configured).
    if prereq_failed:
        print("FAIL: ORCHESTRATOR_PREREQ_FAILED", file=sys.stderr)
        return 2

    print("OK: C2_PAPER_DAY_ORCHESTRATOR_V1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
