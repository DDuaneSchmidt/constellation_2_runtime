#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2].resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import (
    read_paper_session_ledger_ref_v1,
    resolve_authoritative_repo_root_v1,
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
from constellation_2.common.trade_submit_readiness_authority_v1 import (
    resolve_governed_sleeve_truth_bindings,
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
ACCOUNTING_NAV_COMPAT_BRIDGE_TOOL = (REPO_ROOT / "ops/tools/bridge_accounting_nav_v2_to_compat_v1.py").resolve()
ALLOCATION_DAY_V2_TOOL = (REPO_ROOT / "constellation_2/phaseG/allocation/run/run_allocation_day_v2.py").resolve()
RECONCILIATION_REPORT_V3_TOOL = (REPO_ROOT / "ops/tools/run_reconciliation_report_v3.py").resolve()
EXIT_RECONCILIATION_DAY_V1_TOOL = (
    REPO_ROOT / "constellation_2/phaseI/exit_reconciliation/run/run_exit_reconciliation_day_v1.py"
).resolve()
STARTUP_PROOF_TOOL = (REPO_ROOT / "ops/tools/run_startup_proof_validation_v1.py").resolve()
DEPLOYMENT_TOOL = (REPO_ROOT / "ops/tools/run_deployment_state_machine_v1.py").resolve()
TRADING_DAY_STATE_MACHINE_TOOL = (REPO_ROOT / "ops/tools/run_trading_day_state_machine_v1.py").resolve()
EXECUTION_JOURNAL_TOOL = (REPO_ROOT / "ops/tools/run_execution_journal_v1.py").resolve()
CURRENT_SYSTEM_PROJECTION_TOOL = (REPO_ROOT / "ops/tools/run_current_system_projection_v1.py").resolve()
EXECUTION_TOOL = (REPO_ROOT / "ops/tools/run_c2_multi_sleeve_orchestrator_v1.py").resolve()
ENGINE_REGISTRY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json").resolve()
MARKET_DATA_DOWNLOADER_TOOL = (
    REPO_ROOT / "constellation_2/phaseJ/tools/ib_historical_market_data_snapshot_downloader_v1.py"
).resolve()
LIQUIDITY_SLIPPAGE_GATE_TOOL = (REPO_ROOT / "ops/tools/run_liquidity_slippage_gate_v1.py").resolve()
CAPITAL_RISK_ENVELOPE_GATE_TOOL = (REPO_ROOT / "ops/tools/run_c2_capital_risk_envelope_gate_v2.py").resolve()
FEED_ATTESTATION_GATE_TOOL = (REPO_ROOT / "ops/tools/run_feed_attestation_gate_v1.py").resolve()
OPERATOR_DAILY_GATE_TOOL = (REPO_ROOT / "ops/tools/run_operator_daily_gate_v3.py").resolve()
HEARTBEAT_GATE_TOOL = (REPO_ROOT / "ops/tools/run_heartbeat_gate_v1.py").resolve()
CORRELATION_ENVELOPE_GATE_TOOL = (REPO_ROOT / "ops/tools/run_correlation_envelope_gate_v1.py").resolve()
REPLAY_CERTIFICATION_GATE_TOOL = (REPO_ROOT / "ops/tools/run_replay_certification_gate_v1.py").resolve()
GATE_STACK_VERDICT_TOOL = (REPO_ROOT / "ops/tools/run_gate_stack_verdict_v1.py").resolve()


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


def _sha256_file(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _mirror_canonical_file(*, source_path: Path, target_path: Path, artifact_id: str) -> Dict[str, Any]:
    if not source_path.exists() or not source_path.is_file():
        return {
            "status": "SOURCE_MISSING",
            "artifact_id": artifact_id,
            "source_path": str(source_path),
            "target_path": str(target_path),
        }
    source_bytes = source_path.read_bytes()
    source_sha = _sha256_file(source_path)
    if target_path.exists():
        target_sha = _sha256_file(target_path)
        if target_sha != source_sha:
            return {
                "status": "TARGET_MISMATCH",
                "artifact_id": artifact_id,
                "source_path": str(source_path),
                "target_path": str(target_path),
                "source_sha256": source_sha,
                "target_sha256": target_sha,
            }
        return {
            "status": "EXISTS_IDENTICAL",
            "artifact_id": artifact_id,
            "source_path": str(source_path),
            "target_path": str(target_path),
            "sha256": source_sha,
        }
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_bytes(source_bytes)
    return {
        "status": "COPIED",
        "artifact_id": artifact_id,
        "source_path": str(source_path),
        "target_path": str(target_path),
        "sha256": source_sha,
    }


def _mirror_canonical_day_json_dir(*, source_dir: Path, target_dir: Path, artifact_id: str) -> Dict[str, Any]:
    if not source_dir.exists() or not source_dir.is_dir():
        return {
            "status": "SOURCE_DIR_MISSING",
            "artifact_id": artifact_id,
            "source_dir": str(source_dir),
            "target_dir": str(target_dir),
        }

    source_files = sorted([p for p in source_dir.iterdir() if p.is_file() and p.suffix == ".json"], key=lambda p: p.name)
    source_names = {p.name for p in source_files}
    if not source_files:
        return {
            "status": "SOURCE_DIR_EMPTY",
            "artifact_id": artifact_id,
            "source_dir": str(source_dir),
            "target_dir": str(target_dir),
        }

    target_names = set()
    if target_dir.exists() and target_dir.is_dir():
        target_names = {p.name for p in target_dir.iterdir() if p.is_file() and p.suffix == ".json"}
    extra_names = sorted(target_names - source_names)
    if extra_names:
        return {
            "status": "TARGET_EXTRA_FILES",
            "artifact_id": artifact_id,
            "source_dir": str(source_dir),
            "target_dir": str(target_dir),
            "extra_files": extra_names,
        }

    results: List[Dict[str, Any]] = []
    for source_file in source_files:
        target_file = (target_dir / source_file.name).resolve()
        results.append(
            _mirror_canonical_file(
                source_path=source_file.resolve(),
                target_path=target_file,
                artifact_id=f"{artifact_id}:{source_file.name}",
            )
        )
    statuses = {str(row.get("status") or "") for row in results}
    if statuses == {"EXISTS_IDENTICAL"}:
        status = "EXISTS_IDENTICAL"
    elif any(status.endswith("MISMATCH") or status.startswith("SOURCE_") for status in statuses):
        status = "PARTIAL_FAILURE"
    else:
        status = "SYNCED"
    return {
        "status": status,
        "artifact_id": artifact_id,
        "source_dir": str(source_dir),
        "target_dir": str(target_dir),
        "file_count": len(source_files),
        "results": results,
    }


def _active_market_data_symbols() -> list[str]:
    if not ENGINE_REGISTRY_PATH.exists() or not ENGINE_REGISTRY_PATH.is_file():
        raise SystemExit(f"FAIL: engine_registry_missing path={ENGINE_REGISTRY_PATH}")
    try:
        payload = json.loads(ENGINE_REGISTRY_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SystemExit(
            f"FAIL: engine_registry_parse_error path={ENGINE_REGISTRY_PATH} err={type(exc).__name__}:{exc}"
        ) from exc
    engines = payload.get("engines")
    if not isinstance(engines, list):
        raise SystemExit(f"FAIL: engine_registry_invalid_engines path={ENGINE_REGISTRY_PATH}")
    symbols: set[str] = set()
    for row in engines:
        if not isinstance(row, dict):
            continue
        if str(row.get("activation_status") or "").strip().upper() != "ACTIVE":
            continue
        if str(row.get("engine_id") or "").strip() == "C2_INTENT_SIMULATOR_V1":
            continue
        allowed = row.get("allowed_symbols")
        if not isinstance(allowed, list):
            continue
        for symbol in allowed:
            text = str(symbol or "").strip().upper()
            if text:
                symbols.add(text)
    if not symbols:
        raise SystemExit("FAIL: no_active_market_data_symbols")
    return sorted(symbols)


def _resolve_primary_paper_sleeve_truth_root(*, ib_account: str) -> Path:
    authoritative_repo_root = resolve_authoritative_repo_root_v1(REPO_ROOT)
    bindings = resolve_governed_sleeve_truth_bindings(
        repo_root=authoritative_repo_root,
        environment="PAPER",
        requested_ib_account=ib_account,
    )
    for binding in bindings:
        if str(binding.sleeve_id).strip().upper() == "PRIMARY":
            return Path(binding.truth_root).resolve()
    if not bindings:
        raise SystemExit(f"FAIL: no_governed_sleeve_truth_bindings environment=PAPER ib_account={ib_account}")
    return Path(bindings[0].truth_root).resolve()


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
    primary_sleeve_truth_root = _resolve_primary_paper_sleeve_truth_root(ib_account=ib_account)
    operator_statement_path = resolve_operator_statement_path(operator_input_root=truth_root, day_utc=day_utc)
    market_data_symbols = _active_market_data_symbols()
    market_data_run_utc = f"{day_utc}T00:00:00Z"
    canonical_positions_snapshot_path = (
        truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json"
    ).resolve()
    sleeve_positions_snapshot_path = (
        primary_sleeve_truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json"
    ).resolve()
    canonical_cash_ledger_snapshot_path = (
        truth_root / "cash_ledger_v1" / "snapshots" / day_utc / "cash_ledger_snapshot.v1.json"
    ).resolve()
    sleeve_cash_ledger_snapshot_path = (
        primary_sleeve_truth_root / "cash_ledger_v1" / "snapshots" / day_utc / "cash_ledger_snapshot.v1.json"
    ).resolve()
    canonical_intents_day_dir = (truth_root / "intents_v1" / "snapshots" / day_utc).resolve()
    sleeve_intents_day_dir = (primary_sleeve_truth_root / "intents_v1" / "snapshots" / day_utc).resolve()
    ib_host = str(os.environ.get("C2_IB_HOST") or "127.0.0.1").strip()
    ib_port = str(os.environ.get("C2_IB_PORT") or "4002").strip()
    ib_client_id = str(os.environ.get("C2_IB_CLIENT_ID") or "7").strip()
    ib_sleep_sec = str(os.environ.get("C2_IB_SLEEP_SEC") or "0.1").strip()

    runs = {
        "startup_materialization": _run(
            [sys.executable, str(STARTUP_TOOL), "--day_utc", day_utc, "--truth_root", str(truth_root)],
            truth_root=truth_root,
        ),
        "paper_trading_posture": _run(
            [sys.executable, str(POSTURE_TOOL), "--day_utc", day_utc, "--truth_root", str(truth_root)],
            truth_root=truth_root,
        ),
        "submit_boundary_status_pre_gates": _run(
            [sys.executable, str(BOUNDARY_TOOL), "--day_utc", day_utc, "--truth_root", str(truth_root)],
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
        "market_data_snapshot_manifest_v1": _run(
            [
                sys.executable,
                str(MARKET_DATA_DOWNLOADER_TOOL),
                "--run_utc",
                market_data_run_utc,
                "--dataset_version",
                "v1",
                *[part for symbol in market_data_symbols for part in ("--symbol", symbol)],
                "--start_year",
                day_utc[:4],
                "--end_year",
                day_utc[:4],
                "--host",
                ib_host,
                "--port",
                ib_port,
                "--client_id",
                ib_client_id,
                "--sleep_sec",
                ib_sleep_sec,
                "--use_rth",
                "1",
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
        "sleeve_positions_snapshot_seed_v2": _mirror_canonical_file(
            source_path=canonical_positions_snapshot_path,
            target_path=sleeve_positions_snapshot_path,
            artifact_id="sleeve_positions_snapshot_v2",
        ),
        "sleeve_cash_ledger_snapshot_seed_v1": _mirror_canonical_file(
            source_path=canonical_cash_ledger_snapshot_path,
            target_path=sleeve_cash_ledger_snapshot_path,
            artifact_id="sleeve_cash_ledger_snapshot_v1",
        ),
        "sleeve_accounting_nav_v2": _run(
            [
                sys.executable,
                str(ACCOUNTING_NAV_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(primary_sleeve_truth_root),
                "--producer_repo",
                producer_repo,
                "--producer_git_sha",
                producer_git_sha,
            ],
            truth_root=primary_sleeve_truth_root,
        ),
        "sleeve_accounting_nav_compat_v1": _run(
            [
                sys.executable,
                str(ACCOUNTING_NAV_COMPAT_BRIDGE_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(primary_sleeve_truth_root),
            ],
            truth_root=primary_sleeve_truth_root,
        ),
        "sleeve_intents_snapshot_sync_v1": _mirror_canonical_day_json_dir(
            source_dir=canonical_intents_day_dir,
            target_dir=sleeve_intents_day_dir,
            artifact_id="sleeve_intents_snapshot_v1",
        ),
        "sleeve_engine_correlation_matrix_v1": _run(
            [
                sys.executable,
                str(ENGINE_CORRELATION_MATRIX_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(primary_sleeve_truth_root),
            ],
            truth_root=primary_sleeve_truth_root,
        ),
        "sleeve_reconciliation_report_v3": _run(
            [
                sys.executable,
                str(RECONCILIATION_REPORT_V3_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(primary_sleeve_truth_root),
            ],
            truth_root=primary_sleeve_truth_root,
        ),
        "sleeve_exit_reconciliation_v1": _run(
            [
                sys.executable,
                str(EXIT_RECONCILIATION_DAY_V1_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(primary_sleeve_truth_root),
                "--positions_snapshot_path",
                str(sleeve_positions_snapshot_path),
            ],
            truth_root=primary_sleeve_truth_root,
        ),
        "sleeve_allocation_day_v2": _run(
            [
                sys.executable,
                str(ALLOCATION_DAY_V2_TOOL),
                "--day_utc",
                day_utc,
                "--producer_git_sha",
                producer_git_sha,
                "--producer_repo",
                producer_repo,
                "--truth_root",
                str(primary_sleeve_truth_root),
            ],
            truth_root=primary_sleeve_truth_root,
        ),
        "sleeve_gate_liquidity_slippage_gate_v1": _run(
            [sys.executable, str(LIQUIDITY_SLIPPAGE_GATE_TOOL), "--day_utc", day_utc],
            truth_root=primary_sleeve_truth_root,
        ),
        "sleeve_gate_capital_risk_envelope_v2": _run(
            [
                sys.executable,
                str(CAPITAL_RISK_ENVELOPE_GATE_TOOL),
                "--out_day_utc",
                day_utc,
                "--input_day_utc",
                input_day_utc,
                "--truth_root",
                str(primary_sleeve_truth_root),
                "--produced_utc",
                f"{day_utc}T00:00:00Z",
            ],
            truth_root=primary_sleeve_truth_root,
        ),
        "sleeve_gate_feed_attestation_gate_v1": _run(
            [sys.executable, str(FEED_ATTESTATION_GATE_TOOL), "--day_utc", day_utc, "--truth_root", str(primary_sleeve_truth_root)],
            truth_root=primary_sleeve_truth_root,
        ),
        "sleeve_gate_operator_daily_gate_v3": _run(
            [
                sys.executable,
                str(OPERATOR_DAILY_GATE_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(primary_sleeve_truth_root),
                "--produced_utc",
                f"{day_utc}T00:00:00Z",
                "--mode",
                "PAPER",
            ],
            truth_root=primary_sleeve_truth_root,
        ),
        "sleeve_gate_heartbeat_gate_v1": _run(
            [sys.executable, str(HEARTBEAT_GATE_TOOL), "--day_utc", day_utc, "--truth_root", str(primary_sleeve_truth_root)],
            truth_root=primary_sleeve_truth_root,
        ),
        "sleeve_gate_correlation_envelope_gate_v1": _run(
            [
                sys.executable,
                str(CORRELATION_ENVELOPE_GATE_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(primary_sleeve_truth_root),
                "--produced_utc",
                f"{day_utc}T00:00:00Z",
                "--mode",
                "PAPER",
            ],
            truth_root=primary_sleeve_truth_root,
        ),
        "sleeve_gate_replay_certification_gate_v1": _run(
            [
                sys.executable,
                str(REPLAY_CERTIFICATION_GATE_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(primary_sleeve_truth_root),
                "--produced_utc",
                f"{day_utc}T00:00:00Z",
                "--mode",
                "PAPER",
            ],
            truth_root=primary_sleeve_truth_root,
        ),
        "sleeve_gate_gate_stack_verdict_v1": _run(
            [
                sys.executable,
                str(GATE_STACK_VERDICT_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(primary_sleeve_truth_root),
                "--produced_utc",
                f"{day_utc}T00:00:00Z",
                "--mode",
                "PAPER",
            ],
            truth_root=primary_sleeve_truth_root,
        ),
        "submit_boundary_status": _run(
            [sys.executable, str(BOUNDARY_TOOL), "--day_utc", day_utc, "--truth_root", str(truth_root)],
            truth_root=truth_root,
        ),
        "paper_session_ledger": _run(
            [sys.executable, str(LEDGER_TOOL), "--day_utc", day_utc, "--truth_root", str(truth_root)],
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
