#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List
from datetime import date, timedelta

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
from constellation_2.common.execution_outcome_v1 import (
    derive_execution_outcome_payload,
    write_execution_outcome_v1,
)
from constellation_2.common.fresh_day_admission_v1 import (
    derive_fresh_day_admission_payload,
    resolve_fresh_day_admission_path,
    write_fresh_day_admission_v1,
)
from constellation_2.common.next_day_readiness_probe_v1 import (
    derive_next_day_readiness_probe_payload,
    resolve_next_day_readiness_probe_path,
    write_next_day_readiness_probe_v1,
)
from constellation_2.common.capability_state_v1 import (
    resolve_paper_policy_verdict_path,
    resolve_production_policy_verdict_path,
    resolve_policy_diff_path,
)
from constellation_2.common.decision_authority_bridge_v1 import (
    resolve_decision_truth_root_bridge_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_current_system_projection_path,
    resolve_deployment_state_machine_path,
    resolve_operator_statement_path,
    resolve_execution_journal_path,
    resolve_paper_session_ledger_path,
    resolve_submit_boundary_status_path,
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
CAPABILITY_STATE_TOOL = (REPO_ROOT / "ops/tools/run_capability_state_v1.py").resolve()
PAPER_POLICY_VERDICT_TOOL = (REPO_ROOT / "ops/tools/run_paper_policy_verdict_v1.py").resolve()
PRODUCTION_POLICY_VERDICT_TOOL = (REPO_ROOT / "ops/tools/run_production_policy_verdict_v1.py").resolve()
POLICY_DIFF_TOOL = (REPO_ROOT / "ops/tools/run_policy_diff_v1.py").resolve()


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


def _run_if_artifact_missing(
    cmd: List[str],
    *,
    truth_root: Path,
    artifact_path: Path,
    artifact_id: str,
    expected_day_utc: str,
) -> Dict[str, object]:
    if artifact_path.exists() and artifact_path.is_file():
        try:
            payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        except Exception:
            payload = None
        if isinstance(payload, dict):
            existing_day = str(payload.get("day_utc") or "").strip()
            if existing_day and existing_day != expected_day_utc:
                return _run(cmd, truth_root=truth_root)
            return {
                "cmd": cmd,
                "returncode": 0,
                "stdout": (
                    f"OK: {artifact_id} action=SKIP_EXISTING "
                    f"path={artifact_path}"
                ),
                "stderr": "",
            }
    return _run(cmd, truth_root=truth_root)


def _producer_git_sha() -> str:
    try:
        return str(resolve_release_provenance().get("git_sha") or "").strip() or ("0" * 40)
    except Exception:
        return "0" * 40


def _sha256_file(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_bytes_replace(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(prefix=f".{path.name}.tmp.", dir=str(path.parent), delete=False) as tmp:
        tmp.write(data)
        tmp.flush()
        import os as _os
        _os.fsync(tmp.fileno())
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def _positions_items_by_id(snapshot_obj: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    positions = snapshot_obj.get("positions") if isinstance(snapshot_obj.get("positions"), dict) else {}
    items = positions.get("items") if isinstance(positions, dict) else []
    if not isinstance(items, list):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        position_id = str(item.get("position_id") or "").strip()
        if position_id:
            out[position_id] = item
    return out


def _is_safe_positions_snapshot_v2_seed_upgrade(existing_obj: Dict[str, Any], candidate_obj: Dict[str, Any]) -> bool:
    if str(existing_obj.get("schema_id") or "").strip() != "C2_POSITIONS_SNAPSHOT_V2":
        return False
    if str(candidate_obj.get("schema_id") or "").strip() != "C2_POSITIONS_SNAPSHOT_V2":
        return False
    if str(existing_obj.get("day_utc") or "").strip() != str(candidate_obj.get("day_utc") or "").strip():
        return False
    existing_items = _positions_items_by_id(existing_obj)
    candidate_items = _positions_items_by_id(candidate_obj)
    if not existing_items and bool(candidate_items):
        return True
    if len(candidate_items) < len(existing_items):
        return False
    for position_id, existing_item in existing_items.items():
        if candidate_items.get(position_id) != existing_item:
            return False
    return True


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
            if source_path.name == "positions_snapshot.v2.json" and target_path.name == "positions_snapshot.v2.json":
                try:
                    source_obj = json.loads(source_bytes.decode("utf-8"))
                    target_obj = json.loads(target_path.read_text(encoding="utf-8"))
                except Exception:
                    source_obj = None
                    target_obj = None
                if isinstance(source_obj, dict) and isinstance(target_obj, dict):
                    if _is_safe_positions_snapshot_v2_seed_upgrade(target_obj, source_obj):
                        _write_bytes_replace(target_path, source_bytes)
                        return {
                            "status": "BACKFILL_REPAIRED",
                            "artifact_id": artifact_id,
                            "source_path": str(source_path),
                            "target_path": str(target_path),
                            "sha256": source_sha,
                        }
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


def _market_data_client_id() -> str:
    explicit = str(os.environ.get("C2_IB_CLIENT_ID") or "").strip()
    if explicit:
        return explicit
    return str(7000 + (os.getpid() % 1000))


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_paper_session_admission_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--input_day_utc", default="")
    ap.add_argument("--mode", default="PAPER", choices=["PAPER"])
    ap.add_argument("--symbol", default="SPY")
    ap.add_argument("--execute", default="NO", choices=["YES", "NO"])
    ap.add_argument("--truth_root", default=str((REPO_ROOT / "constellation_2/runtime/truth").resolve()))
    args = ap.parse_args()

    truth_root = resolve_decision_truth_root_bridge_v1(
        args.truth_root,
        repo_root=REPO_ROOT,
        caller="ops/tools/run_paper_session_admission_v1.py",
    )
    day_utc = str(args.day_utc).strip()
    input_day_utc = str((args.input_day_utc or "").strip() or day_utc)
    producer_git_sha = _producer_git_sha()
    producer_repo = REPO_ROOT.name
    ib_account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    primary_sleeve_truth_root = _resolve_primary_paper_sleeve_truth_root(ib_account=ib_account)
    fresh_day_probe_path = resolve_next_day_readiness_probe_path(truth_root=truth_root, target_day_utc=day_utc)
    if not fresh_day_probe_path.exists() or not fresh_day_probe_path.is_file():
        fresh_day_probe_payload = derive_next_day_readiness_probe_payload(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            target_day_utc=day_utc,
            environment="PAPER",
            ib_account=ib_account,
        )
        write_next_day_readiness_probe_v1(truth_root=truth_root, payload=fresh_day_probe_payload)
    fresh_day_payload = derive_fresh_day_admission_payload(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        target_day_utc=day_utc,
        environment="PAPER",
        ib_account=ib_account,
    )
    fresh_day_ref = write_fresh_day_admission_v1(truth_root=truth_root, payload=fresh_day_payload)
    if str(fresh_day_payload.get("admission_status") or "").strip().upper() != "ADMIT":
        _print_payload(
            {
                "status": "FRESH_DAY_BLOCKED",
                "day_utc": day_utc,
                "input_day_utc": input_day_utc,
                "fresh_day_admission_path": str(fresh_day_ref.path),
                "fresh_day_admission_status": fresh_day_payload.get("admission_status"),
                "blocking_items": fresh_day_payload.get("blocking_items"),
                "missing_required_artifacts": fresh_day_payload.get("missing_required_artifacts"),
                "next_action": "materialize_and_green_required_target_day_admission_artifacts_before_execution",
            }
        )
        return 2
    operator_statement_path = resolve_operator_statement_path(operator_input_root=truth_root, day_utc=day_utc)
    market_data_symbols = _active_market_data_symbols()
    market_data_run_utc = f"{day_utc}T00:00:00Z"
    canonical_positions_snapshot_path = (
        truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json"
    ).resolve()
    canonical_input_positions_snapshot_path = (
        truth_root / "positions_v1" / "snapshots" / input_day_utc / "positions_snapshot.v2.json"
    ).resolve()
    sleeve_positions_snapshot_path = (
        primary_sleeve_truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json"
    ).resolve()
    sleeve_input_positions_snapshot_path = (
        primary_sleeve_truth_root / "positions_v1" / "snapshots" / input_day_utc / "positions_snapshot.v2.json"
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
    ib_client_id = _market_data_client_id()
    ib_sleep_sec = str(os.environ.get("C2_IB_SLEEP_SEC") or "0.1").strip()
    sleeve_nav_v2_path = (primary_sleeve_truth_root / "accounting_v2" / "nav" / day_utc / "nav.v2.json").resolve()
    sleeve_nav_compat_path = (
        primary_sleeve_truth_root / "accounting_compat_v1" / "nav" / day_utc / "nav_snapshot.v1.json"
    ).resolve()
    sleeve_reconciliation_report_path = (
        primary_sleeve_truth_root / "reports" / "reconciliation_report_v3" / day_utc / "reconciliation_report.v3.json"
    ).resolve()
    sleeve_allocation_summary_path = (
        primary_sleeve_truth_root / "allocation_v1" / "summary" / day_utc / "summary.json"
    ).resolve()
    sleeve_liquidity_slippage_gate_path = (
        primary_sleeve_truth_root / "reports" / "liquidity_slippage_gate_v1" / day_utc / "liquidity_slippage_gate.v1.json"
    ).resolve()
    sleeve_feed_attestation_gate_path = (
        primary_sleeve_truth_root / "reports" / "feed_attestation_gate_v1" / day_utc / "feed_attestation_gate.v1.json"
    ).resolve()
    sleeve_heartbeat_gate_path = (
        primary_sleeve_truth_root / "reports" / "heartbeat_gate_v1" / day_utc / "heartbeat_gate.v1.json"
    ).resolve()
    sleeve_replay_certification_gate_path = (
        primary_sleeve_truth_root / "reports" / "replay_certification_gate_v1" / day_utc / "replay_certification_gate.v1.json"
    ).resolve()

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
        "positions_snapshot_v2": _run_if_artifact_missing(
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
            artifact_path=canonical_positions_snapshot_path,
            artifact_id="positions_snapshot_v2",
            expected_day_utc=day_utc,
        ),
        "input_day_positions_snapshot_v2": _run(
            [
                sys.executable,
                "-m",
                POSITIONS_TOOL,
                "--day_utc",
                input_day_utc,
                "--producer_git_sha",
                producer_git_sha,
                "--producer_repo",
                producer_repo,
            ],
            truth_root=truth_root,
        ) if input_day_utc != day_utc else {
            "cmd": [],
            "returncode": 0,
            "stdout": "OK: input_day_positions_snapshot_v2 action=SKIP_SAME_DAY",
            "stderr": "",
        },
        "cash_ledger_snapshot_v1": _run_if_artifact_missing(
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
            artifact_path=canonical_cash_ledger_snapshot_path,
            artifact_id="cash_ledger_snapshot_v1",
            expected_day_utc=day_utc,
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
        "sleeve_input_positions_snapshot_seed_v2": _mirror_canonical_file(
            source_path=canonical_input_positions_snapshot_path,
            target_path=sleeve_input_positions_snapshot_path,
            artifact_id="sleeve_input_positions_snapshot_v2",
        ) if input_day_utc != day_utc else {
            "status": "SKIP_SAME_DAY",
            "artifact_id": "sleeve_input_positions_snapshot_v2",
            "source_path": str(canonical_input_positions_snapshot_path),
            "target_path": str(sleeve_input_positions_snapshot_path),
        },
        "sleeve_cash_ledger_snapshot_seed_v1": _mirror_canonical_file(
            source_path=canonical_cash_ledger_snapshot_path,
            target_path=sleeve_cash_ledger_snapshot_path,
            artifact_id="sleeve_cash_ledger_snapshot_v1",
        ),
        "sleeve_accounting_nav_v2": _run_if_artifact_missing(
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
            artifact_path=sleeve_nav_v2_path,
            artifact_id="sleeve_accounting_nav_v2",
            expected_day_utc=day_utc,
        ),
        "sleeve_accounting_nav_compat_v1": _run_if_artifact_missing(
            [
                sys.executable,
                str(ACCOUNTING_NAV_COMPAT_BRIDGE_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(primary_sleeve_truth_root),
            ],
            truth_root=primary_sleeve_truth_root,
            artifact_path=sleeve_nav_compat_path,
            artifact_id="sleeve_accounting_nav_compat_v1",
            expected_day_utc=day_utc,
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
        "sleeve_reconciliation_report_v3": _run_if_artifact_missing(
            [
                sys.executable,
                str(RECONCILIATION_REPORT_V3_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(primary_sleeve_truth_root),
            ],
            truth_root=primary_sleeve_truth_root,
            artifact_path=sleeve_reconciliation_report_path,
            artifact_id="sleeve_reconciliation_report_v3",
            expected_day_utc=day_utc,
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
        "sleeve_allocation_day_v2": _run_if_artifact_missing(
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
            artifact_path=sleeve_allocation_summary_path,
            artifact_id="sleeve_allocation_day_v2",
            expected_day_utc=day_utc,
        ),
        "sleeve_gate_liquidity_slippage_gate_v1": _run_if_artifact_missing(
            [sys.executable, str(LIQUIDITY_SLIPPAGE_GATE_TOOL), "--day_utc", day_utc],
            truth_root=primary_sleeve_truth_root,
            artifact_path=sleeve_liquidity_slippage_gate_path,
            artifact_id="sleeve_gate_liquidity_slippage_gate_v1",
            expected_day_utc=day_utc,
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
        "sleeve_gate_feed_attestation_gate_v1": _run_if_artifact_missing(
            [sys.executable, str(FEED_ATTESTATION_GATE_TOOL), "--day_utc", day_utc, "--truth_root", str(primary_sleeve_truth_root)],
            truth_root=primary_sleeve_truth_root,
            artifact_path=sleeve_feed_attestation_gate_path,
            artifact_id="sleeve_gate_feed_attestation_gate_v1",
            expected_day_utc=day_utc,
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
        "sleeve_gate_heartbeat_gate_v1": _run_if_artifact_missing(
            [sys.executable, str(HEARTBEAT_GATE_TOOL), "--day_utc", day_utc, "--truth_root", str(primary_sleeve_truth_root)],
            truth_root=primary_sleeve_truth_root,
            artifact_path=sleeve_heartbeat_gate_path,
            artifact_id="sleeve_gate_heartbeat_gate_v1",
            expected_day_utc=day_utc,
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
        "sleeve_gate_replay_certification_gate_v1": _run_if_artifact_missing(
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
            artifact_path=sleeve_replay_certification_gate_path,
            artifact_id="sleeve_gate_replay_certification_gate_v1",
            expected_day_utc=day_utc,
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
        "capability_state_v1": _run(
            [
                sys.executable,
                str(CAPABILITY_STATE_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--environment",
                "PAPER",
                "--ib_account",
                ib_account,
            ],
            truth_root=truth_root,
        ),
        "paper_policy_verdict_v1": _run(
            [
                sys.executable,
                str(PAPER_POLICY_VERDICT_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
            ],
            truth_root=truth_root,
        ),
        "production_policy_verdict_v1": _run(
            [
                sys.executable,
                str(PRODUCTION_POLICY_VERDICT_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--environment",
                "PAPER",
                "--ib_account",
                ib_account,
            ],
            truth_root=truth_root,
        ),
        "policy_diff_v1": _run(
            [
                sys.executable,
                str(POLICY_DIFF_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
            ],
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
        "fresh_day_admission_path": str(resolve_fresh_day_admission_path(truth_root=truth_root, target_day_utc=day_utc)),
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

    next_day_utc = (date.fromisoformat(day_utc) + timedelta(days=1)).isoformat()
    if missing_control_plane_artifacts:
        summary["status"] = "CONTROL_PLANE_INCOMPLETE"
        summary["next_action"] = "inspect_missing_current_day_control_plane_artifacts"
        _print_payload(summary)
        return 4

    next_day_payload = derive_next_day_readiness_probe_payload(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        target_day_utc=next_day_utc,
        environment="PAPER",
        ib_account=ib_account,
    )
    next_day_ref = write_next_day_readiness_probe_v1(truth_root=truth_root, payload=next_day_payload)
    summary["next_day_probe_path"] = str(next_day_ref.path)

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
        execution_rc = int(subprocess.call(cmd, cwd=str(REPO_ROOT)))
        execution_context = {
            "day_utc": day_utc,
            "release_id": str(resolve_release_provenance().get("release_id") or "").strip(),
            "git_sha": str(resolve_release_provenance().get("git_sha") or "").strip(),
            "entrypoint": "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
            "overall_exit_code": execution_rc,
            "generated_at_utc": f"{day_utc}T00:00:00Z",
            "runs": runs,
            "source_artifacts": [
                {
                    "artifact_family": "submit_boundary_status_v1",
                    "artifact_path": str(resolve_submit_boundary_status_path(truth_root=truth_root, day_utc=day_utc)),
                    "artifact_sha256": _sha256_file(resolve_submit_boundary_status_path(truth_root=truth_root, day_utc=day_utc)),
                },
                {
                    "artifact_family": "paper_policy_verdict_v1",
                    "artifact_path": str(resolve_paper_policy_verdict_path(truth_root=truth_root, day_utc=day_utc)),
                    "artifact_sha256": _sha256_file(resolve_paper_policy_verdict_path(truth_root=truth_root, day_utc=day_utc)),
                },
                {
                    "artifact_family": "production_policy_verdict_v1",
                    "artifact_path": str(resolve_production_policy_verdict_path(truth_root=truth_root, day_utc=day_utc)),
                    "artifact_sha256": _sha256_file(resolve_production_policy_verdict_path(truth_root=truth_root, day_utc=day_utc)),
                },
                {
                    "artifact_family": "policy_diff_v1",
                    "artifact_path": str(resolve_policy_diff_path(truth_root=truth_root, day_utc=day_utc)),
                    "artifact_sha256": _sha256_file(resolve_policy_diff_path(truth_root=truth_root, day_utc=day_utc)),
                },
                {
                    "artifact_family": "sleeve_rollup_v1",
                    "artifact_path": str((truth_root / "reports" / "sleeve_rollup_v1" / day_utc / "sleeve_rollup.v1.json").resolve()),
                    "artifact_sha256": _sha256_file((truth_root / "reports" / "sleeve_rollup_v1" / day_utc / "sleeve_rollup.v1.json").resolve()),
                },
                {
                    "artifact_family": "current_system_projection_v1",
                    "artifact_path": str(resolve_current_system_projection_path(truth_root=truth_root, day_utc=day_utc)),
                    "artifact_sha256": _sha256_file(resolve_current_system_projection_path(truth_root=truth_root, day_utc=day_utc)),
                },
                {
                    "artifact_family": "execution_journal_v1",
                    "artifact_path": str(resolve_execution_journal_path(truth_root=truth_root, day_utc=day_utc)),
                    "artifact_sha256": _sha256_file(resolve_execution_journal_path(truth_root=truth_root, day_utc=day_utc)),
                },
            ],
        }
        execution_payload = derive_execution_outcome_payload(truth_root=truth_root, context=execution_context)
        write_execution_outcome_v1(truth_root=truth_root, payload=execution_payload)
        return execution_rc

    summary["next_action"] = "execution_permitted_via_kernel_gated_entrypoint"
    execution_context = {
        "day_utc": day_utc,
        "release_id": str(resolve_release_provenance().get("release_id") or "").strip(),
        "git_sha": str(resolve_release_provenance().get("git_sha") or "").strip(),
        "entrypoint": "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
        "overall_exit_code": 0,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
        "runs": runs,
        "source_artifacts": [
            {
                "artifact_family": "submit_boundary_status_v1",
                "artifact_path": str(resolve_submit_boundary_status_path(truth_root=truth_root, day_utc=day_utc)),
                "artifact_sha256": _sha256_file(resolve_submit_boundary_status_path(truth_root=truth_root, day_utc=day_utc)),
            },
            {
                "artifact_family": "execution_journal_v1",
                "artifact_path": str(resolve_execution_journal_path(truth_root=truth_root, day_utc=day_utc)),
                "artifact_sha256": _sha256_file(resolve_execution_journal_path(truth_root=truth_root, day_utc=day_utc)),
            },
        ],
    }
    execution_payload = derive_execution_outcome_payload(truth_root=truth_root, context=execution_context)
    write_execution_outcome_v1(truth_root=truth_root, payload=execution_payload)
    _print_payload(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
