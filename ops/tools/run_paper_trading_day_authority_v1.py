#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry
from constellation_2.common.decision_authority_bridge_v1 import resolve_decision_truth_root_bridge_v1
from constellation_2.common.paper_day_manifest_v1 import (
    load_paper_day_manifest_v1,
    manifest_artifact_by_name_v1,
    manifest_artifacts_v1,
    render_manifest_path_template_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    atomic_write_idempotent_validated_json_v1,
    collect_intent_files_v1,
    now_utc_iso_v1,
    parse_day_utc_v1,
    producer_block_v1,
    read_json_object_v1,
    repo_git_sha_v1,
    resolve_paper_intent_truth_root_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_paper_trading_day_authority_path,
)
from constellation_2.common.paper_submit_mode_status_v1 import classify_paper_submit_mode_status_v1
from constellation_2.common.runtime_path_authority_v1 import require_authoritative_repo_runtime_v1
from constellation_2.common.sleeve_execution_root_v1 import resolve_sleeve_execution_root_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


OUTPUT_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_trading_day_authority.v1.schema.json"
)
CORRELATION_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/correlation_envelope_gate.v1.schema.json"
)
REPLAY_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/replay_certification_gate.v1.schema.json"
)
AUTHORIZATION_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/authorization_gate_verdict.v1.schema.json"
)
KILL_SWITCH_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/RISK/global_kill_switch_state.v1.schema.json"
)
PAPER_SESSION_AUTHORITY_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_authority.v1.schema.json"
)
SUBMIT_BOUNDARY_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/submit_boundary_status.v1.schema.json"
)
TRADING_DAY_CONTROL_PLANE_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_control_plane.v1.schema.json"
)
MARKET_DATA_AUTHORITY_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/market_data_authority.v1.schema.json"
)
STRATEGY_DECISION_AUTHORITY_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/strategy_decision_authority.v1.schema.json"
)
PORTFOLIO_ACCOUNT_AUTHORITY_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/portfolio_account_authority.v1.schema.json"
)
RISK_SIZING_AUTHORITY_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/risk_sizing_authority.v1.schema.json"
)
OPTIONS_CHAIN_SCHEMA = "constellation_2/schemas/options_chain_snapshot.v1.schema.json"
ACTIVE_SESSION_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/active_session.v1.schema.json"
)
AUTHORITY_SELF_OR_DOWNSTREAM_V1 = {
    "paper_trading_day_authority_v1",
    "submit_boundary_status_v1",
    "operator_gate_v1",
    "trading_day_control_plane_v1",
    "ui_dashboard_projection_v1",
    "chatgpt_packet_projection_v1",
}


def _read_json(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:path={path}")
    return payload


def _read_validated(path: Path, schema_relpath: str) -> Tuple[Dict[str, Any] | None, str]:
    if not path.exists() or not path.is_file():
        return None, "MISSING"
    try:
        payload = _read_json(path)
    except Exception as exc:  # noqa: BLE001
        return None, f"INVALID_JSON:{type(exc).__name__}"
    try:
        validate_against_repo_schema_v1(payload, REPO_ROOT, schema_relpath)
    except Exception as exc:  # noqa: BLE001
        return None, f"INVALID_SCHEMA:{type(exc).__name__}"
    return payload, ""


def _reason_codes(payload: Dict[str, Any] | None, *keys: str) -> List[str]:
    if not isinstance(payload, dict):
        return []
    out: List[str] = []
    for key in keys:
        values = payload.get(key)
        if not isinstance(values, list):
            continue
        for item in values:
            code = str(item or "").strip()
            if code:
                out.append(code)
    deduped: List[str] = []
    seen: set[str] = set()
    for code in out:
        up = code.upper()
        if up in seen:
            continue
        seen.add(up)
        deduped.append(code)
    return deduped


def _paper_session_submission_blockers(payload: Dict[str, Any] | None) -> List[str]:
    if not isinstance(payload, dict):
        return []
    if bool(payload.get("submission_authorized") is True):
        return []

    direct_codes = _reason_codes(payload, "blocking_reason_codes", "reason_codes")
    if direct_codes:
        return direct_codes

    upstream_refs = payload.get("upstream_refs")
    bootstrap_path = ""
    if isinstance(upstream_refs, dict):
        bootstrap_path = str(upstream_refs.get("paper_session_bootstrap_v1") or "").strip()

    bootstrap_payload: Dict[str, Any] = {}
    if bootstrap_path:
        try:
            bootstrap_payload = _read_json(Path(bootstrap_path).expanduser().resolve())
        except Exception:
            bootstrap_payload = {}

    runtime_verification = bootstrap_payload.get("runtime_prerequisite_verification")
    if isinstance(runtime_verification, dict):
        earliest = runtime_verification.get("earliest_failing_prerequisite")
        if isinstance(earliest, dict):
            prerequisite_id = str(earliest.get("prerequisite_id") or "").strip()
            mapped_code = {
                "target_day_admission_v1": "TARGET_DAY_ADMISSION_NOT_READY",
                "session_promotion_decision_v1": "SESSION_PROMOTION_DECISION_NOT_READY",
                "day_activation_readiness_v1": "DAY_ACTIVATION_NOT_READY",
            }.get(prerequisite_id)
            if mapped_code:
                extra_codes = [
                    str(code).strip()
                    for code in earliest.get("reason_codes") or []
                    if str(code).strip() and str(code).strip() != mapped_code
                ]
                return list(dict.fromkeys([mapped_code] + extra_codes + ["PAPER_SESSION_SUBMISSION_NOT_AUTHORIZED"]))

    specific_codes: List[str] = []
    session_bootstrap = bootstrap_payload.get("session_bootstrap")
    if isinstance(session_bootstrap, dict):
        promotion_gate = session_bootstrap.get("promotion_gate")
        if isinstance(promotion_gate, dict):
            promotion_state = str(promotion_gate.get("promotion_state") or promotion_gate.get("status") or "").strip().upper()
            if promotion_state == "BLOCKED":
                specific_codes.append("SESSION_PROMOTION_DECISION_NOT_READY")
            specific_codes.extend(_reason_codes(promotion_gate, "blocked_reason_codes", "reason_codes"))
        admission = session_bootstrap.get("admission")
        if isinstance(admission, dict):
            admission_status = str(admission.get("admission_status") or admission.get("status") or "").strip().upper()
            if admission_status == "BLOCKED":
                specific_codes.append("TARGET_DAY_ADMISSION_NOT_READY")
            specific_codes.extend(_reason_codes(admission, "blocking_reason_codes", "reason_codes"))

    activation_projection = bootstrap_payload.get("activation_phase")
    generic_codes: List[str] = []
    if isinstance(activation_projection, dict):
        first_real_blocker = str(activation_projection.get("first_real_blocker") or "").strip()
        if first_real_blocker:
            specific_codes.append(first_real_blocker)
        generic_codes.extend(
            str(code).strip()
            for code in activation_projection.get("blocker_chain") or []
            if str(code).strip()
        )

    deduped: List[str] = []
    seen: set[str] = set()
    for code in specific_codes + generic_codes + ["PAPER_SESSION_SUBMISSION_NOT_AUTHORIZED"]:
        text = str(code).strip()
        if not text:
            continue
        up = text.upper()
        if up in seen:
            continue
        seen.add(up)
        deduped.append(text)
    return deduped


def _first(items: List[str]) -> str:
    return items[0] if items else ""


def _operator_actionable(code: str) -> bool:
    upper = str(code or "").strip().upper()
    if not upper:
        return False
    if upper in {
        "C2_KILL_SWITCH_ACTIVE",
        "CANONICAL_KILL_SWITCH_ACTIVE",
        "NON_TRADING_DAY",
        "NO_ACTIVE_PAPER_SESSION",
    }:
        return False
    if "MISSING" in upper or "STALE" in upper or "INVALID" in upper:
        return True
    return True


def _build_input_status(
    *,
    logical_name: str,
    path: Path,
    payload: Dict[str, Any] | None,
    load_error: str,
    reason_codes: List[str],
    pass_statuses: Tuple[str, ...],
) -> Dict[str, Any]:
    exists = payload is not None
    if not exists:
        status = "MISSING" if load_error == "MISSING" else "INVALID"
        codes = [f"{logical_name.upper()}_{status}"]
        if load_error and load_error not in {"MISSING"}:
            codes.append(load_error)
        return {
            "path": str(path),
            "exists": False,
            "status": status,
            "reason_codes": codes,
        }
    observed_status = str(
        payload.get("status")
        or payload.get("state")
        or payload.get("authority_status")
        or payload.get("boundary_status")
        or payload.get("final_start_decision")
        or ""
    ).strip().upper()
    if observed_status in pass_statuses:
        return {
            "path": str(path),
            "exists": True,
            "status": "PASS",
            "reason_codes": [],
        }
    return {
        "path": str(path),
        "exists": True,
        "status": "FAIL",
        "reason_codes": reason_codes or [f"{logical_name.upper()}_NOT_PASS"],
    }


def _producer_commands(*, day_utc: str, truth_root: Path, execution_truth_root: Path) -> Dict[str, str]:
    produced_utc = f"{day_utc}T00:00:00Z"
    return {
        "correlation_envelope_gate_v1": (
            f"python3 ops/tools/run_correlation_envelope_gate_v1.py --day_utc {day_utc} "
            f"--truth_root {execution_truth_root} --produced_utc {produced_utc} --mode PAPER"
        ),
        "replay_certification_gate_v1": (
            f"python3 ops/tools/run_replay_certification_gate_v1.py --day_utc {day_utc} "
            f"--truth_root {execution_truth_root} --produced_utc {produced_utc} --mode PAPER"
        ),
        "authorization_gate_verdict_v1": (
            f"python3 ops/tools/run_gate_authority_plane_v1.py --day_utc {day_utc} "
            f"--truth_root {truth_root} --produced_utc {produced_utc} --mode PAPER"
        ),
        "global_kill_switch_state_v1": (
            f"python3 ops/tools/run_global_kill_switch_v1.py --day_utc {day_utc}"
        ),
        "paper_session_authority_v1": (
            f"python3 ops/tools/run_paper_session_bootstrap_v1.py --day_utc {day_utc} "
            f"--truth_root {truth_root} --materialize YES --emit_report YES"
        ),
        "submit_boundary_status_v1": (
            f"python3 ops/tools/run_submit_boundary_status_v1.py --day_utc {day_utc} --truth_root {truth_root}"
        ),
        "trading_day_control_plane_v1": (
            f"python3 ops/tools/run_trading_day_control_plane_v1.py --day_utc {day_utc} --truth_root {truth_root}"
        ),
        "market_data_authority_v1": (
            f"python3 ops/tools/run_market_data_authority_v1.py --day_utc {day_utc} "
            f"--truth_root {truth_root} --execution_root {execution_truth_root}"
        ),
        "strategy_decision_authority_v1": (
            f"python3 ops/tools/run_strategy_decision_authority_v1.py --day_utc {day_utc} "
            f"--truth_root {truth_root} --execution_root {execution_truth_root}"
        ),
        "portfolio_account_authority_v1": (
            f"python3 ops/tools/run_portfolio_account_authority_v1.py --day_utc {day_utc} "
            f"--truth_root {truth_root} --execution_root {execution_truth_root}"
        ),
        "risk_sizing_authority_v1": (
            f"python3 ops/tools/run_risk_sizing_authority_v1.py --day_utc {day_utc} "
            f"--truth_root {truth_root} --execution_root {execution_truth_root}"
        ),
    }


def _render_manifest_command(command: str, *, day_utc: str, truth_root: Path, execution_truth_root: Path) -> str:
    return str(command or "").format(
        day_utc=day_utc,
        canonical_truth_root=str(truth_root),
        execution_truth_root=str(execution_truth_root),
        repo_root=str(REPO_ROOT),
        state_root=str((Path.home() / ".local/state/constellation_2").resolve()),
    )


def _manifest_producer_commands(
    *,
    manifest: Dict[str, Any],
    day_utc: str,
    truth_root: Path,
    execution_truth_root: Path,
) -> Dict[str, str]:
    commands: Dict[str, str] = {}
    for item in manifest_artifacts_v1(manifest):
        name = str(item.get("artifact_name") or "").strip()
        raw = str(item.get("producer_command") or item.get("producer_function") or "").strip()
        if not name or not raw:
            continue
        commands[name] = _render_manifest_command(
            raw,
            day_utc=day_utc,
            truth_root=truth_root,
            execution_truth_root=execution_truth_root,
        )
    return commands


def _same_day_payload(payload: Dict[str, Any] | None, day_utc: str) -> bool:
    if not isinstance(payload, dict):
        return False
    observed = str(
        payload.get("day_utc")
        or payload.get("target_day")
        or payload.get("day")
        or payload.get("active_day")
        or ""
    ).strip()
    return not observed or observed == day_utc


def _active_option_symbols(*, truth_root: Path, day_utc: str) -> List[str]:
    intent_truth_root = resolve_paper_intent_truth_root_v1(truth_root=truth_root, repo_root=REPO_ROOT)
    symbols: List[str] = []
    for path in collect_intent_files_v1(truth_root=intent_truth_root, day_utc=day_utc):
        try:
            payload = read_json_object_v1(path)
        except ValueError:
            continue
        option = payload.get("option")
        exposure_type = str(payload.get("exposure_type") or "").strip().upper()
        has_option_structure = isinstance(option, dict) or exposure_type in {"SHORT_VOL_DEFINED", "VOL_INCOME_DEFINED"}
        if not has_option_structure:
            continue
        underlying = payload.get("underlying")
        symbol = ""
        if isinstance(underlying, dict):
            symbol = str(underlying.get("symbol") or "").strip().upper()
        elif isinstance(underlying, str):
            symbol = underlying.strip().upper()
        if symbol:
            symbols.append(symbol)
    return sorted(set(symbols))


def _options_snapshot_status(*, path: Path, day_utc: str, required_symbols: List[str]) -> Dict[str, Any]:
    root = path.resolve()
    if not required_symbols:
        return {
            "path": str(root),
            "exists": root.exists(),
            "status": "PASS",
            "reason_codes": [],
            "required_symbols": [],
            "covered_symbols": [],
        }
    if not root.exists() or not root.is_dir():
        return {
            "path": str(root),
            "exists": False,
            "status": "MISSING",
            "reason_codes": ["OPTIONS_SNAPSHOT_ROOT_MISSING"],
            "required_symbols": required_symbols,
            "covered_symbols": [],
        }

    covered: set[str] = set()
    failures: List[str] = []
    for capture_dir in sorted(root.iterdir()):
        snap_path = (capture_dir / "options_chain_snapshot.v1.json").resolve()
        cert_path = (capture_dir / "freshness_certificate.v1.json").resolve()
        if not snap_path.exists() or not snap_path.is_file():
            continue
        if not cert_path.exists() or not cert_path.is_file():
            failures.append("OPTIONS_SNAPSHOT_FRESHNESS_CERT_MISSING")
            continue
        try:
            snap_obj = _read_json(snap_path)
            validate_against_repo_schema_v1(snap_obj, REPO_ROOT, OPTIONS_CHAIN_SCHEMA)
        except Exception as exc:  # noqa: BLE001
            failures.append(f"OPTIONS_SNAPSHOT_INVALID:{type(exc).__name__}")
            continue
        if not str(snap_obj.get("as_of_utc") or "").startswith(day_utc):
            failures.append("OPTIONS_SNAPSHOT_STALE")
            continue
        contracts = snap_obj.get("contracts")
        if not isinstance(contracts, list) or not contracts:
            failures.append("OPTIONS_SNAPSHOT_CONTRACTS_EMPTY")
            continue
        underlying = snap_obj.get("underlying") if isinstance(snap_obj.get("underlying"), dict) else {}
        symbol = str(underlying.get("symbol") or "").strip().upper()
        if symbol:
            covered.add(symbol)

    missing_symbols = sorted(set(required_symbols) - covered)
    if missing_symbols:
        return {
            "path": str(root),
            "exists": True,
            "status": "FAIL",
            "reason_codes": failures or ["OPTIONS_SNAPSHOT_SYMBOL_MISSING"],
            "required_symbols": required_symbols,
            "covered_symbols": sorted(covered),
        }
    return {
        "path": str(root),
        "exists": True,
        "status": "PASS",
        "reason_codes": [],
        "required_symbols": required_symbols,
        "covered_symbols": sorted(covered),
    }


def _dependency_newer(path: Path, dependency_paths: List[Path]) -> str:
    try:
        artifact_mtime = path.stat().st_mtime
    except Exception:
        return ""
    for dep_path in dependency_paths:
        try:
            if dep_path.exists() and dep_path.stat().st_mtime > artifact_mtime + 0.001:
                return str(dep_path)
        except Exception:
            continue
    return ""


def _enrich_input_status_from_manifest(
    *,
    input_status: Dict[str, Dict[str, Any]],
    manifest: Dict[str, Any],
    input_paths: Dict[str, Path],
    day_utc: str,
    producer_commands: Dict[str, str],
) -> None:
    artifact_map = manifest_artifact_by_name_v1(manifest)
    for name, row in input_status.items():
        item = artifact_map.get(name) or {}
        deps = [
            input_paths[str(dep)]
            for dep in item.get("dependencies", [])
            if str(dep) in input_paths
        ]
        role = str(item.get("readiness_role") or "").strip().lower()
        stale_parent = "" if role == "projection" else (_dependency_newer(input_paths[name], deps) if name in input_paths else "")
        row["required_or_diagnostic"] = str(item.get("required_or_diagnostic") or "diagnostic")
        row["readiness_role"] = str(item.get("readiness_role") or "")
        row["owning_subsystem"] = str(item.get("owning_subsystem") or name)
        row["producer_command"] = producer_commands.get(name, "")
        row["causal_parent"] = stale_parent
        if stale_parent and row.get("status") == "PASS":
            row["status"] = "STALE"
            row["reason_codes"] = [str(item.get("blocker_code_if_stale") or f"{name.upper()}_STALE")]
        row["freshness_status"] = "STALE" if stale_parent else "CURRENT"
        if row.get("status") == "INVALID":
            row["schema_status"] = "INVALID"
        elif row.get("exists"):
            row["schema_status"] = "PASS"
        else:
            row["schema_status"] = "UNKNOWN"



def _active_session_open_for_day(truth_root: Path, day_utc: str) -> bool:
    path = (truth_root / "active_session_v1" / "current.json").resolve()
    payload, error = _read_validated(path, ACTIVE_SESSION_SCHEMA)
    if payload is None or error:
        return False
    active_day = str(payload.get("active_day") or "").strip()
    rollover = str(payload.get("rollover_status") or "").strip().upper()
    return active_day == day_utc and rollover != "ROLLOVER_WITHHELD"


def main(argv: List[str] | None = None) -> int:
    require_authoritative_repo_runtime_v1(REPO_ROOT)
    ap = argparse.ArgumentParser(prog="run_paper_trading_day_authority_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_decision_truth_root_bridge_v1(
        args.truth_root or "",
        repo_root=REPO_ROOT,
        caller="ops/tools/run_paper_trading_day_authority_v1.py",
    )
    paper_account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    execution_truth_root = resolve_sleeve_execution_root_v1(
        repo_root=REPO_ROOT,
        environment="PAPER",
        ib_account=paper_account,
        sleeve_id="PRIMARY",
    ).execution_root_path.resolve()
    produced_at_utc = now_utc_iso_v1()
    manifest = load_paper_day_manifest_v1(REPO_ROOT)
    state_root = (Path.home() / ".local/state/constellation_2").resolve()
    producer_commands = _producer_commands(
        day_utc=day_utc,
        truth_root=truth_root,
        execution_truth_root=execution_truth_root,
    )
    producer_commands.update(
        _manifest_producer_commands(
            manifest=manifest,
            day_utc=day_utc,
            truth_root=truth_root,
            execution_truth_root=execution_truth_root,
        )
    )
    input_paths = {
        str(item.get("artifact_name") or "").strip(): render_manifest_path_template_v1(
            str(item.get("path_template") or ""),
            repo_root=REPO_ROOT,
            canonical_truth_root=truth_root,
            execution_truth_root=execution_truth_root,
            state_root=state_root,
            day_utc=day_utc,
        )
        for item in manifest_artifacts_v1(manifest)
        if str(item.get("artifact_name") or "").strip()
    }
    evidence_paths = {key: str(path) for key, path in input_paths.items()}
    required_option_symbols = _active_option_symbols(truth_root=execution_truth_root, day_utc=day_utc)

    corr_payload, corr_error = _read_validated(input_paths["correlation_envelope_gate_v1"], CORRELATION_SCHEMA)
    replay_payload, replay_error = _read_validated(input_paths["replay_certification_gate_v1"], REPLAY_SCHEMA)
    auth_payload, auth_error = _read_validated(input_paths["authorization_gate_verdict_v1"], AUTHORIZATION_SCHEMA)
    kill_payload, kill_error = _read_validated(input_paths["global_kill_switch_state_v1"], KILL_SWITCH_SCHEMA)
    session_payload, session_error = _read_validated(
        input_paths["paper_session_authority_v1"],
        PAPER_SESSION_AUTHORITY_SCHEMA,
    )
    boundary_payload, boundary_error = _read_validated(
        input_paths["submit_boundary_status_v1"],
        SUBMIT_BOUNDARY_SCHEMA,
    )
    control_payload, control_error = _read_validated(
        input_paths["trading_day_control_plane_v1"],
        TRADING_DAY_CONTROL_PLANE_SCHEMA,
    )
    market_data_authority_path = (
        truth_root
        / "reports"
        / "market_data_authority_v1"
        / day_utc
        / "market_data_authority.v1.json"
    ).resolve()
    market_data_payload, market_data_error = _read_validated(
        market_data_authority_path,
        MARKET_DATA_AUTHORITY_SCHEMA,
    )
    strategy_authority_path = truth_root / "reports" / "strategy_decision_authority_v1" / day_utc / "strategy_decision_authority.v1.json"
    portfolio_authority_path = truth_root / "reports" / "portfolio_account_authority_v1" / day_utc / "portfolio_account_authority.v1.json"
    risk_sizing_authority_path = truth_root / "reports" / "risk_sizing_authority_v1" / day_utc / "risk_sizing_authority.v1.json"
    strategy_payload, strategy_error = _read_validated(strategy_authority_path, STRATEGY_DECISION_AUTHORITY_SCHEMA)
    portfolio_payload, portfolio_error = _read_validated(portfolio_authority_path, PORTFOLIO_ACCOUNT_AUTHORITY_SCHEMA)
    risk_sizing_payload, risk_sizing_error = _read_validated(risk_sizing_authority_path, RISK_SIZING_AUTHORITY_SCHEMA)

    corr_codes = _reason_codes(corr_payload, "reason_codes")
    replay_codes = _reason_codes(replay_payload, "reason_codes")
    auth_codes = _reason_codes(auth_payload, "reason_codes")
    kill_codes = _reason_codes(kill_payload, "reason_codes")
    session_codes = _reason_codes(session_payload, "blocking_reason_codes", "reason_codes")
    boundary_codes = _reason_codes(boundary_payload, "blocking_codes", "reason_codes")
    control_codes = _reason_codes(control_payload, "blocking_codes")

    input_status = {
        "correlation_envelope_gate_v1": _build_input_status(
            logical_name="correlation_envelope_gate_v1",
            path=input_paths["correlation_envelope_gate_v1"],
            payload=corr_payload,
            load_error=corr_error,
            reason_codes=corr_codes,
            pass_statuses=("PASS",),
        ),
        "replay_certification_gate_v1": _build_input_status(
            logical_name="replay_certification_gate_v1",
            path=input_paths["replay_certification_gate_v1"],
            payload=replay_payload,
            load_error=replay_error,
            reason_codes=replay_codes,
            pass_statuses=("PASS",),
        ),
        "authorization_gate_verdict_v1": _build_input_status(
            logical_name="authorization_gate_verdict_v1",
            path=input_paths["authorization_gate_verdict_v1"],
            payload=auth_payload,
            load_error=auth_error,
            reason_codes=auth_codes,
            pass_statuses=("PASS", "BOOTSTRAP_PASS"),
        ),
        "global_kill_switch_state_v1": _build_input_status(
            logical_name="global_kill_switch_state_v1",
            path=input_paths["global_kill_switch_state_v1"],
            payload=kill_payload,
            load_error=kill_error,
            reason_codes=kill_codes,
            pass_statuses=("INACTIVE",),
        ),
        "options_chain_snapshot_v1": _options_snapshot_status(
            path=input_paths["options_chain_snapshot_v1"],
            day_utc=day_utc,
            required_symbols=required_option_symbols,
        ),
        "paper_session_authority_v1": _build_input_status(
            logical_name="paper_session_authority_v1",
            path=input_paths["paper_session_authority_v1"],
            payload=session_payload,
            load_error=session_error,
            reason_codes=session_codes,
            pass_statuses=("GRANTED", "AUTHORIZED"),
        ),
        "submit_boundary_status_v1": _build_input_status(
            logical_name="submit_boundary_status_v1",
            path=input_paths["submit_boundary_status_v1"],
            payload=boundary_payload,
            load_error=boundary_error,
            reason_codes=boundary_codes,
            pass_statuses=("AUTHORIZED",),
        ),
        "trading_day_control_plane_v1": _build_input_status(
            logical_name="trading_day_control_plane_v1",
            path=input_paths["trading_day_control_plane_v1"],
            payload=control_payload,
            load_error=control_error,
            reason_codes=control_codes,
            pass_statuses=("READY_NOW",),
        ),
    }
    session_authority_status = (
        str((session_payload or {}).get("authority_status") or "").strip().upper()
        if isinstance(session_payload, dict)
        else ""
    )
    session_authority_denied = bool(session_authority_status == "DENIED")
    if session_authority_denied:
        session_detail_codes = _paper_session_submission_blockers(session_payload)
        input_status["paper_session_authority_v1"]["status"] = "FAIL"
        input_status["paper_session_authority_v1"]["reason_codes"] = list(
            dict.fromkeys(["SESSION_AUTHORITY_DENIED"] + session_detail_codes)
        )
    if control_payload is not None and not control_error:
        control_decision = str(control_payload.get("final_start_decision") or "").strip().upper()
        if control_decision == "READY_NOW":
            input_status["trading_day_control_plane_v1"]["status"] = "PASS"
            input_status["trading_day_control_plane_v1"]["reason_codes"] = []
    if boundary_payload is not None and not boundary_error:
        boundary_authorized_now = (
            str(boundary_payload.get("boundary_status") or "").strip().upper() == "AUTHORIZED"
            and bool(boundary_payload.get("submission_authorized") is True)
        )
        if boundary_authorized_now:
            input_status["submit_boundary_status_v1"]["status"] = "PASS"
            input_status["submit_boundary_status_v1"]["reason_codes"] = []
    if session_payload is not None and not session_error:
        session_authority_open = (
            str(session_payload.get("authority_status") or "").strip().upper() in {"GRANTED", "AUTHORIZED"}
        )
        if session_authority_open and not bool(session_payload.get("submission_authorized") is True):
            input_status["paper_session_authority_v1"]["status"] = "FAIL"
            input_status["paper_session_authority_v1"]["reason_codes"] = _paper_session_submission_blockers(session_payload)

    artifact_map = manifest_artifact_by_name_v1(manifest)
    for logical_name, path in input_paths.items():
        if logical_name in input_status:
            continue
        item = artifact_map.get(logical_name) or {}
        if not path.exists() or not path.is_file():
            input_status[logical_name] = {
                "path": str(path),
                "exists": False,
                "status": "MISSING",
                "reason_codes": [str(item.get("blocker_code_if_missing") or f"{logical_name.upper()}_MISSING")],
            }
            continue
        if logical_name in {"ui_dashboard_projection_v1", "chatgpt_packet_projection_v1"}:
            input_status[logical_name] = {
                "path": str(path),
                "exists": True,
                "status": "PASS",
                "reason_codes": [],
            }
            continue
        payload: Dict[str, Any] | None
        try:
            payload = _read_json(path)
        except Exception as exc:  # noqa: BLE001
            input_status[logical_name] = {
                "path": str(path),
                "exists": False,
                "status": "INVALID",
                "reason_codes": [str(item.get("blocker_code_if_failing") or f"{logical_name.upper()}_INVALID"), type(exc).__name__],
            }
            continue
        if logical_name == "target_day_build_v1":
            ok = (
                str(payload.get("build_status") or "").strip().upper() == "COMPLETE"
                and str(payload.get("completeness_result") or "").strip().upper() == "COMPLETE"
            )
        elif logical_name == "target_day_admission_v1":
            ok = (
                str(payload.get("admission_status") or "").strip().upper() == "ADMIT"
                and bool(payload.get("binding") is True)
            )
        elif logical_name == "session_promotion_decision_v1":
            ok = str(payload.get("promotion_state") or "").strip().upper() == "PROMOTED"
        elif logical_name == "operator_gate_v1":
            ok = str(payload.get("status") or "").strip().upper() == "PASS"
        else:
            ok = _same_day_payload(payload, day_utc)
        input_status[logical_name] = {
            "path": str(path),
            "exists": True,
            "status": "PASS" if ok else "FAIL",
            "reason_codes": [] if ok else [str(item.get("blocker_code_if_failing") or f"{logical_name.upper()}_FAIL")],
        }

    # Normalize kill-switch status to reflect authority semantics.
    if kill_payload is not None and not kill_error:
        kill_state = str(kill_payload.get("state") or "").strip().upper()
        kill_allow_entries = bool(kill_payload.get("allow_entries") is True)
        if kill_state == "INACTIVE" and kill_allow_entries:
            input_status["global_kill_switch_state_v1"]["status"] = "PASS"
            input_status["global_kill_switch_state_v1"]["reason_codes"] = []
        else:
            input_status["global_kill_switch_state_v1"]["status"] = "FAIL"
            input_status["global_kill_switch_state_v1"]["reason_codes"] = (
                kill_codes or ["C2_KILL_SWITCH_ACTIVE"]
            )

    _enrich_input_status_from_manifest(
        input_status=input_status,
        manifest=manifest,
        input_paths=input_paths,
        day_utc=day_utc,
        producer_commands=producer_commands,
    )
    if "options_chain_snapshot_v1" in input_status:
        options_row = input_status["options_chain_snapshot_v1"]
        options_item = artifact_map.get("options_chain_snapshot_v1") or {}
        options_row["condition"] = str(options_item.get("condition") or "")
        options_required = bool(required_option_symbols) and not session_authority_denied
        options_row["required_or_diagnostic"] = "required" if options_required else "diagnostic"
        options_row["readiness_role"] = "authority_input" if options_required else "diagnostic"
        options_row["required_symbols"] = required_option_symbols
        options_row["covered_symbols"] = list(options_row.get("covered_symbols") or [])
    if market_data_payload is not None and not market_data_error:
        market_status = str(market_data_payload.get("status") or "").strip().upper()
        market_state = str(market_data_payload.get("market_data_state") or "").strip().upper()
        operator_impact = str(market_data_payload.get("operator_impact") or "").strip().upper()
        market_blocker = str(market_data_payload.get("first_blocker") or "").strip()
        market_row_status = "PASS" if market_status == "PASS" else ("WARN" if market_status == "WARN" else "FAIL")
        market_required = bool(operator_impact == "PRE_SUBMIT_BLOCKER" and not session_authority_denied)
        input_status["market_data_authority_v1"] = {
            "path": str(market_data_authority_path),
            "exists": True,
            "status": market_row_status,
            "reason_codes": [] if market_row_status in {"PASS", "WARN"} else [market_blocker or f"MARKET_DATA_{market_state}"],
            "required_or_diagnostic": "required" if market_required else "diagnostic",
            "readiness_role": "authority_input" if market_required else "diagnostic",
            "owning_subsystem": "market_data_authority_v1",
            "producer_command": producer_commands.get("market_data_authority_v1", ""),
            "causal_parent": "",
            "freshness_status": "STALE" if market_state == "STALE" else "CURRENT",
            "schema_status": "PASS",
            "market_data_state": market_state,
            "operator_impact": operator_impact,
        }
    post_submit_dry_run = bool(
        boundary_payload is not None
        and str(boundary_payload.get("submit_mode_status") or boundary_payload.get("boundary_status") or "").strip().upper() == "DRY_RUN_COMPLETE"
    )

    def add_direct_authority_row(
        *,
        logical_name: str,
        path: Path,
        payload: Dict[str, Any] | None,
        load_error: str,
        state_key: str,
    ) -> None:
        if payload is None:
            return
        observed_status = str(payload.get("status") or "").strip().upper()
        observed_state = str(payload.get(state_key) or "").strip().upper()
        row_status = "PASS" if observed_status == "PASS" else ("WARN" if observed_status == "WARN" else "FAIL")
        diagnostic = bool(post_submit_dry_run or row_status == "WARN")
        input_status[logical_name] = {
            "path": str(path),
            "exists": True,
            "status": row_status,
            "reason_codes": [] if row_status in {"PASS", "WARN"} else [str(payload.get("first_blocker") or f"{logical_name.upper()}_{observed_state}")],
            "required_or_diagnostic": "diagnostic" if diagnostic else "required",
            "readiness_role": "diagnostic" if diagnostic else "authority_input",
            "owning_subsystem": logical_name,
            "producer_command": producer_commands.get(logical_name, ""),
            "causal_parent": "",
            "freshness_status": "CURRENT",
            "schema_status": "PASS" if not load_error else "INVALID",
            state_key: observed_state,
        }

    add_direct_authority_row(logical_name="strategy_decision_authority_v1", path=strategy_authority_path, payload=strategy_payload, load_error=strategy_error, state_key="strategy_decision_state")
    add_direct_authority_row(logical_name="portfolio_account_authority_v1", path=portfolio_authority_path, payload=portfolio_payload, load_error=portfolio_error, state_key="account_state")
    add_direct_authority_row(logical_name="risk_sizing_authority_v1", path=risk_sizing_authority_path, payload=risk_sizing_payload, load_error=risk_sizing_error, state_key="risk_sizing_state")

    preflight_inputs = tuple(
        name
        for name, row in input_status.items()
        if str(row.get("required_or_diagnostic") or "").strip().lower() == "required"
        and str(row.get("readiness_role") or "").strip().lower() == "authority_input"
    )
    canonical_authority_inputs = set(preflight_inputs)
    preflight_missing = [
        logical_name
        for logical_name in preflight_inputs
        if input_status[logical_name]["status"] in {"MISSING", "INVALID", "STALE"}
    ]
    preflight_failing = [
        logical_name
        for logical_name in preflight_inputs
        if input_status[logical_name]["status"] == "FAIL"
    ]

    missing_or_stale_inputs: List[Dict[str, Any]] = []
    blocker_tree: List[Dict[str, Any]] = []
    reason_codes: List[str] = []

    for logical_name, row in input_status.items():
        row_status = str(row.get("status") or "").strip().upper()
        row_codes = [str(code).strip() for code in row.get("reason_codes") or [] if str(code).strip()]
        if logical_name not in canonical_authority_inputs:
            continue
        manifest_item = artifact_map.get(logical_name) or {}
        owner = str(manifest_item.get("owning_subsystem") or logical_name)
        producer_command = producer_commands.get(logical_name, "")
        causal_parent = str(row.get("causal_parent") or "")
        if row_status in {"MISSING", "INVALID", "STALE"}:
            stale_kind = "STALE" if row_status == "STALE" else ("MISSING" if row_status == "MISSING" else "INVALID")
            missing_or_stale_inputs.append(
                {
                    "logical_name": logical_name,
                    "path": str(row.get("path") or ""),
                    "missing_or_stale": stale_kind,
                    "owning_gate": owner,
                    "required_producer_command": producer_command,
                    "operator_actionable": bool(manifest_item.get(f"operator_actionable_if_{stale_kind.lower()}") is not False),
                    "required_or_diagnostic": str(manifest_item.get("required_or_diagnostic") or "required"),
                    "causal_parent": causal_parent,
                }
            )
        if row_status in {"FAIL", "MISSING", "INVALID", "STALE"}:
            if row_status == "MISSING":
                default_code = str(manifest_item.get("blocker_code_if_missing") or f"{logical_name.upper()}_MISSING")
            elif row_status == "STALE":
                default_code = str(manifest_item.get("blocker_code_if_stale") or f"{logical_name.upper()}_STALE")
            else:
                default_code = str(manifest_item.get("blocker_code_if_failing") or f"{logical_name.upper()}_{row_status}")
            code = _first(row_codes) or default_code
            reason_codes.append(code)
            blocker_tree.append(
                {
                    "code": code,
                    "blocker_class": "REQUIRED_INPUT",
                    "summary": f"{logical_name} status={row_status}",
                    "logical_name": logical_name,
                    "owning_gate": owner,
                    "source_path": str(row.get("path") or ""),
                    "required_producer_command": producer_command,
                    "operator_actionable": _operator_actionable(code),
                    "causal_parent": causal_parent,
                }
            )

    state = "UNKNOWN"
    status = "NOT_READY"
    can_paper_trade_today = False
    can_submit_paper_orders = False
    submit_mode_status = "NO_SUBMIT_ATTEMPT"
    dry_run_policy = "UNKNOWN"
    broker_transmit_enabled: bool | None = None
    broker_order_transmitted = False
    missing_broker_ids_blocker = False
    missing_broker_ids_diagnostic = False
    submit_mode = classify_paper_submit_mode_status_v1(
        execution_root=execution_truth_root,
        day_utc=day_utc,
    )
    submit_mode_status = str(submit_mode.get("submit_mode_status") or "NO_SUBMIT_ATTEMPT")
    dry_run_policy = str(submit_mode.get("dry_run_policy") or "UNKNOWN")
    broker_transmit_enabled = submit_mode.get("broker_transmit_enabled")
    broker_order_transmitted = bool(submit_mode.get("broker_order_transmitted") is True)
    missing_broker_ids_blocker = bool(submit_mode.get("missing_broker_ids_blocker") is True)
    missing_broker_ids_diagnostic = bool(submit_mode.get("missing_broker_ids_diagnostic") is True)
    dry_run_complete = submit_mode_status.upper() == "DRY_RUN_COMPLETE"
    terminal_submit_evidence = bool(dry_run_complete or broker_order_transmitted)

    kill_switch_active = bool(
        input_status["global_kill_switch_state_v1"]["status"] == "FAIL"
    )
    authority_granted = bool(
        session_payload is not None
        and str(session_payload.get("authority_status") or "").strip().upper() in {"GRANTED", "AUTHORIZED"}
        and bool(session_payload.get("submission_authorized") is True)
    )

    if terminal_submit_evidence:
        state = "CLOSED"
        can_paper_trade_today = True
        can_submit_paper_orders = False
    elif preflight_missing:
        state = "PREFLIGHT_REQUIRED"
    elif kill_switch_active:
        state = "INVALIDATED"
    elif preflight_failing:
        state = "PREFLIGHT_BLOCKED"
    else:
        # Under the manifest architecture, active_session_v1 is not an undeclared
        # readiness veto. Same-day paper_session_authority_v1 is the required
        # authority input that represents session open/authorization.
        session_open = authority_granted
        submission_root = (
            execution_truth_root / "execution_evidence_v1" / "submissions" / day_utc
        ).resolve()
        has_submission_evidence = bool(
            submission_root.exists()
            and submission_root.is_dir()
            and any(item.is_dir() for item in submission_root.iterdir())
        )
        boundary_authorized = bool(
            boundary_payload is not None
            and str(boundary_payload.get("boundary_status") or "").strip().upper() == "AUTHORIZED"
            and bool(boundary_payload.get("submission_authorized") is True)
        )
        control_decision = (
            str(control_payload.get("final_start_decision") or "").strip().upper()
            if isinstance(control_payload, dict)
            else ""
        )

        if control_decision == "CLOSED":
            state = "CLOSED"
            can_paper_trade_today = False
        elif authority_granted and session_open and has_submission_evidence:
            state = "SUBMITTING"
            can_paper_trade_today = True
        elif authority_granted and session_open:
            state = "OPEN_READY"
            can_paper_trade_today = True
            can_submit_paper_orders = True
        elif authority_granted and not session_open:
            state = "AUTHORIZED_NOT_OPEN"
            can_paper_trade_today = True
        elif boundary_authorized and session_open:
            # Boundary may be stale/lagging; authority remains conservative.
            state = "AUTHORIZED_NOT_OPEN"
            can_paper_trade_today = True
        else:
            state = "PREFLIGHT_BLOCKED"

    if state == "OPEN_READY":
        status = "READY"
    else:
        status = "NOT_READY"

    # Canonical blocker selection with precedence toward safety controls.
    canonical_blocker = ""
    preferred_codes = []
    if kill_switch_active:
        preferred_codes.extend(input_status["global_kill_switch_state_v1"]["reason_codes"])
    if dry_run_complete:
        preferred_codes.append("DRY_RUN_COMPLETE_ALREADY_SUBMITTED")
    elif broker_order_transmitted:
        preferred_codes.append("BROKER_ORDER_ALREADY_TRANSMITTED")
    preferred_codes.extend(reason_codes)
    if not preferred_codes and state == "AUTHORIZED_NOT_OPEN":
        preferred_codes = session_codes or ["NO_ACTIVE_PAPER_SESSION"]
    canonical_blocker = _first([str(code).strip() for code in preferred_codes if str(code).strip()])
    if state == "CLOSED" and canonical_blocker:
        terminal_source_path = str(
            submit_mode.get("broker_submission_record_path")
            or resolve_paper_trading_day_authority_path(truth_root=truth_root, day_utc=day_utc)
        )
        blocker_tree.insert(
            0,
            {
                "code": canonical_blocker,
                "blocker_class": "TERMINAL_SUBMISSION_EVIDENCE",
                "summary": f"paper submit mode status={submit_mode_status}",
                "logical_name": "paper_submit_mode_status_v1",
                "owning_gate": "paper_submit_mode_status_v1",
                "source_path": terminal_source_path,
                "required_producer_command": f"npm run aegis:paper:submit -- --day_utc {day_utc}",
                "operator_actionable": False,
                "causal_parent": "",
            },
        )

    # Dedup reason codes/blocker rows.
    dedup_reasons: List[str] = []
    seen_reason: set[str] = set()
    for code in [canonical_blocker] + reason_codes:
        text = str(code).strip()
        if not text:
            continue
        up = text.upper()
        if up in seen_reason:
            continue
        seen_reason.add(up)
        dedup_reasons.append(text)
    reason_codes = dedup_reasons

    dedup_blockers: List[Dict[str, Any]] = []
    seen_blocker: set[Tuple[str, str]] = set()
    for row in blocker_tree:
        key = (str(row.get("code") or "").strip().upper(), str(row.get("logical_name") or "").strip())
        if key in seen_blocker:
            continue
        seen_blocker.add(key)
        dedup_blockers.append(row)
    blocker_tree = dedup_blockers

    if state == "OPEN_READY":
        canonical_blocker = ""
        reason_codes = []
        blocker_tree = []

    payload: Dict[str, Any] = {
        "schema_id": "paper_trading_day_authority",
        "schema_version": "v1",
        "authority_scope": "CANONICAL_DAY_READINESS_AUTHORITY",
        "day_utc": day_utc,
        "state": state,
        "status": status,
        "can_paper_trade_today": bool(can_paper_trade_today),
        "can_submit_paper_orders": bool(can_submit_paper_orders),
        "submit_mode_status": submit_mode_status,
        "dry_run_policy": dry_run_policy,
        "broker_transmit_enabled": broker_transmit_enabled,
        "broker_order_transmitted": bool(broker_order_transmitted),
        "missing_broker_ids_blocker": bool(missing_broker_ids_blocker),
        "missing_broker_ids_diagnostic": bool(missing_broker_ids_diagnostic),
        "canonical_blocker": canonical_blocker,
        "reason_codes": reason_codes,
        "blocker_tree": blocker_tree,
        "missing_or_stale_inputs": missing_or_stale_inputs,
        "input_status": input_status,
        "evidence_paths": evidence_paths,
        "producer": producer_block_v1(
            module="ops/tools/run_paper_trading_day_authority_v1.py",
            git_sha=repo_git_sha_v1(),
        ),
        "produced_at_utc": produced_at_utc,
    }

    ref = atomic_write_idempotent_validated_json_v1(
        path=resolve_paper_trading_day_authority_path(truth_root=truth_root, day_utc=day_utc),
        payload=payload,
        schema_relpath=OUTPUT_SCHEMA,
        volatile_field_names=("produced_at_utc",),
    )
    print(
        json.dumps(
            {
                "path": str(ref.path),
                "sha256": ref.sha256,
                "state": state,
                "canonical_blocker": canonical_blocker,
                "can_submit_paper_orders": bool(can_submit_paper_orders),
            },
            sort_keys=True,
        )
    )
    return 0 if state == "OPEN_READY" and can_submit_paper_orders else 2


if __name__ == "__main__":
    raise SystemExit(main())
