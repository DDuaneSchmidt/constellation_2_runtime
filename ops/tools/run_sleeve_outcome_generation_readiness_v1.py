#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1, resolve_fact_plane_truth_root_v1
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1, git_commit_v1, git_dirty_status_v1
from ops.tools.aegis_truth_integrity_common_v1 import now_iso_v1, read_json_v1, write_json_v1
from ops.tools.run_execution_package_from_authorized_intent_v1 import _execution_build_chain_map


PRODUCER = "ops/tools/run_sleeve_outcome_generation_readiness_v1.py"
SCHEMA = REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/sleeve_outcome_generation_readiness.v1.schema.json"
SCORING_ELIGIBLE_SLEEVES = ("C2_TREND_EQ_PRIMARY_V1", "C2_VOL_INCOME_DEFINED_RISK_V1")
SAME_DAY_BROKER_FRESHNESS_SECONDS = 300


def report_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "sleeve_outcome_generation_readiness_v1" / day_utc / "sleeve_outcome_generation_readiness.v1.json"


def _report(root: Path, family: str, day: str, filename: str) -> Path:
    return Path(root).resolve() / "reports" / family / day / filename


def _read_inputs(root: Path, day: str) -> tuple[dict[str, Path], dict[str, dict[str, Any]]]:
    paths = {
        "sleeve_intent_quality_diagnostics": _report(root, "sleeve_intent_quality_diagnostics_v1", day, "sleeve_intent_quality_diagnostics.v1.json"),
        "portfolio_scoring": _report(root, "portfolio_scoring_v1", day, "portfolio_scoring.v1.json"),
        "risk_sizing_authority": _report(root, "risk_sizing_authority_v1", day, "risk_sizing_authority.v1.json"),
        "market_data_authority": _report(root, "market_data_authority_v1", day, "market_data_authority.v1.json"),
        "submit_boundary_status": _report(root, "submit_boundary_status_v1", day, "submit_boundary_status.v1.json"),
        "trade_outcome": _report(root, "trade_outcome_v1", day, "trade_outcome.v1.json"),
        "sleeve_intent_trade_attribution": _report(root, "sleeve_intent_trade_attribution_v1", day, "sleeve_intent_trade_attribution.v1.json"),
    }
    return paths, {name: read_json_v1(path) for name, path in paths.items()}


def _capital_allocation_path(execution_root: Path, day: str) -> Path:
    return (
        execution_root
        / "allocation_v1"
        / "capital_authority_allocation_v1"
        / day
        / "capital_authority_allocation.v1.json"
    )


def _rows(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    return [row for row in payload.get(key, []) if isinstance(row, dict)] if isinstance(payload.get(key), list) else []


def _by_intent(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row.get("intent_id") or ""): row for row in rows if str(row.get("intent_id") or "")}


def _authorization_path(execution_root: Path, day: str, intent_hash: str) -> Path:
    return execution_root / "engine_activity_v1" / "authorization_v1" / day / f"{intent_hash}.authorization.v1.json"


def _find_submit_trace(root: Path, day: str, intent_id: str) -> list[str]:
    trace_root = root / "reports" / "submit_decision_trace_v1" / day
    if not trace_root.is_dir():
        return []
    out: list[str] = []
    for path in sorted(trace_root.rglob("submit_decision_trace.v1.json")):
        try:
            payload = read_json_v1(path)
        except Exception:
            payload = {}
        text = json.dumps(payload, sort_keys=True)
        if intent_id in text:
            out.append(str(path))
    return out


def _outcome_matches(outcome: dict[str, Any], intent_id: str) -> bool:
    return str(outcome.get("intent_id") or "") == intent_id


def _authorization_blockers(auth: dict[str, Any], authorization_row: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    outcome = str(authorization_row.get("authorization_outcome") or "").strip().upper()
    row_qty = authorization_row.get("authorized_quantity")
    if authorization_row:
        if outcome and outcome not in {"APPROVED", "RESIZED"}:
            blockers.append("AUTHORIZATION_REJECTED")
            blockers.append(f"AUTHORIZATION_STATUS_{outcome}")
        if row_qty in (None, "", 0):
            blockers.append("AUTHORIZATION_REJECTED")
            blockers.append("AUTHORIZED_QUANTITY_ZERO_OR_MISSING")
        if outcome not in {"APPROVED", "RESIZED"} or row_qty in (None, "", 0):
            for code in authorization_row.get("reason_codes", []) if isinstance(authorization_row.get("reason_codes"), list) else []:
                if str(code):
                    blockers.append(str(code))
        return sorted(set(blockers))
    if not auth:
        return ["AUTHORIZATION_ARTIFACT_MISSING"]
    status = str(auth.get("status") or "").upper()
    decision = str((auth.get("authorization") or {}).get("decision") or auth.get("decision_enum") or "").upper()
    qty = (auth.get("authorization") or {}).get("authorized_quantity")
    if status and status != "PASS":
        blockers.append(f"AUTHORIZATION_STATUS_{status}")
    if decision in {"REJECTED", "BLOCK"}:
        blockers.append("AUTHORIZATION_REJECTED")
    if qty in (None, "", 0):
        blockers.append("AUTHORIZED_QUANTITY_ZERO_OR_MISSING")
    for code in auth.get("reason_codes", []) if isinstance(auth.get("reason_codes"), list) else []:
        if str(code):
            blockers.append(str(code))
    return sorted(set(blockers))


def _authorization_details(
    auth: dict[str, Any],
    auth_path: Path,
    day_utc: str,
    truth_root: Path,
    authorization_row: dict[str, Any],
    capital_allocation_path: Path,
) -> dict[str, Any]:
    authorization = auth.get("authorization") if isinstance(auth.get("authorization"), dict) else {}
    policy_rules = auth.get("constitutional_shadow", {}).get("decision", {}).get("blocker_rules") if isinstance(auth.get("constitutional_shadow"), dict) else []
    if not isinstance(policy_rules, list):
        policy_rules = []
    reason_codes = (
        authorization_row.get("reason_codes")
        if isinstance(authorization_row.get("reason_codes"), list)
        else auth.get("reason_codes")
        if isinstance(auth.get("reason_codes"), list)
        else []
    )
    outcome = str(authorization_row.get("authorization_outcome") or "").strip().upper()
    row_qty = authorization_row.get("authorized_quantity")
    approved = outcome in {"APPROVED", "RESIZED"} and row_qty not in (None, "", 0)
    rejection_reason = "" if approved else (
        ",".join(str(code) for code in reason_codes if str(code))
        or ",".join(str(rule) for rule in policy_rules if str(rule))
    )
    return {
        "source": "capital_authority_allocation_v1" if authorization_row else "engine_activity_authorization_v1",
        "source_artifact": str(capital_allocation_path if authorization_row else auth_path),
        "artifact_path": str(auth_path),
        "authorization_status": outcome or str(auth.get("status") or "MISSING").upper(),
        "decision": outcome or str(authorization.get("decision") or auth.get("decision_enum") or "").upper(),
        "authorized_quantity": row_qty if authorization_row else authorization.get("authorized_quantity"),
        "rejection_reason": rejection_reason,
        "policy_rules": (
            [str(code) for code in reason_codes if str(code)]
            if authorization_row
            else [str(rule) for rule in policy_rules if str(rule)]
        ),
        "projection_artifact": {
            "path": str(auth_path),
            "status": str(auth.get("status") or "MISSING").upper(),
            "decision": str(authorization.get("decision") or auth.get("decision_enum") or "").upper(),
            "authorized_quantity": authorization.get("authorized_quantity"),
            "reason_codes": [str(code) for code in auth.get("reason_codes", [])] if isinstance(auth.get("reason_codes"), list) else [],
        },
        "required_recovery_command": f"PYTHONPATH=\"$PWD\" python3 ops/tools/run_authorization_artifacts_day_v1.py --day_utc {day_utc} --truth_root {truth_root}",
    }


def _authorization_rows_by_intent(capital_allocation: dict[str, Any]) -> dict[str, dict[str, Any]]:
    decision_chain = capital_allocation.get("decision_chain") if isinstance(capital_allocation.get("decision_chain"), dict) else {}
    rows = decision_chain.get("authorized_trade_intents") if isinstance(decision_chain.get("authorized_trade_intents"), list) else []
    return {
        str(row.get("intent_id") or ""): row
        for row in rows
        if isinstance(row, dict) and str(row.get("intent_id") or "")
    }


def _sleeve_controls_by_scope(capital_allocation: dict[str, Any]) -> dict[str, dict[str, Any]]:
    control_state = (
        capital_allocation.get("governed_evaluation_control_state")
        if isinstance(capital_allocation.get("governed_evaluation_control_state"), dict)
        else {}
    )
    rows = control_state.get("sleeve_controls") if isinstance(control_state.get("sleeve_controls"), list) else []
    return {
        str(row.get("scope_id") or ""): row
        for row in rows
        if isinstance(row, dict) and str(row.get("scope_id") or "")
    }


def _strategy_scope_id(sleeve_id: str) -> str:
    text = str(sleeve_id or "").strip()
    return text[:-3] if text.endswith("_V1") else text


def _read_intent_obj(intent_path: str) -> dict[str, Any]:
    if not str(intent_path or "").strip():
        return {}
    try:
        return read_json_v1(Path(str(intent_path)).expanduser().resolve())
    except Exception:
        return {}


def _sizing_root_cause(*, row: dict[str, Any], sleeve_control: dict[str, Any]) -> str:
    requested_quantity = int(row.get("requested_quantity") or 0)
    risk_per_unit = int(row.get("risk_per_unit_cents") or 0)
    required = int(row.get("required_risk_cents") or 0)
    sleeve_headroom = int(row.get("available_sleeve_headroom_cents") or 0)
    reason_codes = {str(code) for code in row.get("reason_codes", []) if str(code)} if isinstance(row.get("reason_codes"), list) else set()
    control_state = str(sleeve_control.get("control_state") or "").strip()
    artifact_status = str(sleeve_control.get("artifact_status") or "").strip()
    if "AUTHZ_MISSING_DEFINED_RISK_EVIDENCE" in reason_codes:
        return "DEFINED_RISK_EVIDENCE_MISSING"
    if requested_quantity <= 0 or risk_per_unit <= 0:
        return "QUANTITY_OR_DEFINED_RISK_UNPROVEN"
    if "BUNDLE_B_HEADROOM_REJECTED" in reason_codes and required > sleeve_headroom:
        if control_state == "fail_safe_block_new_risk" or artifact_status == "MISSING":
            return "REAL_HEADROOM_POLICY_REJECT_WITH_GOVERNANCE_FAILSAFE"
        return "REAL_HEADROOM_POLICY_REJECT"
    return "NO_SIZING_DEFECT_DETECTED"


def _sizing_audit(
    *,
    sleeve_id: str,
    intent_path: str,
    risk_row: dict[str, Any],
    authorization_row: dict[str, Any],
    sleeve_control: dict[str, Any],
    capital_allocation_path: Path,
) -> dict[str, Any]:
    intent_obj = _read_intent_obj(intent_path)
    constraints = intent_obj.get("constraints") if isinstance(intent_obj.get("constraints"), dict) else {}
    row = authorization_row if authorization_row else {}
    return {
        "source": "capital_authority_allocation_v1",
        "source_artifact": str(capital_allocation_path),
        "requested_target_pct": str(
            row.get("target_notional_pct")
            or intent_obj.get("target_notional_pct")
            or ""
        ),
        "nav_basis": {
            "account_net_liquidation_cents": risk_row.get("account_net_liquidation_cents"),
            "allowed_risk_cents": risk_row.get("allowed_risk_cents"),
        },
        "risk_per_unit_cents": row.get("risk_per_unit_cents"),
        "stop_distance_bps": constraints.get("stop_loss_bps"),
        "requested_quantity": row.get("requested_quantity"),
        "requested_quantity_basis": row.get("requested_quantity_basis"),
        "authorized_quantity": row.get("authorized_quantity"),
        "final_quantity": risk_row.get("final_quantity"),
        "required_risk_cents": row.get("required_risk_cents"),
        "sleeve_headroom_cents": row.get("available_sleeve_headroom_cents"),
        "portfolio_headroom_cents": row.get("available_portfolio_headroom_cents"),
        "policy_reason_codes": [str(code) for code in row.get("reason_codes", [])] if isinstance(row.get("reason_codes"), list) else [],
        "sleeve_governance_control": {
            "scope_id": str(sleeve_control.get("scope_id") or _strategy_scope_id(sleeve_id)),
            "artifact_status": str(sleeve_control.get("artifact_status") or ""),
            "control_state": str(sleeve_control.get("control_state") or ""),
            "diagnostic": str(sleeve_control.get("diagnostic") or ""),
            "effective_headroom_cents": sleeve_control.get("effective_headroom_cents"),
            "reason_codes": [str(code) for code in sleeve_control.get("reason_codes", [])] if isinstance(sleeve_control.get("reason_codes"), list) else [],
        },
        "root_cause": _sizing_root_cause(row=row, sleeve_control=sleeve_control),
    }


def _classifications(blockers: list[str]) -> list[str]:
    out: list[str] = []
    if "BUNDLE_B_HEADROOM_REJECTED" in blockers:
        out.append("REAL_POLICY_REJECT")
    if "OPTIONS_CHAIN_SNAPSHOT_MISSING" in blockers:
        out.append("MISSING_OPTIONS_CHAIN_INPUT")
    if any(code in blockers for code in ("AUTHZ_MISSING_DEFINED_RISK_EVIDENCE", "BUNDLE_B_REQUESTED_QUANTITY_UNPROVEN", "BUNDLE_B_REQUESTED_QUANTITY_ZERO")):
        out.append("MISSING_DEFINED_RISK_OR_QUANTITY_EVIDENCE")
    if "EXECUTION_PACKAGE_MISSING" in blockers:
        out.append("EXECUTION_PACKAGE_DOWNSTREAM_OF_AUTHORIZATION")
    if "SUBMIT_DECISION_TRACE_MISSING" in blockers:
        out.append("SUBMIT_TRACE_DOWNSTREAM_OF_SUBMIT_BOUNDARY")
    if "COMPLETED_OUTCOME_MISSING" in blockers:
        out.append("OUTCOME_MISSING_NO_REAL_FILL_OR_POSITION_LIFECYCLE")
    return sorted(set(out or ["UNKNOWN"]))


def _execution_package_readiness(
    *,
    blockers: list[str],
    execution_package_path: str,
    day_utc: str,
    latest_execution_build: dict[str, Any],
) -> dict[str, Any]:
    if execution_package_path:
        status = "PRESENT"
    elif latest_execution_build:
        status = "BLOCKED_BY_EXECUTION_BUILD"
    elif "AUTHORIZATION_REJECTED" in blockers or "AUTHORIZED_QUANTITY_ZERO_OR_MISSING" in blockers:
        status = "BLOCKED_BY_AUTHORIZATION_REJECTED"
    else:
        status = "MISSING"
    readiness = {
        "status": status,
        "expected_path": execution_package_path,
        "producer": "ops/tools/run_execution_package_from_authorized_intent_v1.py",
        "recovery_command": f"PYTHONPATH=\"$PWD\" python3 ops/tools/run_execution_package_from_authorized_intent_v1.py --day_utc {day_utc} --truth_root <EXECUTION_TRUTH_ROOT> --intent_id <AUTHORIZED_INTENT_ID>",
    }
    if latest_execution_build:
        first = latest_execution_build.get("first_real_blocker") if isinstance(latest_execution_build.get("first_real_blocker"), dict) else {}
        build_path = str(latest_execution_build.get("_path") or "")
        candidate_ref = latest_execution_build.get("candidate_ref") if isinstance(latest_execution_build.get("candidate_ref"), dict) else {}
        readiness["latest_execution_build"] = {
            "build_path": build_path,
            "closure_status": str(latest_execution_build.get("closure_status") or ""),
            "first_real_blocker": first,
            "blocking_chain": list(latest_execution_build.get("blocking_chain") or []),
            "materializable_now": list(latest_execution_build.get("materializable_now") or []),
            "chain_map": _execution_build_chain_map(
                day_utc=day_utc,
                truth_root=Path(str(candidate_ref.get("execution_truth_root") or ".")),
                build_obj=latest_execution_build,
                build_path=build_path,
                package_path=execution_package_path,
            ),
        }
    return readiness


def _find_execution_package_for_intent(*, execution_root: Path, day_utc: str, intent_id: str, intent_hash: str) -> str:
    day_root = (execution_root / "execution_package_v1" / day_utc).resolve()
    if not day_root.exists() or not day_root.is_dir():
        return ""
    for path in sorted(day_root.glob("*/execution_package.v1.json")):
        obj = read_json_v1(path)
        if str(obj.get("intent_id") or "").strip() == intent_id:
            return str(path.resolve())
        if intent_hash and str(obj.get("intent_hash") or "").strip() == intent_hash:
            return str(path.resolve())
    return ""


def _find_latest_execution_build_for_intent(*, truth_root: Path, day_utc: str, intent_id: str) -> dict[str, Any]:
    build_root = truth_root / "reports" / "execution_build_v1" / day_utc
    if not build_root.exists() or not build_root.is_dir():
        return {}
    matches: list[tuple[str, str, dict[str, Any]]] = []
    for path in sorted(build_root.glob("*/execution_build.v1.json")):
        obj = read_json_v1(path)
        if str(obj.get("intent_id") or "").strip() != intent_id:
            continue
        generated = str(obj.get("generated_utc") or obj.get("generated_at") or "")
        obj["_path"] = str(path.resolve())
        matches.append((generated, str(path), obj))
    if not matches:
        return {}
    return sorted(matches, key=lambda item: (item[0], item[1]))[-1][2]


def _submit_trace_readiness(*, blockers: list[str], submit_traces: list[str], day_utc: str, truth_root: Path) -> dict[str, Any]:
    if submit_traces:
        status = "PRESENT"
    elif "EXECUTION_PACKAGE_MISSING" in blockers or "AUTHORIZATION_REJECTED" in blockers:
        status = "EXPECTED_MISSING_SUBMIT_BOUNDARY_NOT_REACHED"
    else:
        status = "MISSING"
    return {
        "status": status,
        "producer": "ops/tools/run_submit_decision_trace_v1.py",
        "recovery_command": f"PYTHONPATH=\"$PWD\" python3 ops/tools/run_submit_decision_trace_v1.py --day_utc {day_utc} --truth_root {truth_root} --environment PAPER",
        "trace_paths": submit_traces,
    }


def _symbol_requires_market_input(market: dict[str, Any], symbol: str, code: str) -> bool:
    required_symbols = {str(sym).upper() for sym in market.get("required_symbols", []) if str(sym)}
    if code == "OPTIONS_CHAIN_SNAPSHOT_MISSING":
        return str(symbol).upper() in required_symbols
    return True


def _options_chain_recovery(*, execution_root: Path, day_utc: str, symbol: str, required: bool) -> dict[str, Any]:
    expected_root = execution_root / "options_chain_snapshot_v1" / day_utc
    return {
        "required": bool(required),
        "symbol": str(symbol or "").strip().upper(),
        "expected_artifact_root": str(expected_root),
        "expected_artifact_pattern": str(expected_root / "<capture_id>" / "options_chain_snapshot.v1.json"),
        "producer": "ops/tools/run_options_chain_snapshot_required_day_v1.py",
        "recovery_command": f"PYTHONPATH=\"$PWD\" python3 ops/tools/run_options_chain_snapshot_required_day_v1.py --day_utc {day_utc} --truth_root <TRUTH_ROOT> --symbol {str(symbol or '').strip().upper()}",
        "same_day_only": True,
    }


def _same_day_execution_requirements(*, day_utc: str, truth_root: Path, execution_root: Path) -> list[dict[str, Any]]:
    return [
        {
            "requirement_id": "broker_event_observer",
            "producer": "ops/ib/c2_execution_observer_v1.py",
            "producer_command": "PYTHONPATH=\"$PWD\" python3 ops/ib/c2_execution_observer_v1.py --truth_root <EXECUTION_TRUTH_ROOT> --environment PAPER",
            "expected_artifact_path": str(execution_root / "execution_evidence_v1" / "broker_events" / day_utc / "broker_event_log.v1.jsonl"),
            "freshness_threshold_seconds": SAME_DAY_BROKER_FRESHNESS_SECONDS,
            "must_be_produced_during_target_day": True,
            "recovery_command": "Start or keep the governed IB execution observer running during the target trading day.",
        },
        {
            "requirement_id": "broker_supply_v1",
            "producer": "ops/tools/run_broker_supply_v1.py",
            "producer_command": f"PYTHONPATH=\"$PWD\" python3 ops/tools/run_broker_supply_v1.py --day_utc {day_utc} --environment PAPER --truth_root {truth_root} --freshness_seconds {SAME_DAY_BROKER_FRESHNESS_SECONDS}",
            "expected_artifact_path": str(truth_root / "reports" / "broker_supply_v1" / day_utc / "broker_supply.v1.json"),
            "freshness_threshold_seconds": SAME_DAY_BROKER_FRESHNESS_SECONDS,
            "must_be_produced_during_target_day": True,
            "recovery_command": "Rerun broker supply after same-day broker events are fresh.",
        },
        {
            "requirement_id": "runtime_resilience_authority_v1",
            "producer": "ops/tools/run_runtime_resilience_authority_v1.py",
            "producer_command": f"PYTHONPATH=\"$PWD\" python3 ops/tools/run_runtime_resilience_authority_v1.py --day_utc {day_utc} --environment PAPER --truth_root {truth_root} --freshness_seconds {SAME_DAY_BROKER_FRESHNESS_SECONDS}",
            "expected_artifact_path": str(truth_root / "reports" / "runtime_resilience_authority_v1" / day_utc / "runtime_resilience_authority.v1.json"),
            "freshness_threshold_seconds": SAME_DAY_BROKER_FRESHNESS_SECONDS,
            "must_be_produced_during_target_day": True,
            "recovery_command": "Rerun runtime resilience after broker supply and broker event probe are fresh.",
        },
        {
            "requirement_id": "session_readiness_refresh_v1",
            "producer": "ops/tools/run_session_readiness_refresh_v1.py",
            "producer_command": f"PYTHONPATH=\"$PWD\" python3 ops/tools/run_session_readiness_refresh_v1.py --day_utc {day_utc}",
            "expected_artifact_path": str(truth_root / "reports" / "session_readiness_refresh_v1" / day_utc / "session_readiness_refresh.v1.json"),
            "freshness_threshold_seconds": None,
            "must_be_produced_during_target_day": True,
            "recovery_command": "Run governed same-day session readiness refresh after startup and broker evidence are current.",
        },
        {
            "requirement_id": "day_authority_decision_v1",
            "producer": "ops/tools/run_day_authority_decision_v1.py",
            "producer_command": f"PYTHONPATH=\"$PWD\" python3 ops/tools/run_day_authority_decision_v1.py --day_utc {day_utc} --truth_root {truth_root}",
            "expected_artifact_path": str(truth_root / "reports" / "day_authority_decision_v1" / day_utc / "day_authority_decision.v1.json"),
            "freshness_threshold_seconds": None,
            "must_be_produced_during_target_day": True,
            "recovery_command": "Rerun day authority decision after governed session readiness refresh.",
        },
        {
            "requirement_id": "target_day_admission_v1",
            "producer": "ops/tools/run_session_authority_v1.py",
            "producer_command": f"PYTHONPATH=\"$PWD\" python3 ops/tools/run_session_authority_v1.py --target_day {day_utc} --truth_root {truth_root} --environment PAPER --ib_account DUO847203 --phase admit",
            "expected_artifact_path": str(truth_root / "target_day_admission_v1" / f"{day_utc}.json"),
            "freshness_threshold_seconds": None,
            "must_be_produced_during_target_day": True,
            "recovery_command": "Run governed target-day admission after required same-day session gates pass.",
        },
        {
            "requirement_id": "day_activation_package_v1",
            "producer": "ops/tools/run_day_activation_authority_v1.py",
            "producer_command": f"PYTHONPATH=\"$PWD\" python3 ops/tools/run_day_activation_authority_v1.py --operation_type fresh_paper_entry_v1 --day_utc {day_utc} --sleeve_id PRIMARY --environment PAPER --ib_account DUO847203 --materialize YES --emit_package YES",
            "expected_artifact_path": str(execution_root / "day_activation_package_v1" / day_utc / "<submission_id>" / "day_activation_package.v1.json"),
            "freshness_threshold_seconds": None,
            "must_be_produced_during_target_day": True,
            "recovery_command": "Run governed day activation only after target-day admission passes.",
        },
        {
            "requirement_id": "global_context_package_v1",
            "producer": "ops/tools/run_global_context_authority_v1.py",
            "producer_command": f"PYTHONPATH=\"$PWD\" python3 ops/tools/run_global_context_authority_v1.py --operation_type fresh_paper_entry_v1 --day_utc {day_utc} --sleeve_id PRIMARY --environment PAPER --ib_account DUO847203 --materialize YES --emit_package YES",
            "expected_artifact_path": str(execution_root / "global_context_package_v1" / day_utc / "<submission_id>" / "global_context_package.v1.json"),
            "freshness_threshold_seconds": None,
            "must_be_produced_during_target_day": True,
            "recovery_command": "Run governed global context only after day activation passes.",
        },
        {
            "requirement_id": "execution_package_v1",
            "producer": "ops/tools/run_execution_package_from_authorized_intent_v1.py",
            "producer_command": f"PYTHONPATH=\"$PWD\" python3 ops/tools/run_execution_package_from_authorized_intent_v1.py --day_utc {day_utc} --truth_root {execution_root} --intent_id <AUTHORIZED_INTENT_ID>",
            "expected_artifact_path": str(execution_root / "execution_package_v1" / day_utc / "<submission_id>" / "execution_package.v1.json"),
            "freshness_threshold_seconds": None,
            "must_be_produced_during_target_day": True,
            "recovery_command": "Run governed execution package producer only after authorization and upstream packages pass.",
        },
    ]


def _same_day_evidence_status(*, blockers: list[str], day_utc: str) -> dict[str, Any]:
    stale = "BROKER_EVENT_LOG_STALE" in set(blockers)
    target = date.fromisoformat(day_utc)
    today = datetime.now(UTC).date()
    if stale and target < today:
        status = "NON_RECOVERABLE_STALE_SAME_DAY_EVIDENCE"
        recovery = "Do not backfill freshness. Prepare and run the governed same-day producer chain on the next valid trading day."
    elif stale:
        status = "RECOVERABLE_DURING_TARGET_DAY_ONLY"
        recovery = "Refresh same-day broker event truth through the governed IB observer during the target trading day."
    else:
        status = "NOT_BLOCKED_BY_SAME_DAY_FRESHNESS"
        recovery = ""
    return {
        "status": status,
        "blocking_artifact": "broker_event_log.v1.jsonl" if stale else "",
        "reason_codes": ["BROKER_EVENT_LOG_STALE"] if stale else [],
        "must_be_produced_during_target_day": bool(stale),
        "recovery_command": recovery,
    }


def _next_step(blockers: list[str]) -> tuple[str, str]:
    if "OPTIONS_CHAIN_SNAPSHOT_MISSING" in blockers:
        return (
            "ops/tools/run_options_chain_snapshot_required_day_v1.py",
            "PYTHONPATH=\"$PWD\" python3 ops/tools/run_options_chain_snapshot_required_day_v1.py --day_utc <DAY>",
        )
    if "AUTHORIZATION_REJECTED" in blockers or "AUTHORIZED_QUANTITY_ZERO_OR_MISSING" in blockers:
        return (
            "ops/tools/run_authorization_artifacts_day_v1.py",
            "PYTHONPATH=\"$PWD\" python3 ops/tools/run_authorization_artifacts_day_v1.py --day_utc <DAY> --truth_root <TRUTH_ROOT>",
        )
    if "EXECUTION_PACKAGE_MISSING" in blockers:
        return (
            "ops/tools/run_execution_package_from_authorized_intent_v1.py",
            "PYTHONPATH=\"$PWD\" python3 ops/tools/run_execution_package_from_authorized_intent_v1.py --day_utc <DAY> --truth_root <EXECUTION_TRUTH_ROOT> --intent_id <AUTHORIZED_INTENT_ID>",
        )
    if "SUBMIT_DECISION_TRACE_MISSING" in blockers:
        return (
            "ops/tools/run_submit_decision_trace_v1.py",
            "PYTHONPATH=\"$PWD\" python3 ops/tools/run_submit_decision_trace_v1.py --day_utc <DAY> --truth_root <TRUTH_ROOT> --environment PAPER",
        )
    return (
        "ops/tools/run_trade_outcome_v1.py",
        "PYTHONPATH=\"$PWD\" python3 ops/tools/run_trade_outcome_v1.py --day_utc <DAY> --truth_root <TRUTH_ROOT> --environment PAPER",
    )


def build_sleeve_outcome_generation_readiness_v1(*, day_utc: str, truth_root: Path, runtime_root: Path | None = None, execution_root: Path | None = None) -> dict[str, Any]:
    root = Path(truth_root).resolve()
    runtime = Path(runtime_root or truth_root).resolve()
    execution = Path(execution_root or truth_root).resolve()
    paths, payloads = _read_inputs(root, day_utc)
    capital_allocation_path = _capital_allocation_path(execution, day_utc)
    capital_allocation = read_json_v1(capital_allocation_path)
    authorization_rows = _authorization_rows_by_intent(capital_allocation)
    sleeve_controls = _sleeve_controls_by_scope(capital_allocation)
    diag = _by_intent(_rows(payloads["sleeve_intent_quality_diagnostics"], "intent_diagnostics"))
    scoring_rows = [
        row for row in _rows(payloads["portfolio_scoring"], "rankings")
        if str(row.get("sleeve_id") or "") in SCORING_ELIGIBLE_SLEEVES and row.get("executable_eligible") is True
    ]
    risk = _by_intent(_rows(payloads["risk_sizing_authority"], "sizing_decisions"))
    outcome = payloads["trade_outcome"]
    submit_boundary = payloads["submit_boundary_status"]
    market = payloads["market_data_authority"]
    results: list[dict[str, Any]] = []
    for score in sorted(scoring_rows, key=lambda row: int(row.get("rank") or 999)):
        intent_id = str(score.get("intent_id") or "")
        sleeve_id = str(score.get("sleeve_id") or "")
        drow = diag.get(intent_id, {})
        rrow = risk.get(intent_id, {})
        intent_path = str(rrow.get("intent_path") or next(iter(score.get("evidence_paths", []) or []), ""))
        intent_hash = str(rrow.get("intent_hash") or Path(intent_path).name.split(".", 1)[0]).strip()
        auth_path = _authorization_path(execution, day_utc, intent_hash)
        auth = read_json_v1(auth_path) if auth_path.exists() else {}
        authorization_row = authorization_rows.get(intent_id, {})
        sleeve_control = sleeve_controls.get(_strategy_scope_id(sleeve_id), {})
        submit_traces = _find_submit_trace(root, day_utc, intent_id)
        execution_package_path = str(rrow.get("execution_package_path") or "")
        if not execution_package_path:
            execution_package_path = _find_execution_package_for_intent(
                execution_root=execution,
                day_utc=day_utc,
                intent_id=intent_id,
                intent_hash=intent_hash,
            )
        latest_execution_build = _find_latest_execution_build_for_intent(
            truth_root=root,
            day_utc=day_utc,
            intent_id=intent_id,
        )
        symbol = str(score.get("symbol") or drow.get("symbol") or "")
        raw_missing_inputs = drow.get("missing_inputs") if isinstance(drow.get("missing_inputs"), list) else []
        missing_inputs = [
            str(code)
            for code in raw_missing_inputs
            if _symbol_requires_market_input(market, symbol, str(code))
        ]
        options_required = "OPTIONS_CHAIN_SNAPSHOT_MISSING" in missing_inputs or (
            str(market.get("status") or "").upper() == "FAIL"
            and str(market.get("first_blocker") or "") == "OPTIONS_CHAIN_SNAPSHOT_MISSING"
            and str(symbol).upper() in {str(sym).upper() for sym in market.get("required_symbols", []) if str(sym)}
        )
        blockers = _authorization_blockers(auth, authorization_row)
        missing_evidence: list[str] = []
        if not execution_package_path:
            blockers.append("EXECUTION_PACKAGE_MISSING")
            missing_evidence.append("execution_package_v1")
        if not submit_traces:
            blockers.append("SUBMIT_DECISION_TRACE_MISSING")
            missing_evidence.append("submit_decision_trace_v1")
        if str(submit_boundary.get("submit_allowed") or "").lower() != "true":
            blockers.append(str(submit_boundary.get("canonical_blocker") or submit_boundary.get("first_blocker_code") or "SUBMIT_BOUNDARY_NOT_READY"))
        if not _outcome_matches(outcome, intent_id) or str(outcome.get("outcome_status") or "").upper() not in {"OPEN", "CLOSED"}:
            blockers.append("COMPLETED_OUTCOME_MISSING")
            missing_evidence.append("trade_outcome_v1")
        if str(market.get("status") or "").upper() == "FAIL" and str(symbol).upper() in {str(sym).upper() for sym in market.get("required_symbols", []) if str(sym)}:
            blockers.append(str(market.get("first_blocker") or "MARKET_DATA_AUTHORITY_FAIL"))
        unique_blockers = sorted(set(item for item in blockers if item))
        producer, command = _next_step(unique_blockers)
        results.append(
            {
                "sleeve_id": sleeve_id,
                "intent_id": intent_id,
                "intent_artifact": intent_path,
                "signal_trigger": str(drow.get("signal_trigger_reason") or ",".join(str(code) for code in score.get("reason_codes", []) if str(code))),
                "market_inputs": {
                    "symbol": symbol,
                    "missing_inputs": missing_inputs,
                    "market_data_status": str(market.get("status") or "UNKNOWN"),
                    "options_chain_recovery": _options_chain_recovery(
                        execution_root=execution,
                        day_utc=day_utc,
                        symbol=symbol,
                        required=options_required,
                    ),
                },
                "risk_sizing_result": {
                    "sizing_state": str(rrow.get("sizing_state") or "MISSING"),
                    "execution_package_path": execution_package_path,
                    "final_quantity": rrow.get("final_quantity"),
                    "reason_code": str(rrow.get("reason_code") or ""),
                    "account_net_liquidation_cents": rrow.get("account_net_liquidation_cents"),
                    "allowed_risk_cents": rrow.get("allowed_risk_cents"),
                },
                "sizing_audit": _sizing_audit(
                    sleeve_id=sleeve_id,
                    intent_path=intent_path,
                    risk_row=rrow,
                    authorization_row=authorization_row,
                    sleeve_control=sleeve_control,
                    capital_allocation_path=capital_allocation_path,
                ),
                "authorization_details": _authorization_details(
                    auth,
                    auth_path,
                    day_utc,
                    root,
                    authorization_row,
                    capital_allocation_path,
                ),
                "execution_package_readiness": _execution_package_readiness(
                    blockers=unique_blockers,
                    execution_package_path=execution_package_path,
                    day_utc=day_utc,
                    latest_execution_build=latest_execution_build,
                ),
                "submit_trace_readiness": _submit_trace_readiness(blockers=unique_blockers, submit_traces=submit_traces, day_utc=day_utc, truth_root=root),
                "scoring_eligible": True,
                "execution_ready_if_aegis_ready": not unique_blockers,
                "root_cause_classification": _classifications(unique_blockers),
                "same_day_evidence_status": _same_day_evidence_status(blockers=unique_blockers, day_utc=day_utc),
                "why_did_not_submit": unique_blockers,
                "missing_evidence": sorted(set(missing_evidence)),
                "next_governed_producer": producer,
                "recovery_command": command.replace("<DAY>", day_utc).replace("<TRUTH_ROOT>", str(root)),
                "outcome_generation_blockers": unique_blockers,
                "evidence_paths": sorted(set([intent_path, str(auth_path), str(capital_allocation_path), *submit_traces, *[str(path) for path in paths.values() if path.exists()]])),
            }
        )
    return {
        "schema_id": "sleeve_outcome_generation_readiness",
        "schema_version": "sleeve_outcome_generation_readiness.v1",
        "day_utc": day_utc,
        "generated_at": now_iso_v1(),
        "git_commit": git_commit_v1(),
        "git_dirty_status": git_dirty_status_v1(),
        "truth_root": str(root),
        "runtime_root": str(runtime),
        "execution_root": str(execution),
        "producer": PRODUCER,
        "authority": "DIAGNOSTIC_ONLY",
        "readiness_effect": "NONE",
        "submit_effect": "NONE",
        "allocation_effect": "NONE",
        "same_day_execution_requirements": _same_day_execution_requirements(day_utc=day_utc, truth_root=root, execution_root=execution),
        "scoring_eligible_sleeves": list(SCORING_ELIGIBLE_SLEEVES),
        "sleeve_results": results,
        "summary": {
            "eligible_intent_count": len(results),
            "execution_ready_count": sum(1 for row in results if row["execution_ready_if_aegis_ready"] is True),
            "completed_outcome_ready_count": 0,
            "allocation_eligibility_effect": "NONE",
        },
    }


def _validate(payload: dict[str, Any]) -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    errors = sorted(Draft202012Validator(schema).iter_errors(payload), key=lambda err: list(err.path))
    if errors:
        raise ValueError("SLEEVE_OUTCOME_GENERATION_READINESS_SCHEMA_INVALID:" + ";".join(str(err.message) for err in errors[:3]))


def write_report_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any], command: str, execution_root: Path | None = None) -> Path:
    path = report_path(truth_root=truth_root, day_utc=day_utc)
    input_paths, _ = _read_inputs(truth_root, day_utc)
    contract_inputs = list(input_paths.values())
    if execution_root is not None:
        contract_inputs.append(_capital_allocation_path(execution_root, day_utc))
    attach_producer_contract_v1(
        payload,
        producer_name=PRODUCER,
        producer_command=command,
        input_artifacts=contract_inputs,
        output_artifacts=[path],
        schema_versions={"sleeve_outcome_generation_readiness_v1": "sleeve_outcome_generation_readiness.v1"},
    )
    _validate(payload)
    write_json_v1(path, payload)
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_sleeve_outcome_generation_readiness_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--runtime_root", default="")
    parser.add_argument("--execution_root", default="")
    args = parser.parse_args(argv)
    day = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    runtime_root = Path(args.runtime_root).expanduser().resolve() if str(args.runtime_root or "").strip() else truth_root
    execution_root = Path(args.execution_root).expanduser().resolve() if str(args.execution_root or "").strip() else truth_root
    payload = build_sleeve_outcome_generation_readiness_v1(day_utc=day, truth_root=truth_root, runtime_root=runtime_root, execution_root=execution_root)
    command = f"PYTHONPATH=\"$PWD\" python3 {PRODUCER} --day_utc {day} --truth_root {truth_root} --runtime_root {runtime_root} --execution_root {execution_root}"
    path = write_report_v1(truth_root=truth_root, day_utc=day, payload=payload, command=command, execution_root=execution_root)
    print(json.dumps({"status": "PASS", "path": str(path), "eligible_intent_count": payload["summary"]["eligible_intent_count"], "execution_ready_count": payload["summary"]["execution_ready_count"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
