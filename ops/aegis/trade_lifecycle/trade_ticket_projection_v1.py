from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from ops.aegis.trade_lifecycle.readiness_domain_evaluation_v1 import blockers_by_domain_v1, domain_status_map_v1

SCHEMA_ID = "trade_ticket_projection"
SCHEMA_VERSION = "v1"
REPORT_FAMILY = "trade_ticket_projection_v1"
REPORT_FILENAME = "trade_ticket_projection.v1.json"

DOMAIN_ORDER = [
    "market_data",
    "trade_construction",
    "capital_authority",
    "stop_risk",
    "manual_capture",
    "paper_submit",
    "execution",
]


def _stable_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def trade_ticket_projection_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def _action_for_domain(domain: str) -> str:
    return {
        "market_data": "Refresh canonical market data, then rerun paper trade construction.",
        "trade_construction": "Resolve missing entry, quantity, notional, stop/invalidation, or risk fields in paper construction.",
        "capital_authority": "Review paper capital authority/headroom; manual capture may still be governed independently.",
        "stop_risk": "Provide a governed stop price or invalidation level plus risk estimate.",
        "manual_capture": "Resolve manual capture blockers before recording captured_manually.",
        "paper_submit": "Run or repair paper submit boundary only if governed paper submit is required.",
        "execution": "Execution is disabled by default and is not part of manual paper capture.",
    }.get(domain, "Resolve readiness blockers for this domain.")


def trade_ticket_projection_v1(case: Mapping[str, Any]) -> dict[str, Any]:
    construction = case.get("paper_trade_construction") if isinstance(case.get("paper_trade_construction"), Mapping) else {}
    evaluations = case.get("readiness_domain_evaluations") if isinstance(case.get("readiness_domain_evaluations"), list) else []
    statuses = domain_status_map_v1(evaluations)
    blockers = blockers_by_domain_v1(evaluations)
    lineage = case.get("trade_ticket_lineage_v1") if isinstance(case.get("trade_ticket_lineage_v1"), Mapping) else {}
    submit_precheck = case.get("submit_boundary_precheck_v1") if isinstance(case.get("submit_boundary_precheck_v1"), Mapping) else {}
    lineage_status = str(lineage.get("lineage_status") or "MISSING_SUBMIT_BOUNDARY") if lineage else "MISSING_SUBMIT_BOUNDARY"
    submit_valid = str(submit_precheck.get("validation_status") or "") == "VALIDATED"
    records = case.get("manual_capture_records") if isinstance(case.get("manual_capture_records"), list) else []
    latest_record = records[-1] if records and isinstance(records[-1], Mapping) else {}
    latest_capture_status = str(latest_record.get("capture_status") or "").lower()
    case_state = str(case.get("current_state") or "")
    if case_state in {"CAPTURED_MANUALLY", "CAPTURED_HISTORICAL"} or latest_capture_status in {"captured_manually", "partial"}:
        original_lineage = latest_record.get("trade_ticket_lineage") if isinstance(latest_record.get("trade_ticket_lineage"), Mapping) else lineage
        payload = {
            "schema_id": SCHEMA_ID,
            "schema_version": SCHEMA_VERSION,
            "artifact_id": "",
            "trade_lifecycle_case_id": str(case.get("trade_lifecycle_case_id") or ""),
            "case_id": str(case.get("trade_lifecycle_case_id") or ""),
            "symbol": str(case.get("symbol") or latest_record.get("symbol") or construction.get("symbol") or "").upper(),
            "direction": str(case.get("direction") or construction.get("direction") or ""),
            "side": str(original_lineage.get("side") or case.get("direction") or construction.get("direction") or ""),
            "sleeve_id": str(case.get("sleeve_id") or latest_record.get("sleeve") or construction.get("sleeve_id") or ""),
            "sleeve": str(case.get("sleeve_id") or latest_record.get("sleeve") or construction.get("sleeve_id") or ""),
            "engine_id": str(case.get("engine_id") or construction.get("engine_id") or ""),
            "source_day": str(case.get("source_day") or construction.get("source_day") or ""),
            "source_run_id": str(case.get("source_run_id") or construction.get("source_run_id") or ""),
            "selected_exposure_intent_id": str(case.get("selected_exposure_intent_id") or latest_record.get("selected_exposure_intent_id") or construction.get("selected_exposure_intent_id") or ""),
            "ticket_id": str(latest_record.get("ticket_id") or original_lineage.get("ticket_id") or lineage.get("ticket_id") or ""),
            "lineage_status": "CAPTURED_HISTORICAL",
            "historical_state": "CAPTURED_HISTORICAL",
            "lifecycle_state": "CAPTURED_HISTORICAL",
            "current_state": "CAPTURED_HISTORICAL",
            "case_state": case_state,
            "state_reason": str(case.get("state_reason") or "Immutable manual capture evidence is attached."),
            "editable": False,
            "read_only": True,
            "historical_read_only": True,
            "captured_read_only": True,
            "runtime_evaluation_hash": str(original_lineage.get("runtime_evaluation_hash") or ""),
            "original_runtime_evaluation_hash": str(original_lineage.get("runtime_evaluation_hash") or ""),
            "lineage_hash": str(original_lineage.get("lineage_hash") or ""),
            "original_lineage_hash": str(original_lineage.get("lineage_hash") or ""),
            "construction_contract_id": str(original_lineage.get("construction_contract_id") or construction.get("construction_contract_id") or ""),
            "construction_contract_hash": str(original_lineage.get("construction_contract_hash") or construction.get("construction_contract_hash") or ""),
            "paper_trade_construction_id": str(latest_record.get("paper_trade_construction_id") or case.get("paper_trade_construction_id") or construction.get("construction_id") or ""),
            "submit_boundary_status": "CAPTURE_TIME_VALIDATED",
            "submit_boundary_id": str(original_lineage.get("submit_boundary_id") or ""),
            "submit_boundary_hash": str(original_lineage.get("submit_boundary_hash") or ""),
            "entry_reference_price": construction.get("entry_reference_price") or "",
            "entry": construction.get("entry_reference_price") or "",
            "suggested_quantity": construction.get("suggested_quantity"),
            "quantity": latest_record.get("quantity") if latest_record else construction.get("suggested_quantity"),
            "suggested_notional": construction.get("suggested_notional") or "",
            "notional": construction.get("suggested_notional") or "",
            "stop_price": latest_record.get("stop_price") or construction.get("stop_price") or "",
            "stop": latest_record.get("stop_price") or construction.get("stop_price") or construction.get("invalidation_level") or "",
            "risk": construction.get("max_loss_estimate") or construction.get("risk_per_share") or "",
            "max_loss_estimate": construction.get("max_loss_estimate") or "",
            "manual_capture_ready": False,
            "capture_ready": False,
            "manual_capture_only": True,
            "manual_capture_mode": "manual_capture_only",
            "capture_save_ready": False,
            "submit_boundary_validated_for_save": False,
            "paper_submit_ready": False,
            "execution_ready": False,
            "captured_manually_allowed": False,
            "capture_as_trade_disabled": True,
            "capture_editing_disabled": True,
            "manual_capture_record": dict(latest_record),
            "manual_capture_records": [dict(row) for row in records if isinstance(row, Mapping)],
            "capture_record_id": str(latest_record.get("record_id") or latest_record.get("manual_capture_record_id") or ""),
            "event_ids": [str(item) for item in latest_record.get("event_ids") or [] if str(item)],
            "captured_at_utc": str(latest_record.get("fill_time") or latest_record.get("created_at_utc") or ""),
            "operator_id": str(latest_record.get("operator_id") or ""),
            "fill_price": str(latest_record.get("fill_price") or ""),
            "fill_time": str(latest_record.get("fill_time") or ""),
            "capture_status": "Manual capture recorded",
            "allowed_actions": ["view_evidence", "export_capture"],
            "disallowed_actions": ["captured_manually", "broker_execution", "execution", "live_order", "paper_submit", "paper_submit_automation", "real_capital_allocation"],
            "blockers_by_domain": blockers,
            "blockers": [],
            "next_required_actions": [],
            "next_action": "Manual capture has been recorded; show the read-only historical record.",
            "trade_construction_status": str(construction.get("trade_construction_status") or "complete"),
            "trade_ticket_status": "captured_historical",
            "readiness_summary": [
                "Manual capture RECORDED",
                "Historical record READ-ONLY",
                "Broker submit DISABLED",
                "No broker action was taken",
            ],
            "broker_submit_status": "DISABLED",
            "ib_api_handshake_manual_capture_requirement": "NOT_REQUIRED",
            "paper_trade_construction": dict(construction),
            "paper_trade_construction_v1": dict(construction),
            "trade_ticket_lineage_v1": dict(original_lineage),
            "submit_boundary_precheck_v1": dict(submit_precheck),
            "trade_lifecycle_case": dict(case),
            "trade_lifecycle_case_v1": dict(case),
            "broker_execution_allowed": False,
            "live_trading_allowed": False,
            "order_routing_allowed": False,
            "capital_allocation_allowed": False,
            "paper_submit_created": False,
            "selected_candidate_mutation_allowed": False,
        }
        payload["artifact_id"] = f"trade_ticket_projection_v1:{payload['source_day']}:{_stable_hash(payload)[:20]}"
        payload["immutable_hash"] = _stable_hash({**payload, "immutable_hash": ""})
        return payload
    manual_ready = statuses.get("manual_capture") == "READY"
    capture_save_ready = manual_ready and lineage_status == "ACTIVE_CURRENT" and submit_valid
    paper_submit_ready = statuses.get("paper_submit") == "READY"
    execution_ready = statuses.get("execution") == "READY"
    allowed_actions = ["not_captured_record", "skipped_record", "blocked_record", "notes"]
    disallowed_actions = ["broker_execution", "live_order", "real_capital_allocation", "paper_submit_automation"]
    if manual_ready:
        allowed_actions.insert(0, "captured_manually")
    else:
        disallowed_actions.append("captured_manually")
    if paper_submit_ready:
        allowed_actions.append("paper_submit")
    else:
        disallowed_actions.append("paper_submit")
    if not execution_ready:
        disallowed_actions.append("execution")
    next_required_actions = []
    for domain in DOMAIN_ORDER:
        status = statuses.get(domain, "")
        if status and status not in {"READY", "NOT_REQUIRED"}:
            next_required_actions.append({"domain": domain, "status": status, "action": _action_for_domain(domain)})
    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": "",
        "trade_lifecycle_case_id": str(case.get("trade_lifecycle_case_id") or ""),
        "case_id": str(case.get("trade_lifecycle_case_id") or ""),
        "symbol": str(case.get("symbol") or construction.get("symbol") or "").upper(),
        "direction": str(case.get("direction") or construction.get("direction") or ""),
        "sleeve_id": str(case.get("sleeve_id") or construction.get("sleeve_id") or ""),
        "sleeve": str(case.get("sleeve_id") or construction.get("sleeve_id") or ""),
        "engine_id": str(case.get("engine_id") or construction.get("engine_id") or ""),
        "source_day": str(case.get("source_day") or construction.get("source_day") or ""),
        "source_run_id": str(case.get("source_run_id") or construction.get("source_run_id") or ""),
        "selected_exposure_intent_id": str(case.get("selected_exposure_intent_id") or construction.get("selected_exposure_intent_id") or ""),
        "ticket_id": str(lineage.get("ticket_id") or ""),
        "lineage_status": lineage_status,
        "editable": capture_save_ready,
        "read_only": not capture_save_ready,
        "runtime_evaluation_hash": str(lineage.get("runtime_evaluation_hash") or construction.get("runtime_evaluation_hash") or ""),
        "construction_contract_id": str(lineage.get("construction_contract_id") or construction.get("construction_contract_id") or ""),
        "construction_contract_hash": str(lineage.get("construction_contract_hash") or construction.get("construction_contract_hash") or ""),
        "paper_trade_construction_id": str(case.get("paper_trade_construction_id") or construction.get("construction_id") or ""),
        "submit_boundary_status": str(submit_precheck.get("validation_status") or "MISSING"),
        "submit_boundary_id": str(submit_precheck.get("submit_boundary_id") or ""),
        "submit_boundary_hash": str(submit_precheck.get("submit_boundary_hash") or ""),
        "market_freshness_status": str((lineage.get("evidence_status") or {}).get("market_freshness") or "MISSING") if isinstance(lineage.get("evidence_status"), Mapping) else "MISSING",
        "paper_intent_status": str((lineage.get("evidence_status") or {}).get("paper_intent") or "MISSING") if isinstance(lineage.get("evidence_status"), Mapping) else "MISSING",
        "conversion_evidence_status": str((lineage.get("evidence_status") or {}).get("conversion") or "MISSING") if isinstance(lineage.get("evidence_status"), Mapping) else "MISSING",
        "lifecycle_state": str(case.get("current_state") or ""),
        "current_state": str(case.get("current_state") or ""),
        "state_reason": str(case.get("state_reason") or ""),
        "domain_statuses": {domain: statuses.get(domain, "") for domain in DOMAIN_ORDER},
        "readiness_domain_evaluations": [dict(row) for row in evaluations],
        "entry_reference_price": construction.get("entry_reference_price") or "",
        "entry": construction.get("entry_reference_price") or "",
        "suggested_quantity": construction.get("suggested_quantity"),
        "quantity": construction.get("suggested_quantity"),
        "suggested_notional": construction.get("suggested_notional") or "",
        "notional": construction.get("suggested_notional") or "",
        "allocation_percent": construction.get("allocation_percent") or "",
        "stop_price": construction.get("stop_price") or "",
        "invalidation_level": construction.get("invalidation_level") or "",
        "stop": construction.get("stop_price") or construction.get("invalidation_level") or "",
        "stop_policy_source": construction.get("stop_policy_source") or "",
        "stop_policy_id": construction.get("stop_policy_id") or "",
        "stop_loss_bps": construction.get("stop_loss_bps") or "",
        "invalidation_policy": construction.get("invalidation_policy") if isinstance(construction.get("invalidation_policy"), Mapping) else {},
        "expected_holding_days": construction.get("expected_holding_days") or "",
        "risk_per_share": construction.get("risk_per_share") or "",
        "max_loss_estimate": construction.get("max_loss_estimate") or "",
        "estimated_notional_risk_pct": construction.get("estimated_notional_risk_pct") or "",
        "risk": construction.get("max_loss_estimate") or construction.get("risk_per_share") or "",
        "manual_capture_ready": manual_ready,
        "capture_ready": manual_ready,
        "manual_capture_only": True,
        "manual_capture_mode": "manual_capture_only",
        "capture_save_ready": capture_save_ready,
        "submit_boundary_validated_for_save": submit_valid,
        "paper_submit_ready": paper_submit_ready,
        "execution_ready": execution_ready,
        "captured_manually_allowed": manual_ready,
        "capture_as_trade_disabled": not manual_ready,
        "capture_editing_disabled": not capture_save_ready,
        "allowed_actions": sorted(set(allowed_actions)),
        "disallowed_actions": sorted(set(disallowed_actions)),
        "blockers_by_domain": blockers,
        "blockers": [blocker for domain in DOMAIN_ORDER for blocker in blockers.get(domain, [])],
        "next_required_actions": next_required_actions,
        "next_action": next_required_actions[0]["action"] if next_required_actions else "Ready for manual paper capture.",
        "trade_construction_status": str(construction.get("trade_construction_status") or ""),
        "trade_ticket_status": "complete" if manual_ready else "blocked",
        "capture_status": "Ready for manual paper capture" if manual_ready else "Not capture-ready",
        "readiness_summary": [
            "Manual capture READY" if manual_ready else "Manual capture BLOCKED",
            "Submit-boundary VALIDATED" if submit_valid else "Submit-boundary NOT VALIDATED",
            "Broker submit DISABLED",
            "IB handshake NOT REQUIRED FOR MANUAL CAPTURE",
        ],
        "broker_submit_status": "DISABLED",
        "ib_api_handshake_manual_capture_requirement": "NOT_REQUIRED",
        "manual_capture_record": dict(latest_record),
        "manual_capture_records": records,
        "market_data_latest_session": str(construction.get("market_data_latest_session") or ""),
        "latest_market_session": str(construction.get("market_data_latest_session") or ""),
        "required_market_session": str(construction.get("source_day") or case.get("source_day") or ""),
        "paper_trade_construction": dict(construction),
        "paper_trade_construction_v1": dict(construction),
        "trade_ticket_lineage_v1": dict(lineage),
        "submit_boundary_precheck_v1": dict(submit_precheck),
        "trade_lifecycle_case": dict(case),
        "trade_lifecycle_case_v1": dict(case),
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
        "order_routing_allowed": False,
        "capital_allocation_allowed": False,
        "paper_submit_created": False,
        "selected_candidate_mutation_allowed": False,
    }
    payload["artifact_id"] = f"trade_ticket_projection_v1:{payload['source_day']}:{_stable_hash(payload)[:20]}"
    payload["immutable_hash"] = _stable_hash({**payload, "immutable_hash": ""})
    return payload


def write_trade_ticket_projection_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any]) -> Path:
    path = trade_ticket_projection_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(dict(payload)) + b"\n")
    return path


def latest_trade_ticket_projection_v1(*, truth_root: Path | str, day_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc or "")
    if not day:
        base = root / "reports" / REPORT_FAMILY
        days = sorted(path.name for path in base.iterdir() if path.is_dir()) if base.exists() else []
        day = days[-1] if days else ""
    if not day:
        return {}
    path = trade_ticket_projection_path_v1(truth_root=root, day_utc=day)
    payload = _read_json(path)
    if payload:
        payload["artifact_path"] = str(path)
    return payload
