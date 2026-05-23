from __future__ import annotations

from typing import Any, Mapping


SCHEMA_ID = "trade_case_projection"
SCHEMA_VERSION = "v1"


def trade_case_projection_v1(case: Mapping[str, Any]) -> dict[str, Any]:
    construction = case.get("paper_trade_construction") if isinstance(case.get("paper_trade_construction"), Mapping) else {}
    state = str(case.get("current_state") or "OBSERVED")
    records = case.get("manual_capture_records") if isinstance(case.get("manual_capture_records"), list) else []
    latest_record = records[-1] if records and isinstance(records[-1], Mapping) else {}
    if state in {"CAPTURED_MANUALLY", "CAPTURED_HISTORICAL"} or str(latest_record.get("capture_status") or "").lower() in {"captured_manually", "partial"}:
        return {
            "schema_id": SCHEMA_ID,
            "schema_version": SCHEMA_VERSION,
            "case_id": str(case.get("trade_lifecycle_case_id") or ""),
            "trade_lifecycle_case_id": str(case.get("trade_lifecycle_case_id") or ""),
            "state": "CAPTURED_HISTORICAL",
            "current_state": "CAPTURED_HISTORICAL",
            "state_reason": str(case.get("state_reason") or "Immutable manual capture evidence is attached."),
            "symbol": str(case.get("symbol") or latest_record.get("symbol") or construction.get("symbol") or "").upper(),
            "direction": str(case.get("direction") or construction.get("direction") or ""),
            "sleeve": str(case.get("sleeve_id") or latest_record.get("sleeve") or construction.get("sleeve_id") or ""),
            "sleeve_id": str(case.get("sleeve_id") or latest_record.get("sleeve") or construction.get("sleeve_id") or ""),
            "engine_id": str(case.get("engine_id") or construction.get("engine_id") or ""),
            "selected_exposure_intent_id": str(case.get("selected_exposure_intent_id") or latest_record.get("selected_exposure_intent_id") or construction.get("selected_exposure_intent_id") or ""),
            "paper_trade_construction_id": str(case.get("paper_trade_construction_id") or latest_record.get("paper_trade_construction_id") or construction.get("construction_id") or ""),
            "manual_capture_candidate_id": "",
            "entry": construction.get("entry_reference_price") or "",
            "entry_reference_price": construction.get("entry_reference_price") or "",
            "quantity": latest_record.get("quantity") if latest_record else construction.get("suggested_quantity"),
            "suggested_quantity": construction.get("suggested_quantity"),
            "notional": construction.get("suggested_notional") or "",
            "suggested_notional": construction.get("suggested_notional") or "",
            "stop": latest_record.get("stop_price") or construction.get("stop_price") or construction.get("invalidation_level") or "",
            "stop_price": latest_record.get("stop_price") or construction.get("stop_price") or "",
            "risk": construction.get("max_loss_estimate") or construction.get("risk_per_share") or "",
            "max_loss_estimate": construction.get("max_loss_estimate") or "",
            "blockers": [],
            "blocker_codes": [],
            "blocker_messages": [],
            "missing_fields": [],
            "capture_ready": False,
            "manual_capture_ready": False,
            "manual_capture_record": dict(latest_record),
            "manual_capture_records": records,
            "trade_construction_status": "captured_historical",
            "trade_ticket_status": "captured_historical",
            "capture_status": "Manual capture recorded",
            "captured_manually_allowed": False,
            "capture_as_trade_disabled": True,
            "capture_editing_disabled": True,
            "candidate_available": True,
            "next_action": "Manual capture has been recorded; show the read-only historical record.",
            "next_required_action": "Manual capture has been recorded; show the read-only historical record.",
            "source_day": str(case.get("source_day") or construction.get("source_day") or ""),
            "source_run_id": str(case.get("source_run_id") or construction.get("source_run_id") or ""),
            "submit_boundary_status": "CAPTURE_TIME_VALIDATED",
            "paper_trade_construction": dict(construction),
            "paper_trade_construction_v1": dict(construction),
            "trade_lifecycle_case": dict(case),
            "trade_ticket_projection_v1": dict(construction),
            "broker_execution_allowed": False,
            "live_trading_allowed": False,
            "order_routing_allowed": False,
            "capital_allocation_allowed": False,
            "paper_submit_created": False,
        }
    status = str(construction.get("trade_construction_status") or ("complete" if state == "CAPTURE_READY" else "blocked_incomplete_trade_definition"))
    capture_ready = state == "CAPTURE_READY"
    blocker_codes = [str(code) for code in case.get("blocker_codes") or construction.get("blocker_codes") or []]
    blocker_messages = [str(message) for message in case.get("blocker_messages") or construction.get("blocker_messages") or []]
    blockers = [
        {"code": code, "message": blocker_messages[index] if index < len(blocker_messages) else code}
        for index, code in enumerate(blocker_codes)
    ]
    next_action = _next_action(state, status)
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "case_id": str(case.get("trade_lifecycle_case_id") or ""),
        "trade_lifecycle_case_id": str(case.get("trade_lifecycle_case_id") or ""),
        "state": state,
        "current_state": state,
        "state_reason": str(case.get("state_reason") or ""),
        "symbol": str(case.get("symbol") or construction.get("symbol") or "").upper(),
        "direction": str(case.get("direction") or construction.get("direction") or ""),
        "sleeve": str(case.get("sleeve_id") or construction.get("sleeve_id") or ""),
        "sleeve_id": str(case.get("sleeve_id") or construction.get("sleeve_id") or ""),
        "engine_id": str(case.get("engine_id") or construction.get("engine_id") or ""),
        "selected_exposure_intent_id": str(case.get("selected_exposure_intent_id") or construction.get("selected_exposure_intent_id") or ""),
        "paper_trade_construction_id": str(case.get("paper_trade_construction_id") or construction.get("construction_id") or ""),
        "manual_capture_candidate_id": str(case.get("manual_capture_candidate_id") or ""),
        "entry": construction.get("entry_reference_price") or "",
        "entry_reference_price": construction.get("entry_reference_price") or "",
        "quantity": construction.get("suggested_quantity"),
        "suggested_quantity": construction.get("suggested_quantity"),
        "notional": construction.get("suggested_notional") or "",
        "suggested_notional": construction.get("suggested_notional") or "",
        "allocation_percent": construction.get("allocation_percent") or "",
        "stop": construction.get("stop_price") or construction.get("invalidation_level") or "",
        "stop_price": construction.get("stop_price") or "",
        "invalidation_level": construction.get("invalidation_level") or "",
        "risk": construction.get("max_loss_estimate") or construction.get("risk_per_share") or "",
        "risk_per_share": construction.get("risk_per_share") or "",
        "max_loss_estimate": construction.get("max_loss_estimate") or "",
        "blockers": blockers,
        "blocker_codes": blocker_codes,
        "blocker_messages": blocker_messages,
        "missing_fields": construction.get("missing_fields") if isinstance(construction.get("missing_fields"), list) else [],
        "capture_ready": capture_ready,
        "manual_capture_ready": capture_ready,
        "manual_capture_records": case.get("manual_capture_records") if isinstance(case.get("manual_capture_records"), list) else [],
        "trade_construction_status": status,
        "trade_ticket_status": status,
        "capture_status": "Ready for manual paper capture" if capture_ready else "Not capture-ready",
        "captured_manually_allowed": capture_ready,
        "capture_as_trade_disabled": not capture_ready,
        "candidate_available": bool(case.get("selected_exposure_intent_id") or construction.get("selected_exposure_intent_id")),
        "next_action": next_action,
        "next_required_action": next_action,
        "source_day": str(case.get("source_day") or construction.get("source_day") or ""),
        "source_run_id": str(case.get("source_run_id") or construction.get("source_run_id") or ""),
        "market_data_latest_session": str(construction.get("market_data_latest_session") or ""),
        "latest_market_session": str(construction.get("market_data_latest_session") or ""),
        "required_market_session": str(construction.get("source_day") or case.get("source_day") or ""),
        "stale_market_data": construction.get("market_data_latest_session") != construction.get("source_day"),
        "submit_boundary_status": str(construction.get("submit_boundary_status") or case.get("submit_boundary_status") or ""),
        "paper_trade_construction": dict(construction),
        "paper_trade_construction_v1": dict(construction),
        "trade_lifecycle_case": dict(case),
        "trade_ticket_projection_v1": dict(construction),
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
        "order_routing_allowed": False,
        "capital_allocation_allowed": False,
        "paper_submit_created": False,
    }
    payload["data_freshness_status"] = {
        "status": "CURRENT" if payload["latest_market_session"] == payload["required_market_session"] else "STALE_OR_MISSING",
        "expected_session": payload["required_market_session"],
        "observed_session": payload["latest_market_session"],
        "current": payload["latest_market_session"] == payload["required_market_session"],
    }
    return payload


def _next_action(state: str, status: str) -> str:
    if state == "CAPTURE_READY":
        return "Ready for manual paper capture."
    if state == "CAPTURED_MANUALLY":
        return "Manual capture has been recorded; continue lifecycle tracking."
    if state == "SKIPPED":
        return "Case was explicitly skipped by the operator."
    if status == "blocked_missing_market_data":
        return "Refresh canonical market data, rerun paper construction, then rebuild the trade lifecycle case."
    if status == "blocked_missing_capital_authority":
        return "Generate paper capital authority, rerun paper construction, then rebuild the case."
    if status == "blocked_missing_stop_policy":
        return "Generate stop or invalidation policy, rerun paper construction, then rebuild the case."
    if status == "blocked_missing_sizing_policy":
        return "Generate sizing policy or approved quantity, rerun paper construction, then rebuild the case."
    return "Resolve construction blockers, rerun paper construction, then rebuild the trade lifecycle case."
