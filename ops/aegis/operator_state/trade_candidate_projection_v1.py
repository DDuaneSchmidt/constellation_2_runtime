from __future__ import annotations

import argparse
import json
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from ops.aegis.operator_state.current_operator_truth_resolver_v1 import resolve_current_operator_truth_v1
from ops.aegis.operator_state.trade_candidate_contract_v1 import (
    SCHEMA_VERSION,
    blocker_v1,
    canonical_trade_candidate_id_v1,
    content_hash_file_v1,
    first_text_v1,
    is_positive_decimal_text_v1,
    nested_dict_v1,
    normalize_decimal_text_v1,
    now_iso_v1,
    positive_int_or_none_v1,
    read_json_object_v1,
    stable_hash_v1,
)

REPORT_FAMILY = "trade_candidate_projection_v1"
REPORT_FILENAME = "trade_candidate_projection.v1.json"
SCHEMA_ID = "trade_candidate_projection"


def trade_candidate_projection_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def build_trade_candidate_projection_v1(
    *,
    truth_root: Path | str,
    day_utc: str | None = None,
    current_operator_truth: Mapping[str, Any] | None = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated = generated_at_utc or now_iso_v1()
    current_truth = dict(current_operator_truth or resolve_current_operator_truth_v1(truth_root=root, day_utc=day_utc, generated_at_utc=generated))
    root = Path(str(current_truth.get("truth_root") or root)).expanduser().resolve()
    source_day = str(current_truth.get("source_day") or day_utc or generated[:10])
    selected = nested_dict_v1(current_truth, "selected_exposure")
    selected_row = nested_dict_v1(current_truth, "selected_candidate_row")
    conversion = nested_dict_v1(current_truth, "conversion")
    market = nested_dict_v1(conversion, "market_data_status")
    selected_intent = _selected_intent_payload(selected)
    latest_capture = _latest_capture_record(root=root, day_utc=source_day, selected_id=str(selected.get("candidate_id") or ""))
    submit_boundary = _submit_boundary_status(root=root, day_utc=source_day)

    selected_id = first_text_v1(selected.get("candidate_id"), selected.get("selected_exposure_intent_id"), selected_row.get("candidate_id"), selected_row.get("intent_candidate_id"))
    source_run_id = str(current_truth.get("source_run_id") or "")
    trade_candidate_id = canonical_trade_candidate_id_v1(selected_exposure_intent_id=selected_id, source_day=source_day, source_run_id=source_run_id)
    symbol = first_text_v1(selected.get("symbol"), selected_row.get("symbol"), nested_dict_v1(selected_intent, "underlying").get("symbol"), conversion.get("symbol")).upper()
    direction = first_text_v1(selected.get("direction"), selected_row.get("direction"), selected_row.get("proposed_direction"), _direction_from_intent(selected_intent))
    sleeve_id = first_text_v1(selected.get("sleeve_id"), selected_row.get("sleeve_id"), selected.get("engine_id"), nested_dict_v1(selected_intent, "engine").get("engine_id"))
    engine_id = first_text_v1(selected.get("engine_id"), selected_row.get("engine_id"), sleeve_id)

    entry_reference_price = normalize_decimal_text_v1(
        first_text_v1(
            conversion.get("entry_reference_price"),
            conversion.get("entry_price"),
            conversion.get("reference_price"),
            selected_row.get("entry_reference_price"),
            selected_row.get("entry_price"),
            selected_intent.get("entry_reference_price"),
            market.get("last_price"),
            market.get("close"),
            market.get("close_price"),
        )
    )
    quantity = positive_int_or_none_v1(
        first_text_v1(
            conversion.get("quantity"),
            conversion.get("quantity_shares"),
            conversion.get("suggested_quantity"),
            conversion.get("final_quantity"),
            selected_row.get("quantity"),
            selected_row.get("suggested_quantity"),
            selected_intent.get("quantity"),
            selected_intent.get("quantity_shares"),
        )
    )
    allocation_percent = first_text_v1(
        conversion.get("allocation_percent"),
        conversion.get("target_notional_pct"),
        selected_row.get("allocation_percent"),
        selected_intent.get("target_notional_pct"),
    )
    constraints = nested_dict_v1(selected_intent, "constraints")
    risk_budget_reference = first_text_v1(
        conversion.get("risk_budget_reference"),
        conversion.get("risk_budget_pct"),
        selected_row.get("risk_budget_reference"),
        constraints.get("max_risk_pct"),
        allocation_percent,
    )
    notional_value = normalize_decimal_text_v1(first_text_v1(conversion.get("notional_value"), conversion.get("notional"), selected_row.get("notional_value")))
    stop_price = normalize_decimal_text_v1(first_text_v1(conversion.get("stop_price"), selected_row.get("stop_price"), selected_intent.get("stop_price")))
    invalidation_level = first_text_v1(conversion.get("invalidation_level"), selected_row.get("invalidation_level"), selected_intent.get("invalidation_level"), selected_row.get("stop_logic"), selected_intent.get("stop_logic"))
    if not stop_price:
        stop_price = _derived_stop_price(entry_reference_price=entry_reference_price, direction=direction, selected_intent=selected_intent)
    risk_per_share = _risk_per_share(entry_reference_price, stop_price)
    max_loss_estimate = _max_loss(risk_per_share, quantity)
    if not notional_value:
        notional_value = _notional_value(entry_reference_price, quantity)
    target_exit_rule = first_text_v1(conversion.get("target_exit_rule"), conversion.get("exit_rule"), selected_row.get("target_exit_rule"), selected_intent.get("exit_rule"))
    holding_period = first_text_v1(conversion.get("holding_period"), selected_row.get("holding_period"), selected_intent.get("holding_period"))
    review_date = first_text_v1(conversion.get("review_date"), selected_row.get("review_date"), selected_intent.get("review_date"))

    conversion_blocker_code = str(conversion.get("blocker_code") or "").strip()
    conversion_blocker_message = str(conversion.get("blocker_message") or "").strip()
    blockers = _required_blockers(
        selected_id=selected_id,
        symbol=symbol,
        entry_reference_price=entry_reference_price,
        quantity=quantity,
        allocation_percent=allocation_percent,
        risk_budget_reference=risk_budget_reference,
        stop_price=stop_price,
        invalidation_level=invalidation_level,
        current_truth=current_truth,
        conversion=conversion,
        market=market,
        submit_boundary=submit_boundary,
    )
    capture_status = str(latest_capture.get("capture_status") or "not_captured")
    readiness = _readiness_status(
        blockers=blockers,
        conversion=conversion,
        latest_capture=latest_capture,
        quantity=quantity,
        entry_reference_price=entry_reference_price,
        stop_price=stop_price,
        invalidation_level=invalidation_level,
        risk_budget_reference=risk_budget_reference,
        allocation_percent=allocation_percent,
    )
    trade_ticket = build_trade_ticket_projection_v1(
        selected_id=selected_id,
        symbol=symbol,
        direction=direction,
        entry_reference_price=entry_reference_price,
        quantity=quantity,
        suggested_notional=notional_value,
        allocation_percent=allocation_percent,
        risk_budget_reference=risk_budget_reference,
        stop_price=stop_price,
        invalidation_level=invalidation_level,
        risk_per_share=risk_per_share,
        max_loss_estimate=max_loss_estimate,
        blockers=blockers,
        conversion=conversion,
        market=market,
        selected_intent=selected_intent,
        current_truth=current_truth,
        submit_boundary=submit_boundary,
    )
    operator_action_allowed = trade_ticket["trade_ticket_status"] == "complete"
    disallowed_actions = [
        "broker_execution",
        "live_order",
        "order_routing",
        "capital_allocation",
        "paper_submit",
        "selected_candidate_mutation",
    ]
    if not operator_action_allowed:
        disallowed_actions.append("captured_manually_without_required_fields_or_policy")

    projection_core: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": generated,
        "day_utc": source_day,
        "trade_candidate_id": trade_candidate_id,
        "candidate_available": bool(selected_id),
        "exposure_intent_id": selected_id,
        "selected_exposure_intent_id": selected_id,
        "sleeve_id": sleeve_id,
        "engine_id": engine_id,
        "symbol": symbol,
        "direction": direction,
        "source_day": source_day,
        "source_run_id": source_run_id,
        "selected_by_arbitration": bool(selected.get("selected_by_arbitration") or selected_id),
        "selected_rank": selected_row.get("rank_before_arbitration") or selected_row.get("rank") or "",
        "suppression_watchlist_context": {
            "suppressed_count": int(current_truth.get("suppressed_count") or 0),
            "suppression_code_counts": current_truth.get("suppression_code_counts") if isinstance(current_truth.get("suppression_code_counts"), dict) else {},
        },
        "entry_reference_price": entry_reference_price,
        "fill_price": str(latest_capture.get("fill_price") or ""),
        "quantity": quantity,
        "notional_value": notional_value,
        "allocation_percent": allocation_percent,
        "risk_budget_reference": risk_budget_reference,
        "stop_price": stop_price,
        "invalidation_level": invalidation_level,
        "target_exit_rule": target_exit_rule,
        "time_in_force": first_text_v1(conversion.get("time_in_force"), selected_row.get("time_in_force"), selected_intent.get("time_in_force")),
        "holding_period": holding_period,
        "review_date": review_date,
        "risk_per_share": risk_per_share,
        "max_loss_estimate": max_loss_estimate,
        "trade_ticket_status": trade_ticket["trade_ticket_status"],
        "trade_ticket_projection_v1": trade_ticket,
        "suggested_quantity": trade_ticket["suggested_quantity"],
        "suggested_notional": trade_ticket["suggested_notional"],
        "suggested_allocation_percent": trade_ticket["suggested_allocation_percent"],
        "capture_status": trade_ticket["capture_status"],
        "blocker_codes": trade_ticket["blocker_codes"],
        "next_required_action": trade_ticket["next_required_action"],
        "thesis_reason": first_text_v1(conversion.get("thesis"), conversion.get("reason"), selected_row.get("reason"), selected_row.get("suppression_reason"), selected_intent.get("thesis"), selected_id),
        "blocker_status": "BLOCKED" if blockers else "CLEAR",
        "blockers": blockers,
        "missing_required_fields": [row["field"] for row in blockers if row.get("field")],
        "data_freshness_status": _freshness_status(market, source_day),
        "current_truth_snapshot_id": _current_truth_snapshot_id(current_truth),
        "symbol_authority_source": "selected_exposure" if symbol else "",
        "market_data_snapshot_id": first_text_v1(market.get("snapshot_id"), market.get("path")),
        "portfolio_gate_decision_id": source_run_id if str(current_truth.get("current_truth_status") or "") in {"CURRENT_OK", "CURRENT"} else "",
        "conversion_blocker_code": conversion_blocker_code,
        "conversion_blocker_message": conversion_blocker_message,
        "conversion_status": str(current_truth.get("conversion_status") or conversion.get("status") or ""),
        "submit_boundary_status": str(submit_boundary.get("status") or ""),
        "submit_boundary_status_path": str(submit_boundary.get("path") or ""),
        "paper_intent_status": "CREATED" if conversion.get("paper_trade_intent_created") is True else "NOT_CREATED",
        "lifecycle_status": readiness,
        "operator_action_allowed": operator_action_allowed,
        "disallowed_actions": disallowed_actions,
        "manual_capture_record": {
            "capture_status": capture_status,
            "quantity": latest_capture.get("quantity"),
            "fill_price": str(latest_capture.get("fill_price") or ""),
            "fill_time": str(latest_capture.get("fill_time") or ""),
            "stop_price": str(latest_capture.get("stop_price") or ""),
            "invalidation_level": str(latest_capture.get("invalidation_level") or ""),
            "operator_id": str(latest_capture.get("operator_id") or ""),
            "notes": str(latest_capture.get("notes") or ""),
            "external_reference": str(latest_capture.get("external_reference") or ""),
            "blocker_snapshot": latest_capture.get("blocker_snapshot") if isinstance(latest_capture.get("blocker_snapshot"), dict) else {},
            "immutable_hash": str(latest_capture.get("immutable_hash") or latest_capture.get("content_hash") or ""),
        },
        "source_artifacts": _source_artifacts(current_truth=current_truth, selected=selected, selected_intent=selected_intent, submit_boundary=submit_boundary),
        "safety": {
            "broker_execution_allowed": False,
            "live_trading_allowed": False,
            "order_routing_allowed": False,
            "capital_allocation_allowed": False,
            "paper_submit_created_by_projection": False,
            "candidate_selection_mutated": False,
        },
    }
    projection_id = "trade-candidate-projection:" + stable_hash_v1(projection_core)[:24]
    projection = {
        **projection_core,
        "projection_id": projection_id,
        "artifact_id": f"trade_candidate_projection_v1:{source_day}:{projection_id.rsplit(':', 1)[-1]}",
    }
    projection["canonical_json_hash"] = stable_hash_v1({**projection, "canonical_json_hash": ""})
    return projection


def write_trade_candidate_projection_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any]) -> Path:
    path = trade_candidate_projection_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(dict(payload)) + b"\n")
    return path


def build_and_write_trade_candidate_projection_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None) -> tuple[dict[str, Any], Path]:
    payload = build_trade_candidate_projection_v1(truth_root=truth_root, day_utc=day_utc, generated_at_utc=generated_at_utc)
    path = write_trade_candidate_projection_v1(truth_root=truth_root, day_utc=str(payload.get("day_utc") or day_utc), payload=payload)
    payload["artifact_path"] = str(path)
    return payload, path


def read_trade_candidate_projection_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    return read_json_object_v1(trade_candidate_projection_path_v1(truth_root=truth_root, day_utc=day_utc))

def manual_capture_view_from_projection_v1(projection: Mapping[str, Any]) -> dict[str, Any]:
    blockers = projection.get("blockers") if isinstance(projection.get("blockers"), list) else []
    trade_ticket = projection.get("trade_ticket_projection_v1") if isinstance(projection.get("trade_ticket_projection_v1"), Mapping) else {}
    ticket_status = str(projection.get("trade_ticket_status") or trade_ticket.get("trade_ticket_status") or "")
    conversion_blocker = str(projection.get("conversion_blocker_code") or "")
    conversion_message = str(projection.get("conversion_blocker_message") or "")
    status = str(projection.get("lifecycle_status") or "")
    return {
        "schema_id": "manual_capture_candidate",
        "schema_version": "v1_from_trade_candidate_projection",
        "trade_candidate_projection_id": str(projection.get("projection_id") or ""),
        "trade_candidate_id": str(projection.get("trade_candidate_id") or ""),
        "candidate_available": bool(projection.get("candidate_available")),
        "source_day": str(projection.get("source_day") or ""),
        "latest_report_day": str(projection.get("day_utc") or projection.get("source_day") or ""),
        "source_run_id": str(projection.get("source_run_id") or ""),
        "stale_source": False,
        "source_mismatch_warning": "",
        "selected_exposure_intent_id": str(projection.get("selected_exposure_intent_id") or ""),
        "symbol": str(projection.get("symbol") or ""),
        "sleeve_id": str(projection.get("sleeve_id") or ""),
        "engine_id": str(projection.get("engine_id") or ""),
        "direction": str(projection.get("direction") or ""),
        "selected_rank": projection.get("selected_rank"),
        "selected_by_arbitration": bool(projection.get("selected_by_arbitration")),
        "paper_trade_intent_created": projection.get("paper_intent_status") == "CREATED",
        "paper_intent_created": projection.get("paper_intent_status") == "CREATED",
        "blocker_code": conversion_blocker,
        "blocker_message": conversion_message,
        "conversion_status": str(projection.get("conversion_status") or ""),
        "blockers": blockers,
        "trade_ticket_status": ticket_status,
        "trade_ticket_projection_v1": dict(trade_ticket),
        "missing_ticket_fields": trade_ticket.get("missing_fields") if isinstance(trade_ticket.get("missing_fields"), list) else [],
        "next_required_action": str(trade_ticket.get("next_required_action") or projection.get("next_required_action") or ""),
        "entry_reference_price": str(projection.get("entry_reference_price") or ""),
        "quantity": projection.get("quantity"),
        "suggested_quantity": projection.get("suggested_quantity"),
        "notional_value": str(projection.get("notional_value") or ""),
        "suggested_notional": str(projection.get("suggested_notional") or ""),
        "allocation_percent": str(projection.get("allocation_percent") or ""),
        "suggested_allocation_percent": str(projection.get("suggested_allocation_percent") or ""),
        "risk_budget_reference": str(projection.get("risk_budget_reference") or ""),
        "stop_price": str(projection.get("stop_price") or ""),
        "invalidation_level": str(projection.get("invalidation_level") or ""),
        "risk_per_share": str(projection.get("risk_per_share") or ""),
        "max_loss_estimate": str(projection.get("max_loss_estimate") or ""),
        "fill_price": str(projection.get("fill_price") or ""),
        "fill_time": str((projection.get("manual_capture_record") or {}).get("fill_time") if isinstance(projection.get("manual_capture_record"), Mapping) else ""),
        "capture_status": str((projection.get("manual_capture_record") or {}).get("capture_status") if isinstance(projection.get("manual_capture_record"), Mapping) else ""),
        "paper_intent_status": str(projection.get("paper_intent_status") or ""),
        "submit_boundary_status": str(projection.get("submit_boundary_status") or ""),
        "data_freshness_status": projection.get("data_freshness_status") if isinstance(projection.get("data_freshness_status"), Mapping) else {},
        "latest_market_session": str((projection.get("data_freshness_status") or {}).get("observed_session") if isinstance(projection.get("data_freshness_status"), Mapping) else ""),
        "required_market_session": str((projection.get("data_freshness_status") or {}).get("expected_session") if isinstance(projection.get("data_freshness_status"), Mapping) else ""),
        "stale_market_data": _has_blocker(blockers, "STALE_MARKET_DATA_BLOCKS_CONVERSION"),
        "suppressed_count": int(((projection.get("suppression_watchlist_context") or {}).get("suppressed_count") if isinstance(projection.get("suppression_watchlist_context"), Mapping) else 0) or 0),
        "operator_action_label": _operator_action_label(status),
        "operator_action_allowed": bool(projection.get("operator_action_allowed")),
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "order_routing_allowed": False,
        "captured_manually_allowed": ticket_status == "complete",
        "capture_as_trade_disabled": ticket_status != "complete",
        "capture_editing_disabled": False,
    }


def _selected_intent_payload(selected: Mapping[str, Any]) -> dict[str, Any]:
    path_text = str(selected.get("source_artifact_path") or selected.get("intent_path") or "").strip()
    if not path_text:
        return {}
    return read_json_object_v1(Path(path_text).expanduser().resolve())


def _direction_from_intent(intent: Mapping[str, Any]) -> str:
    exposure_type = str(intent.get("exposure_type") or "").upper()
    if exposure_type == "LONG_EQUITY":
        return "LONG"
    if exposure_type == "SHORT_EQUITY":
        return "SHORT"
    return ""


def _risk_per_share(entry: str, stop: str) -> str:
    if not entry or not stop:
        return ""
    try:
        return format(abs(Decimal(str(entry)) - Decimal(str(stop))).normalize(), "f")
    except (InvalidOperation, ValueError):
        return ""


def _max_loss(risk_per_share: str, quantity: int | None) -> str:
    if not risk_per_share or not quantity:
        return ""
    try:
        return format((Decimal(str(risk_per_share)) * Decimal(quantity)).normalize(), "f")
    except (InvalidOperation, ValueError):
        return ""



def _notional_value(entry: str, quantity: int | None) -> str:
    if not entry or not quantity:
        return ""
    try:
        return format((Decimal(str(entry)) * Decimal(quantity)).quantize(Decimal("0.01")), "f")
    except (InvalidOperation, ValueError):
        return ""


def _derived_stop_price(*, entry_reference_price: str, direction: str, selected_intent: Mapping[str, Any]) -> str:
    if not entry_reference_price:
        return ""
    constraints = nested_dict_v1(selected_intent, "constraints")
    bps_text = first_text_v1(constraints.get("stop_loss_bps"), selected_intent.get("stop_loss_bps"))
    if not bps_text:
        return ""
    try:
        entry = Decimal(str(entry_reference_price))
        bps = Decimal(str(bps_text))
    except (InvalidOperation, ValueError):
        return ""
    if entry <= 0 or bps <= 0:
        return ""
    multiplier = Decimal("1") + (bps / Decimal("10000")) if str(direction).upper() == "SHORT" else Decimal("1") - (bps / Decimal("10000"))
    if multiplier <= 0:
        return ""
    return format((entry * multiplier).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP).normalize(), "f")


def build_trade_ticket_projection_v1(
    *,
    selected_id: str,
    symbol: str,
    direction: str,
    entry_reference_price: str,
    quantity: int | None,
    suggested_notional: str,
    allocation_percent: str,
    risk_budget_reference: str,
    stop_price: str,
    invalidation_level: str,
    risk_per_share: str,
    max_loss_estimate: str,
    blockers: list[dict[str, Any]],
    conversion: Mapping[str, Any],
    market: Mapping[str, Any],
    selected_intent: Mapping[str, Any],
    current_truth: Mapping[str, Any],
    submit_boundary: Mapping[str, Any],
) -> dict[str, Any]:
    freshness = _freshness_status(market, str(current_truth.get("source_day") or ""))
    blocker_codes = [str(row.get("code") or "") for row in blockers if isinstance(row, Mapping) and str(row.get("code") or "")]
    stale_market_data = _has_blocker(blockers, "STALE_MARKET_DATA_BLOCKS_CONVERSION") or freshness["status"] == "STALE_OR_MISSING"
    constraints = nested_dict_v1(selected_intent, "constraints")
    stop_policy_available = bool(stop_price or invalidation_level or first_text_v1(constraints.get("stop_loss_bps"), selected_intent.get("stop_loss_bps")))
    sizing_policy_available = bool(quantity is not None or allocation_percent or risk_budget_reference)
    risk_policy_available = bool(risk_budget_reference or allocation_percent or constraints.get("max_risk_pct"))

    missing: list[dict[str, Any]] = []
    if not selected_id:
        missing.append(_ticket_missing("selected_exposure_intent_id", "selected exposure lineage", "Portfolio gate did not expose a selected candidate id.", "Run portfolio gate candidate report."))
    if not symbol:
        missing.append(_ticket_missing("symbol", "symbol", "Selected exposure does not include symbol authority.", "Regenerate selected exposure from canonical symbol authority."))
    if stale_market_data:
        missing.append(_ticket_missing("current_market_data", "current market data", _market_data_reason(market), "Refresh canonical market data, then rerun conversion."))
    if not entry_reference_price:
        missing.append(_ticket_missing("entry_reference_price", "entry reference price", "Execution package converter did not produce an entry because current market data is unavailable." if stale_market_data else "Execution package converter did not produce an entry reference price.", "Refresh market data and rerun exposure-intent conversion."))
    if quantity is None:
        missing.append(_ticket_missing("suggested_quantity", "suggested quantity", "Sizing module did not produce share quantity because conversion is blocked." if stale_market_data else "Sizing module did not produce suggested share quantity.", "Run sizing policy through the execution package converter."))
    if not suggested_notional:
        missing.append(_ticket_missing("suggested_notional", "notional", "Notional cannot be calculated without entry price and suggested quantity.", "Generate entry price and suggested quantity."))
    if not stop_price and not invalidation_level:
        missing.append(_ticket_missing("stop_price", "stop/invalidation policy", "Stop policy cannot be applied until current entry price exists." if stale_market_data and stop_policy_available else "No stop price or invalidation level was produced.", "Generate stop/invalidation policy in the converter."))
    if not risk_per_share or not max_loss_estimate:
        missing.append(_ticket_missing("risk_estimate", "risk estimate", "Risk estimate requires entry, stop/invalidation, and suggested quantity.", "Run sizing/risk policy after market data and stop policy are available."))
    if not sizing_policy_available:
        missing.append(_ticket_missing("sizing_policy", "sizing policy", "No quantity, allocation percent, or risk budget reference is present.", "Generate sleeve sizing policy."))
    if not stop_policy_available:
        missing.append(_ticket_missing("stop_policy", "stop policy", "No stop-loss bps, stop price, or invalidation level is present.", "Generate sleeve stop/invalidation policy."))
    if not risk_policy_available:
        missing.append(_ticket_missing("risk_policy", "risk policy", "No risk budget reference or allocation percent is present.", "Generate risk budget policy."))

    if stale_market_data:
        status = "blocked_missing_market_data"
        next_action = "Refresh market data, then rerun exposure-intent conversion and sizing."
    elif not sizing_policy_available:
        status = "blocked_missing_sizing_policy"
        next_action = "Generate sleeve sizing policy, then rerun conversion."
    elif not stop_policy_available:
        status = "blocked_missing_stop_policy"
        next_action = "Generate stop/invalidation policy, then rerun conversion."
    elif not risk_policy_available:
        status = "blocked_missing_risk_policy"
        next_action = "Generate risk budget policy, then rerun conversion."
    elif missing:
        status = "incomplete"
        next_action = "Rerun execution package conversion and sizing until all ticket fields are present."
    else:
        status = "complete"
        next_action = "Ready for manual paper capture."

    capture_status = "Ready for manual paper capture" if status == "complete" else "Not capture-ready"
    return {
        "schema_id": "trade_ticket_projection",
        "schema_version": "v1",
        "trade_ticket_status": status,
        "symbol": symbol,
        "direction": direction,
        "entry_reference_price": entry_reference_price,
        "suggested_quantity": quantity,
        "suggested_notional": suggested_notional,
        "suggested_allocation_percent": allocation_percent,
        "stop_price": stop_price,
        "invalidation_level": invalidation_level,
        "risk_per_share": risk_per_share,
        "max_loss_estimate": max_loss_estimate,
        "capture_status": capture_status,
        "missing_fields": _dedupe_missing(missing),
        "blocker_codes": sorted(set([code for code in blocker_codes if code] + [_ticket_blocker_code(row) for row in missing])),
        "next_required_action": next_action,
        "portfolio_gate_decision": "ALLOW" if str(current_truth.get("current_truth_status") or "") in {"CURRENT_OK", "CURRENT"} else "",
        "portfolio_gate_decision_id": str(current_truth.get("source_run_id") or ""),
        "conversion_status": str(conversion.get("status") or ""),
        "conversion_blocker_code": str(conversion.get("blocker_code") or ""),
        "market_data_status": freshness,
        "submit_boundary_status": str(submit_boundary.get("status") or ""),
        "captured_manually_allowed": status == "complete",
    }


def _ticket_missing(field: str, label: str, why_missing: str, upstream_step: str) -> dict[str, Any]:
    return {
        "field": field,
        "label": label,
        "why_missing": why_missing,
        "upstream_step": upstream_step,
        "blocker_code": _ticket_blocker_code({"field": field}),
    }


def _ticket_blocker_code(row: Mapping[str, Any]) -> str:
    field = str(row.get("field") or "").upper()
    if field == "CURRENT_MARKET_DATA":
        return "MISSING_CURRENT_MARKET_DATA"
    if field:
        return f"MISSING_{field}"
    return ""


def _dedupe_missing(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        field = str(row.get("field") or "")
        if field in seen:
            continue
        seen.add(field)
        out.append(row)
    return out


def _market_data_reason(market: Mapping[str, Any]) -> str:
    expected = str(market.get("expected_session") or "")
    observed = str(market.get("observed_session") or "")
    if expected or observed:
        return "Market data is not current: expected_session=" + (expected or "UNKNOWN") + ", observed_session=" + (observed or "UNKNOWN") + "."
    return "Market data is stale or missing."


def _freshness_status(market: Mapping[str, Any], source_day: str) -> dict[str, Any]:
    expected = str(market.get("expected_session") or source_day or "")
    observed = str(market.get("observed_session") or "")
    status = str(market.get("status") or "UNKNOWN")
    stale = status.upper() in {"STALE_OR_MISSING", "STALE_OR_INVALID", "STALE"} or bool(expected and observed and expected != observed)
    return {
        "status": "STALE_OR_MISSING" if stale else status,
        "expected_session": expected,
        "observed_session": observed,
        "source_path": str(market.get("path") or ""),
        "current": not stale and status.upper() in {"CURRENT", "PASS", "OK", "READY", "FRESH"},
    }


def _required_blockers(
    *,
    selected_id: str,
    symbol: str,
    entry_reference_price: str,
    quantity: int | None,
    allocation_percent: str,
    risk_budget_reference: str,
    stop_price: str,
    invalidation_level: str,
    current_truth: Mapping[str, Any],
    conversion: Mapping[str, Any],
    market: Mapping[str, Any],
    submit_boundary: Mapping[str, Any],
) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    if not selected_id:
        blockers.append(blocker_v1("MISSING_TRADE_CANDIDATE_ID", field="trade_candidate_id"))
        blockers.append(blocker_v1("MISSING_SELECTED_EXPOSURE_LINEAGE", field="selected_exposure_intent_id"))
    if not symbol:
        blockers.append(blocker_v1("MISSING_SYMBOL_AUTHORITY", field="symbol_authority_source"))
    if not entry_reference_price or not is_positive_decimal_text_v1(entry_reference_price):
        blockers.append(blocker_v1("MISSING_ENTRY_REFERENCE_PRICE", field="entry_reference_price"))
    if not stop_price and not invalidation_level:
        blockers.append(blocker_v1("MISSING_STOP_OR_INVALIDATION_LEVEL", field="stop_price"))
    if quantity is None and not allocation_percent and not risk_budget_reference:
        blockers.append(blocker_v1("MISSING_POSITION_SIZE", field="quantity"))
    if not risk_budget_reference and not allocation_percent:
        blockers.append(blocker_v1("MISSING_RISK_BUDGET", field="risk_budget_reference"))
    freshness = _freshness_status(market, str(current_truth.get("source_day") or ""))
    if freshness["status"] == "STALE_OR_MISSING" or str(current_truth.get("stale_market_data") or "").lower() == "true" or str(conversion.get("blocker_code") or "") == "STALE_MARKET_DATA_BLOCKS_CONVERSION":
        blockers.append(blocker_v1("STALE_MARKET_DATA_BLOCKS_CONVERSION", field="data_freshness_status"))
    if str(current_truth.get("current_truth_status") or "") not in {"CURRENT_OK", "CURRENT"}:
        blockers.append(blocker_v1("MISSING_PORTFOLIO_GATE_DECISION", field="portfolio_gate_decision_id"))
    if not str(submit_boundary.get("status") or ""):
        blockers.append(blocker_v1("MISSING_SUBMIT_BOUNDARY_STATUS", field="submit_boundary_status", severity="MEDIUM"))
    conversion_blocker = str(conversion.get("blocker_code") or "").strip()
    if conversion_blocker and conversion_blocker != "STALE_MARKET_DATA_BLOCKS_CONVERSION":
        blockers.append(blocker_v1(conversion_blocker, field="conversion_status", message=str(conversion.get("blocker_message") or ""), severity="MEDIUM"))
    return _dedupe_blockers(blockers)


def _readiness_status(
    *,
    blockers: list[dict[str, Any]],
    conversion: Mapping[str, Any],
    latest_capture: Mapping[str, Any],
    quantity: int | None,
    entry_reference_price: str,
    stop_price: str,
    invalidation_level: str,
    risk_budget_reference: str,
    allocation_percent: str,
) -> str:
    if str(latest_capture.get("capture_status") or "") == "captured_manually":
        return "captured_manually"
    if str(latest_capture.get("capture_status") or "") == "skipped":
        return "skipped"
    if _has_blocker(blockers, "STALE_MARKET_DATA_BLOCKS_CONVERSION") or _has_blocker(blockers, "MISSING_PORTFOLIO_GATE_DECISION"):
        return "review_only_blocked"
    trade_definition_codes = {"MISSING_ENTRY_REFERENCE_PRICE", "MISSING_STOP_OR_INVALIDATION_LEVEL", "MISSING_POSITION_SIZE", "MISSING_RISK_BUDGET", "MISSING_TRADE_CANDIDATE_ID", "MISSING_SYMBOL_AUTHORITY"}
    if any(str(row.get("code") or "") in trade_definition_codes for row in blockers):
        return "incomplete_trade_definition"
    if conversion.get("paper_trade_intent_created") is True:
        return "paper_intent_created"
    if conversion.get("execution_package_created") is True:
        return "paper_package_ready"
    if entry_reference_price and (quantity is not None or allocation_percent or risk_budget_reference) and (stop_price or invalidation_level):
        return "manual_capture_ready"
    return "incomplete_trade_definition"


def _dedupe_blockers(blockers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for blocker in blockers:
        code = str(blocker.get("code") or "")
        if code in seen:
            continue
        seen.add(code)
        out.append(blocker)
    return out


def _has_blocker(blockers: list[dict[str, Any]] | Any, code: str) -> bool:
    return any(isinstance(row, Mapping) and str(row.get("code") or "") == code for row in (blockers if isinstance(blockers, list) else []))


def _current_truth_snapshot_id(current_truth: Mapping[str, Any]) -> str:
    return stable_hash_v1(
        {
            "source_day": current_truth.get("source_day"),
            "selected_exposure_intent_id": current_truth.get("selected_exposure_intent_id"),
            "source_run_id": current_truth.get("source_run_id"),
            "portfolio_gate_report_path": current_truth.get("portfolio_gate_report_path"),
            "conversion_path": current_truth.get("conversion_path"),
        }
    )[:24]


def _submit_boundary_status(*, root: Path, day_utc: str) -> dict[str, Any]:
    path = root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json"
    payload = read_json_object_v1(path)
    return {**payload, "path": str(path), "exists": path.exists(), "sha256": content_hash_file_v1(path)}


def _latest_capture_record(*, root: Path, day_utc: str, selected_id: str) -> dict[str, Any]:
    path = root / "reports" / "manual_capture_record_v1" / day_utc / "manual_capture_record.v1.jsonl"
    if not path.exists() or not path.is_file():
        return {}
    latest: dict[str, Any] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict) and (not selected_id or str(row.get("selected_exposure_intent_id") or "") == selected_id):
            latest = row
    return latest


def _source_artifacts(*, current_truth: Mapping[str, Any], selected: Mapping[str, Any], selected_intent: Mapping[str, Any], submit_boundary: Mapping[str, Any]) -> list[dict[str, Any]]:
    artifacts = current_truth.get("source_artifacts") if isinstance(current_truth.get("source_artifacts"), list) else []
    out = [row for row in artifacts if isinstance(row, dict)]
    selected_path = str(selected.get("source_artifact_path") or "")
    if selected_path:
        path = Path(selected_path).expanduser().resolve()
        out.append({"artifact_id": "selected_exposure_intent", "path": str(path), "exists": path.exists(), "sha256": content_hash_file_v1(path), "schema_id": str(selected_intent.get("schema_id") or "")})
    out.append({"artifact_id": "submit_boundary_status", "path": str(submit_boundary.get("path") or ""), "exists": bool(submit_boundary.get("exists")), "sha256": str(submit_boundary.get("sha256") or ""), "schema_id": str(submit_boundary.get("schema_id") or "")})
    return out


def _operator_action_label(status: str) -> str:
    if status == "review_only_blocked":
        return "Review only - blocked"
    if status == "incomplete_trade_definition":
        return "Incomplete trade definition"
    if status == "captured_manually":
        return "Captured manually"
    if status == "skipped":
        return "Skipped"
    return "Manual capture review available"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="trade_candidate_projection_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", required=True)
    parser.add_argument("--day", "--day-utc", dest="day_utc", required=True)
    args = parser.parse_args(argv)
    payload, path = build_and_write_trade_candidate_projection_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    print(json.dumps({"path": str(path), "projection_id": payload.get("projection_id"), "lifecycle_status": payload.get("lifecycle_status"), "blocker_count": len(payload.get("blockers") or [])}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
