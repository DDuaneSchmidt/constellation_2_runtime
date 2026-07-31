from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from ops.aegis.domain_certification_v1 import build_domain_certification_report_v1, write_domain_certification_report_v1
from ops.aegis.domain_source_builders_v1 import build_domain_source_artifact_v1, setup_requirements_v1, write_domain_source_template_v1

SCHEMA_ID = "aegis_operator_command_registry"
SCHEMA_VERSION = "v1"
AUDIT_FAMILY = "aegis_operator_command_audit_v1"

API_COMMAND = "API_COMMAND"
IN_PAGE_DETAIL = "IN_PAGE_DETAIL"
SCROLL_FOCUS = "SCROLL_FOCUS"
EXPAND_SECTION = "EXPAND_SECTION"
EXTERNAL_LINK = "EXTERNAL_LINK"

COMMAND_IDS = {
    "START_RESEARCH",
    "VIEW_FINDINGS",
    "VIEW_WAITING_REASON",
    "VIEW_BLOCKER",
    "REPAIR_DOMAIN",
    "VIEW_QUEUE",
    "VIEW_RECOMMENDATION",
    "VIEW_PROGRESS",
    "MARK_CAPTURE_COMPLETE",
    "SCROLL_TO_HYPOTHESIS_SECTION",
    "FOCUS_HYPOTHESIS_CARD",
    "OPEN_VALID_ROUTE",
    "OPEN_IN_PAGE_MODAL",
    "VALIDATE_SOURCE",
    "UPLOAD_SOURCE",
    "VIEW_SOURCE_SETUP",
    "DOWNLOAD_SOURCE_TEMPLATE",
    "DOWNLOAD_EOD_SOURCE_TEMPLATE",
    "CERTIFY_SOURCE",
    "RECHECK_DOMAIN",
    "VIEW_REPAIR_JOB",
    "QUEUE_CERTIFICATION",
    "CERTIFY_SELECTED_SYMBOLS",
    "VIEW_CERTIFICATION_RESULT",
    "VIEW_TRADE_DETAIL",
    "ADD_MANUAL_RECEIPT",
    "RECORD_TRADE_EXIT",
    "REVIEW_TRADE_OUTCOME",
    "RECONCILE_TRADE_STATE",
    "GENERATE_BACKFILLED_EXIT_PLAN",
    "REVIEW_EXIT_INTENT",
    "VIEW_EXIT_HISTORY",
    "VIEW_EXIT_DECISION",
    "UPDATE_STOP_PLAN",
    "UPDATE_TARGET_PLAN",
    "RECORD_PARTIAL_EXIT",
    "RECORD_FULL_EXIT",
    "RECORD_TRADE_OUTCOME",
    "VIEW_POSITION_DETAIL",
    "RUN_CONTEXT_READINESS_COMMAND",
    "REFRESH_CANDIDATE_CONTRACTS",
    "REFRESH_CANDIDATE_PROJECTION",
    "REVIEW_PAPER_CANDIDATE",
    "APPROVE_PAPER_CANDIDATE",
    "REJECT_PAPER_CANDIDATE",
    "CONFIRM_CANDIDATE_CAPTURED",
    "MARK_CANDIDATE_NOT_CAPTURED",
    "DEFER_CANDIDATE",
    "CORRECT_CANDIDATE_CAPTURE",
    "RECORD_PAPER_ENTRY",
    "PAPER_TRADE_CANDIDATE",
    "RECORD_PAPER_EXIT",
}

REQUIRED_COMMAND_FIELDS = (
    "command_id",
    "label",
    "applies_to",
    "visible_when",
    "enabled_when",
    "action_type",
    "endpoint",
    "route",
    "input_schema",
    "success_state_transition",
    "failure_behavior",
    "audit_artifact_type",
    "safety_classification",
    "browser_acceptance_test_required",
)

SAFETY = {
    "review_only": True,
    "broker_submit_transmit_allowed": False,
    "broker_execution_allowed": False,
    "order_routing_allowed": False,
    "live_trading_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
    "automatic_approval_allowed": False,
    "automatic_promotion_allowed": False,
}


def utc_now_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def _read_json_v1(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _coalesce_v1(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _trade_event_time_v1(trade: Mapping[str, Any]) -> str:
    for event in trade.get("lifecycle_events") or []:
        if not isinstance(event, Mapping):
            continue
        value = _coalesce_v1(event.get("fill_time"), event.get("event_time"), event.get("captured_at_utc"), event.get("created_at_utc"))
        if value:
            return value
    return _coalesce_v1(trade.get("fill_time"), trade.get("event_time"), trade.get("created_at_utc"))


def _find_trade_for_receipt_v1(*, truth_root: Path | str, day_utc: str, target_id: str) -> dict[str, Any]:
    from ops.aegis.trade_lifecycle.trade_evaluation_projection_v1 import build_paper_trade_evaluation_projection_v1

    projection = build_paper_trade_evaluation_projection_v1(truth_root=truth_root, day_utc=day_utc)
    needle = str(target_id or "").strip()
    for trade in projection.get("all_trades") or []:
        if not isinstance(trade, dict):
            continue
        keys = {
            str(trade.get("trade_id") or ""),
            str(trade.get("capture_ticket_id") or ""),
            str(trade.get("candidate_id") or ""),
        }
        if needle and needle in keys:
            return trade
    return {}


def _write_manual_receipt_from_trade_v1(*, truth_root: Path | str, day_utc: str, target_id: str, payload: Mapping[str, Any], actor: str) -> dict[str, Any]:
    from ops.tools.capture_manual_trade_receipt_v1 import validate_manual_trade_receipt_v1, write_manual_execution_receipt_aggregate_v1
    from ops.aegis.trade_lifecycle.trade_evaluation_projection_v1 import (
        build_paper_trade_evaluation_projection_v1,
        build_trade_lifecycle_ledger_v1,
        write_paper_trade_evaluation_projection_v1,
        write_trade_lifecycle_ledger_v1,
    )
    from ops.aegis.trade_lifecycle.exit_review_projection_v1 import build_exit_review_projection_v1, write_exit_review_projection_v1

    root = Path(truth_root).expanduser().resolve()
    body = dict(payload or {})
    trade = _find_trade_for_receipt_v1(truth_root=root, day_utc=day_utc, target_id=target_id)
    trade_id = _coalesce_v1(body.get("trade_id"), target_id, trade.get("trade_id"), trade.get("capture_ticket_id"))
    symbol = _coalesce_v1(body.get("symbol"), trade.get("symbol")).upper()
    side = _coalesce_v1(body.get("side"), trade.get("side"))
    quantity = _coalesce_v1(body.get("quantity"), trade.get("quantity"))
    price = _coalesce_v1(body.get("price"), body.get("fill_price"), trade.get("entry_price"))
    execution_time = _coalesce_v1(body.get("execution_time"), body.get("fill_time"), _trade_event_time_v1(trade), day_utc)
    trade_date = _coalesce_v1(body.get("trade_date"), execution_time[:10], day_utc)
    raw_receipt = {
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "price": price,
        "trade_date": trade_date,
        "execution_time": execution_time,
        "entered_by": _coalesce_v1(body.get("entered_by"), actor, "operator"),
        "broker": _coalesce_v1(body.get("broker"), body.get("source"), "IB_MANUAL_OR_PAPER"),
        "account_alias": _coalesce_v1(body.get("account_alias"), body.get("account"), "paper/manual"),
        "currency": _coalesce_v1(body.get("currency"), "USD"),
        "order_type": _coalesce_v1(body.get("order_type"), "UNKNOWN"),
        "fees": _coalesce_v1(body.get("fees"), "0"),
        "notes": _coalesce_v1(body.get("notes"), "Reconciled from Aegis captured ticket history."),
        "strategy_or_sleeve": _coalesce_v1(body.get("strategy_or_sleeve"), trade.get("sleeve_id")),
        "related_candidate_id": _coalesce_v1(body.get("related_candidate_id"), trade.get("candidate_id")),
        "external_order_id_redacted": _coalesce_v1(body.get("external_order_id_redacted")),
        "operator_attestation": body.get("operator_attestation", True),
        "broker_submission_by_aegis": False,
        "autonomous_execution": False,
    }
    receipt, errors = validate_manual_trade_receipt_v1(raw_receipt)
    if errors:
        return {"ok": False, "errors": errors, "trade": trade}
    receipt.update(
        {
            "trade_id": trade_id,
            "position_id": _coalesce_v1(body.get("position_id"), str(trade_id).replace(":", "_")),
            "capture_ticket_id": _coalesce_v1(body.get("capture_ticket_id"), trade.get("capture_ticket_id"), trade_id),
            "manual_capture_record_id": _coalesce_v1(body.get("manual_capture_record_id"), trade.get("capture_record_id")),
            "linked_recommendation": _coalesce_v1(body.get("linked_recommendation"), trade.get("recommendation_id")),
            "linked_promoted_candidate": _coalesce_v1(body.get("linked_promoted_candidate"), trade.get("candidate_id")),
            "verification_status": "VERIFIED",
            "receipt_type": "MANUAL_FILL_RECORDED",
            "timestamp": execution_time,
            "manual_operator_confirmation_required": True,
            "broker_submit_transmit_allowed": False,
            "broker_execution_allowed": False,
            "order_routing_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
        }
    )
    receipt["evidence_hash"] = stable_hash_v1({**receipt, "evidence_hash": ""})
    receipt_id = str(receipt["receipt_id"])
    receipt_path = root / "manual_trade_receipts" / trade_date / f"{receipt_id}.manual_trade_receipt.v1.json"
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    aggregate_path = write_manual_execution_receipt_aggregate_v1(truth_root=root, day_utc=trade_date)
    ledger = build_trade_lifecycle_ledger_v1(truth_root=root, day_utc=day_utc)
    ledger_paths = write_trade_lifecycle_ledger_v1(truth_root=root, day_utc=day_utc, payload=ledger)
    trade_projection = build_paper_trade_evaluation_projection_v1(truth_root=root, day_utc=day_utc)
    trade_paths = write_paper_trade_evaluation_projection_v1(truth_root=root, day_utc=day_utc, payload=trade_projection)
    exit_projection = build_exit_review_projection_v1(truth_root=root, day_utc=day_utc)
    exit_paths = write_exit_review_projection_v1(truth_root=root, day_utc=day_utc, payload=exit_projection)
    refreshed_trade = next((row for row in trade_projection.get("all_trades") or [] if isinstance(row, dict) and str(row.get("trade_id") or "") == trade_id), {})
    refreshed_exit = next((row for row in exit_projection.get("rows") or [] if isinstance(row, dict) and str(row.get("trade_id") or "") == trade_id), {})
    return {
        "ok": True,
        "receipt": receipt,
        "receipt_id": receipt_id,
        "receipt_path": str(receipt_path),
        "manual_execution_receipt_path": str(aggregate_path),
        "ledger_paths": ledger_paths,
        "trade_projection_paths": trade_paths,
        "exit_review_paths": exit_paths,
        "refreshed_trade": refreshed_trade,
        "refreshed_exit_review": refreshed_exit,
    }


def command_audit_dir_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / AUDIT_FAMILY / day_utc


def command_audit_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return command_audit_dir_v1(truth_root=truth_root, day_utc=day_utc) / "operator_command_audit.v1.jsonl"


def _command(
    command_id: str,
    label: str,
    applies_to: str,
    visible_when: str,
    enabled_when: str,
    action_type: str,
    *,
    endpoint: str = "",
    route: str = "",
    input_schema: dict[str, Any] | None = None,
    success_state_transition: str = "UI state remains unchanged until the authoritative projection refreshes.",
    failure_behavior: str = "Show inline error and keep the operator on the current Aegis page.",
    audit_artifact_type: str = "operator_command_audit.v1",
    safety_classification: dict[str, Any] | None = None,
    browser_acceptance_test_required: bool = True,
) -> dict[str, Any]:
    if command_id not in COMMAND_IDS:
        raise ValueError(f"unregistered_command_id:{command_id}")
    return {
        "command_id": command_id,
        "label": label,
        "applies_to": applies_to,
        "visible_when": visible_when,
        "enabled_when": enabled_when,
        "action_type": action_type,
        "endpoint": endpoint,
        "route": route,
        "input_schema": input_schema or {"type": "object", "additionalProperties": True},
        "success_state_transition": success_state_transition,
        "failure_behavior": failure_behavior,
        "audit_artifact_type": audit_artifact_type,
        "safety_classification": safety_classification or dict(SAFETY),
        "browser_acceptance_test_required": browser_acceptance_test_required,
    }


def command_registry_v1() -> dict[str, Any]:
    commands = [
        _command(
            "START_RESEARCH",
            "Start Research",
            "hypothesis",
            "Hypothesis is Ready to Start or Monitoring and not blocked.",
            "hypothesis_id is present and no blocker is active.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "required": ["hypothesis_id"], "properties": {"hypothesis_id": {"type": "string"}, "title": {"type": "string"}, "symbols": {"type": "string"}}},
            success_state_transition="Create USER_INITIATED research-run ledger event and move projection to Queued/Researching after refresh.",
        ),
        _command("VIEW_QUEUE", "View Queue", "hypothesis", "Hypothesis has queued or scheduled research.", "Detail panel is present.", IN_PAGE_DETAIL, success_state_transition="Open in-page queue/progress details."),
        _command("VIEW_PROGRESS", "View Progress", "hypothesis", "Hypothesis has active research.", "Detail panel is present.", IN_PAGE_DETAIL, success_state_transition="Open in-page progress details."),
        _command("VIEW_WAITING_REASON", "View Waiting Reason", "hypothesis", "Hypothesis is waiting.", "Detail panel is present.", IN_PAGE_DETAIL, success_state_transition="Open in-page waiting reason with dependency, next retry, and expected unblock when known."),
        _command("VIEW_BLOCKER", "View Blocker", "hypothesis", "Hypothesis is blocked.", "Detail panel is present.", IN_PAGE_DETAIL, success_state_transition="Open in-page blocker details."),
        _command("VIEW_FINDINGS", "View Findings", "hypothesis", "Hypothesis is complete or findings may exist.", "Detail panel is present.", IN_PAGE_DETAIL, success_state_transition="Open findings panel; show no-findings explanation if findings are not available."),
        _command("VIEW_RECOMMENDATION", "View Recommendation", "hypothesis", "Manual recommendation is ready.", "Detail panel is present.", IN_PAGE_DETAIL, success_state_transition="Open recommendation panel with manual IB capture guidance."),
        _command(
            "REPAIR_DOMAIN",
            "Repair",
            "domain_certification",
            "Domain certification status is DELAYED.",
            "domain_id is present and a delayed-domain repair plan exists.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "required": ["domain_id"], "properties": {"domain_id": {"type": "string"}}},
            success_state_transition="Source-missing domains return SOURCE_SETUP_REQUIRED; automatic EOD repair queues an auditable remediation job.",
        ),
        _command(
            "VALIDATE_SOURCE",
            "Validate Source",
            "domain_source",
            "A source-missing repair item has a required source path.",
            "Required source path exists or can be checked.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "required": ["domain_id"], "properties": {"domain_id": {"type": "string"}}},
            success_state_transition="Validate the configured source path and report whether certification can be rerun.",
        ),
        _command(
            "UPLOAD_SOURCE",
            "Upload/configure source",
            "domain_source",
            "A source-missing repair item requires operator/admin input.",
            "Required source path is known.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "required": ["domain_id"], "properties": {"domain_id": {"type": "string"}}},
            success_state_transition="Return exact source setup requirements; Aegis does not fabricate source files.",
        ),
        _command(
            "VIEW_SOURCE_SETUP",
            "View setup requirements",
            "domain_source",
            "A source-missing repair item needs operator/admin setup.",
            "In-page setup panel is present.",
            IN_PAGE_DETAIL,
            success_state_transition="Open guided setup requirements without leaving Repair Center.",
        ),
        _command(
            "DOWNLOAD_SOURCE_TEMPLATE",
            "Download template",
            "domain_source",
            "External JSON source setup requires a template.",
            "Operational day and domain_id are present.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "required": ["domain_id"], "properties": {"domain_id": {"type": "string"}, "day_utc": {"type": "string"}}},
            success_state_transition="Write a JSON template for the required governed source; no source data is fabricated.",
        ),
        _command(
            "DOWNLOAD_EOD_SOURCE_TEMPLATE",
            "Download CSV template",
            "domain_source",
            "US_EQUITIES_EOD requires uploaded EOD source coverage.",
            "Operational day is present.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "properties": {"domain_id": {"type": "string"}, "day_utc": {"type": "string"}}},
            success_state_transition="Write the required US equities EOD universe and return a CSV template path.",
        ),
        _command(
            "CERTIFY_SOURCE",
            "Certify from source file",
            "domain_source",
            "US_EQUITIES_EOD has a configured uploaded EOD source file.",
            "AEGIS_US_EQUITIES_EOD_SOURCE_FILE points to a source file or payload.source_file is provided.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "properties": {"domain_id": {"type": "string"}, "source_file": {"type": "string"}, "day_utc": {"type": "string"}}},
            success_state_transition="Validate uploaded source, write canonical final EOD artifact, and rerun domain certification.",
        ),
        _command(
            "RECHECK_DOMAIN",
            "Recheck Domain",
            "domain_certification",
            "A repair item needs domain recertification.",
            "Domain id is present.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "required": ["domain_id"], "properties": {"domain_id": {"type": "string"}}},
            success_state_transition="Rebuild and persist the domain certification report for the operational day.",
        ),
        _command(
            "QUEUE_CERTIFICATION",
            "Queue Certification",
            "dynamic_certification_queue",
            "Candidate Funnel has uncovered symbols with candidate/intent pressure.",
            "Operational day is present; queue selection is read-only and audit-backed.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "properties": {"day_utc": {"type": "string"}}},
            success_state_transition="Build dynamic_certification_queue_v1 from candidate pressure without changing broker, routing, thresholds, or scheduler policy.",
            audit_artifact_type="dynamic_certification_queue_v1",
        ),
        _command(
            "CERTIFY_SELECTED_SYMBOLS",
            "Certify Selected Symbols",
            "dynamic_certification_queue",
            "A dynamic certification queue has requested symbols.",
            "Selected symbols are present in dynamic_certification_queue_v1.requested_symbols.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "properties": {"day_utc": {"type": "string"}, "symbols": {"type": "array", "items": {"type": "string"}}}},
            success_state_transition="Fetch and validate only queued symbols, append successful rows to immutable final EOD artifact, and leave failed symbols excluded.",
            audit_artifact_type="dynamic_certification_queue_v1",
        ),
        _command(
            "VIEW_CERTIFICATION_RESULT",
            "View Certification Result",
            "dynamic_certification_queue",
            "A dynamic certification queue or result exists.",
            "Operational day is present.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "properties": {"day_utc": {"type": "string"}}},
            success_state_transition="Return latest dynamic certification result panel without mutating certification state.",
            audit_artifact_type="dynamic_certification_queue_v1",
        ),
        _command("VIEW_REPAIR_JOB", "View Job", "repair_item", "A repair command or remediation job exists.", "Repair item is present in the current projection.", IN_PAGE_DETAIL, success_state_transition="Open in-page repair job details."),
        _command("VIEW_TRADE_DETAIL", "View Trade Detail", "paper_trade", "A trade lifecycle projection row exists.", "Detail panel is present.", IN_PAGE_DETAIL, success_state_transition="Open in-page trade lifecycle, evidence, and attribution details."),
        _command(
            "ADD_MANUAL_RECEIPT",
            "Add Manual Receipt",
            "paper_trade",
            "A captured trade is missing manual IB receipt evidence.",
            "Receipt fields are present and operator attestation is true; recording remains manual-only.",
            IN_PAGE_DETAIL,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "required": ["trade_id", "symbol", "side", "quantity", "price", "execution_time"], "properties": {"trade_id": {"type": "string"}, "symbol": {"type": "string"}, "side": {"type": "string"}, "quantity": {"type": "string"}, "price": {"type": "string"}, "execution_time": {"type": "string"}, "operator_attestation": {"type": "boolean"}}},
            success_state_transition="Open the in-page receipt form; form submission writes manual_execution_receipt_v1 and refreshes trade/exit projections. No broker route or autonomous execution is enabled.",
            audit_artifact_type="manual_execution_receipt_v1",
        ),
        _command("RECORD_TRADE_EXIT", "Record Trade Exit", "paper_trade", "An open captured trade has no exit evidence.", "Detail panel is present; exit recording remains manual evidence capture.", IN_PAGE_DETAIL, success_state_transition="Open exit evidence guidance panel; no broker route or autonomous execution is enabled."),
        _command("REVIEW_TRADE_OUTCOME", "Review Trade Outcome", "paper_trade", "A closed trade has outcome evidence.", "Detail panel is present.", IN_PAGE_DETAIL, success_state_transition="Open in-page realized outcome details."),
        _command("RECONCILE_TRADE_STATE", "Reconcile Trade State", "paper_trade", "A trade has incomplete or conflicting evaluation evidence.", "Detail panel is present.", IN_PAGE_DETAIL, success_state_transition="Open reconciliation guidance panel with missing evidence and source artifacts."),
        _command("GENERATE_BACKFILLED_EXIT_PLAN", "Generate Backfilled Exit Plan", "exit_review_position", "A legacy open trade is missing trade_intent_ledger_v1 evidence.", "Trade id or position id is present; output remains partial and manual-review only.", API_COMMAND, endpoint="/api/aegis/commands/execute", input_schema={"type": "object", "properties": {"trade_id": {"type": "string"}, "position_id": {"type": "string"}}}, success_state_transition="Append BACKFILLED_INTENT to trade_intent_ledger_v1 and refresh exit projections. No thesis details are invented."),
        _command("REVIEW_EXIT_INTENT", "Review Exit Intent", "exit_review_position", "An adaptive exit review row needs manual review.", "Detail panel is present.", IN_PAGE_DETAIL, success_state_transition="Open current/original exit intent and evidence in-page."),
        _command("VIEW_EXIT_HISTORY", "View Exit History", "exit_review_position", "Trade intent ledger history exists or can be inspected.", "Detail panel is present.", IN_PAGE_DETAIL, success_state_transition="Open append-only exit intent history in-page."),
        _command("VIEW_EXIT_DECISION", "View Exit Decision", "exit_review_position", "An exit review row has an exit decision.", "Detail panel is present.", IN_PAGE_DETAIL, success_state_transition="Open the exit decision details in-page."),
        _command("UPDATE_STOP_PLAN", "Update Stop Plan", "exit_review_position", "Exit review recommends UPDATE_STOP or reports a missing stop plan.", "Detail panel is present; updating remains manual/operator-confirmed.", IN_PAGE_DETAIL, success_state_transition="Open stop-plan guidance. No broker order, route, or transmit occurs."),
        _command("UPDATE_TARGET_PLAN", "Update Target Plan", "exit_review_position", "Operator wants to record a manual target-plan change.", "Detail panel is present; updating remains manual/operator-confirmed.", IN_PAGE_DETAIL, success_state_transition="Open target-plan guidance. No broker order, route, or transmit occurs."),
        _command("RECORD_PARTIAL_EXIT", "Record Partial Exit", "exit_review_position", "Exit review recommends TAKE_PARTIAL.", "Detail panel is present; recording remains manual/operator-confirmed.", IN_PAGE_DETAIL, success_state_transition="Open partial-exit evidence guidance. No broker order, route, or transmit occurs."),
        _command("RECORD_FULL_EXIT", "Record Full Exit", "exit_review_position", "Exit review recommends EXIT_FULL.", "Detail panel is present; recording remains manual/operator-confirmed.", IN_PAGE_DETAIL, success_state_transition="Open full-exit evidence guidance. No broker order, route, or transmit occurs."),
        _command("RECORD_TRADE_OUTCOME", "Record Trade Outcome", "exit_review_position", "A closed outcome needs operator review or final outcome evidence.", "Detail panel is present; recording remains manual/operator-confirmed.", IN_PAGE_DETAIL, success_state_transition="Open trade-outcome evidence guidance."),
        _command("VIEW_POSITION_DETAIL", "View Position Detail", "exit_review_position", "An open position exists in Exit Review.", "Detail panel is present.", IN_PAGE_DETAIL, success_state_transition="Open position detail, evidence, and safety context in-page."),
        _command(
            "RUN_CONTEXT_READINESS_COMMAND",
            "Repair Context Readiness",
            "runtime_context",
            "Context readiness evidence is missing, stale, or blocked.",
            "Operational day is present and the hardened repair_context_readiness action is allowlisted.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={
                "type": "object",
                "properties": {
                    "action_id": {"type": "string", "const": "repair_context_readiness"},
                    "command": {"type": "string"},
                    "day_utc": {"type": "string"},
                },
                "additionalProperties": True,
            },
            success_state_transition="Run the allowlisted context-readiness repair argv; refresh runtime truth, verified graph, hydrate packet, and audit handoff. No trading, advice, capture, promotion, or broker policy is changed.",
            failure_behavior="Show the hardened action result and keep Aegis policy gates fail-closed.",
            audit_artifact_type="aegis_portal_action_record.v1",
            safety_classification={**SAFETY, "portal_action_id": "repair_context_readiness"},
        ),
        _command(
            "REVIEW_PAPER_CANDIDATE",
            "Review",
            "paper_review_candidate",
            "A paper-review candidate needs human review.",
            "Candidate id is present; opens governed evidence in-page only.",
            IN_PAGE_DETAIL,
            success_state_transition="Open candidate review modal with lineage, evidence, and paper-only warnings.",
            safety_classification={**SAFETY, "portal_action_id": "review_paper_candidate"},
        ),
        _command(
            "PAPER_TRADE_CANDIDATE",
            "Paper Trade",
            "paper_review_candidate",
            "Candidate workflow state is AWAITING_REVIEW.",
            "Candidate id, entry price, and quantity or notional are provided; receipt type remains SIMULATED_PAPER.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "required": ["candidate_id", "paper_entry_price"], "properties": {"candidate_id": {"type": "string"}, "paper_entry_price": {"type": "string"}, "quantity": {"type": "string"}, "notional": {"type": "string"}, "timestamp": {"type": "string"}, "notes": {"type": "string"}}},
            success_state_transition="Approve the candidate, create a SIMULATED_PAPER receipt, update paper review queue/outcomes/candidate projection, and set workflow state PAPER_POSITION_OPEN. No broker order is created.",
            audit_artifact_type="paper_trade_receipts.v1.json",
            safety_classification={**SAFETY, "portal_action_id": "paper_trade_candidate"},
        ),
        _command(
            "APPROVE_PAPER_CANDIDATE",
            "Approve for Paper",
            "paper_review_candidate",
            "Candidate workflow state is AWAITING_REVIEW.",
            "Candidate id is in paper_review_queue and live_trade_eligible is false.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "required": ["candidate_id"], "properties": {"candidate_id": {"type": "string"}, "reason": {"type": "string"}, "day_utc": {"type": "string"}}},
            success_state_transition="Append operator decision artifact and set workflow state APPROVED_FOR_PAPER. No broker order is created.",
            audit_artifact_type="paper_review_decisions.v1.jsonl",
            safety_classification={**SAFETY, "portal_action_id": "approve_paper_candidate"},
        ),
        _command(
            "REJECT_PAPER_CANDIDATE",
            "Reject",
            "paper_review_candidate",
            "Candidate workflow state is AWAITING_REVIEW.",
            "Candidate id is in paper_review_queue and live_trade_eligible is false.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "required": ["candidate_id"], "properties": {"candidate_id": {"type": "string"}, "reason": {"type": "string"}, "day_utc": {"type": "string"}}},
            success_state_transition="Append operator decision artifact and set workflow state REJECTED_BY_OPERATOR. No broker order is created.",
            audit_artifact_type="paper_review_decisions.v1.jsonl",
            safety_classification={**SAFETY, "portal_action_id": "reject_paper_candidate"},
        ),
        _command(
            "CONFIRM_CANDIDATE_CAPTURED",
            "Confirm Captured",
            "paper_review_candidate",
            "Candidate is visible in Today's Candidates and the operator manually captured it outside Aegis.",
            "Candidate id and paper session id are present; broker submit/transmit remains disabled.",
            API_COMMAND,
            endpoint="/api/aegis/commands",
            input_schema={"type": "object", "required": ["candidate_id", "paper_session_id"], "properties": {"candidate_id": {"type": "string"}, "paper_session_id": {"type": "string"}, "actual_entry": {"type": "string"}, "actual_stop": {"type": "string"}, "quantity": {"type": "string"}, "notes": {"type": "string"}}},
            success_state_transition="Record a durable candidate capture command and paper entry receipt. No broker order is created.",
            audit_artifact_type="aegis_paper_entry_receipts_v1/paper_entry_receipts.v1.json",
            safety_classification={**SAFETY, "portal_action_id": "confirm_candidate_captured"},
        ),
        _command(
            "MARK_CANDIDATE_NOT_CAPTURED",
            "Mark Not Captured",
            "paper_review_candidate",
            "Candidate is visible in Today's Candidates and the operator did not capture it.",
            "Candidate id and paper session id are present; no receipt or broker order is created.",
            API_COMMAND,
            endpoint="/api/aegis/commands",
            input_schema={"type": "object", "required": ["candidate_id", "paper_session_id"], "properties": {"candidate_id": {"type": "string"}, "paper_session_id": {"type": "string"}, "notes": {"type": "string"}}},
            success_state_transition="Append CANDIDATE_NOT_CAPTURED decision event. No broker order is created.",
            audit_artifact_type="aegis_candidate_decision_ledger_v1/candidate_decision_ledger.v1.json",
            safety_classification={**SAFETY, "portal_action_id": "mark_candidate_not_captured"},
        ),
        _command(
            "DEFER_CANDIDATE",
            "Defer",
            "paper_review_candidate",
            "Candidate is visible in Today's Candidates and the operator is postponing the capture decision.",
            "Candidate id and paper session id are present.",
            API_COMMAND,
            endpoint="/api/aegis/commands",
            input_schema={"type": "object", "required": ["candidate_id", "paper_session_id"], "properties": {"candidate_id": {"type": "string"}, "paper_session_id": {"type": "string"}, "notes": {"type": "string"}}},
            success_state_transition="Append CANDIDATE_DEFERRED decision event. No broker order is created.",
            audit_artifact_type="aegis_candidate_decision_ledger_v1/candidate_decision_ledger.v1.json",
            safety_classification={**SAFETY, "portal_action_id": "defer_candidate"},
        ),
        _command(
            "CORRECT_CANDIDATE_CAPTURE",
            "Correct Capture",
            "paper_review_candidate",
            "Candidate has capture information the operator needs to inspect or correct.",
            "Candidate id and paper session id are present.",
            API_COMMAND,
            endpoint="/api/aegis/commands",
            input_schema={"type": "object", "required": ["candidate_id", "paper_session_id"], "properties": {"candidate_id": {"type": "string"}, "paper_session_id": {"type": "string"}, "notes": {"type": "string"}}},
            success_state_transition="Record a durable correction command for manual paper capture. No broker order is created.",
            audit_artifact_type="aegis_paper_entry_receipts_v1/paper_entry_receipts.v1.json",
            safety_classification={**SAFETY, "portal_action_id": "correct_candidate_capture"},
        ),
        _command(
            "RECORD_PAPER_ENTRY",
            "Record Entry",
            "paper_review_candidate",
            "Candidate workflow state is APPROVED_FOR_PAPER.",
            "Entry price plus quantity or notional is provided; receipt type remains SIMULATED_PAPER.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "required": ["candidate_id", "paper_entry_price"], "properties": {"candidate_id": {"type": "string"}, "paper_entry_price": {"type": "string"}, "quantity": {"type": "string"}, "notional": {"type": "string"}, "timestamp": {"type": "string"}, "notes": {"type": "string"}}},
            success_state_transition="Create SIMULATED_PAPER receipt and set workflow state PAPER_POSITION_OPEN. No broker order is created.",
            audit_artifact_type="paper_trade_receipts.v1.json",
            safety_classification={**SAFETY, "portal_action_id": "record_paper_entry"},
        ),
        _command(
            "RECORD_PAPER_EXIT",
            "Record Exit",
            "paper_review_candidate",
            "Candidate workflow state is PAPER_POSITION_OPEN.",
            "Exit price is provided for an open SIMULATED_PAPER position.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "required": ["candidate_id", "paper_exit_price"], "properties": {"candidate_id": {"type": "string"}, "paper_exit_price": {"type": "string"}, "timestamp": {"type": "string"}, "notes": {"type": "string"}, "exit_reason_selected_by_operator": {"type": "string"}}},
            success_state_transition="Append SIMULATED_PAPER exit receipt and close paper_trade_outcomes. No broker order is created.",
            audit_artifact_type="paper_trade_outcomes.v1.json",
            safety_classification={**SAFETY, "portal_action_id": "record_paper_exit"},
        ),
        _command(
            "REFRESH_CANDIDATE_CONTRACTS",
            "Refresh Candidate Contracts",
            "candidate_contracts",
            "Entry reference price evidence is missing, stale, uncertified, or mismatched.",
            "Operational day is present; the command runs only allowlisted market-data, data-registry, and candidate-contract argv arrays.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "properties": {"day_utc": {"type": "string"}}},
            success_state_transition="Regenerate market data, data registry, and candidate contracts only. No trading, advice, capture, promotion, or broker policy is changed.",
            failure_behavior="Show the failed argv step and keep Aegis policy gates fail-closed.",
            audit_artifact_type="aegis_portal_action_record.v1",
            safety_classification={**SAFETY, "portal_action_id": "refresh_candidate_contracts"},
        ),
        _command(
            "REFRESH_CANDIDATE_PROJECTION",
            "Refresh Candidate Projection",
            "candidate_projection",
            "Candidate diagnostics, contracts, or paper review projection is missing, stale, or mismatched.",
            "Operational day is present; the command runs only allowlisted Aegis projection refresh argv arrays.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "properties": {"day_utc": {"type": "string"}}},
            success_state_transition="Regenerate candidate diagnostics, contracts, paper review queue, canonical operator state, and verified graph/portal model. No trading, advice, capture, promotion, or broker policy is changed.",
            failure_behavior="Show the failed argv step and keep Aegis policy gates fail-closed.",
            audit_artifact_type="aegis_portal_action_record.v1",
            safety_classification={**SAFETY, "portal_action_id": "refresh_candidate_projection"},
        ),
        _command(
            "MARK_CAPTURE_COMPLETE",
            "Mark IB capture complete",
            "manual_capture_ticket",
            "A valid manual-capture ticket is ready for operator recording.",
            "Ticket lineage and submit boundary hashes are present.",
            API_COMMAND,
            endpoint="/api/aegis/commands/execute",
            input_schema={"type": "object", "required": ["ticket_id"], "properties": {"ticket_id": {"type": "string"}}},
            success_state_transition="Append manual capture record; Aegis still does not route or transmit orders.",
        ),
        _command("SCROLL_TO_HYPOTHESIS_SECTION", "Show Section", "hypotheses_summary", "Summary section has visible count.", "Target section exists in DOM.", SCROLL_FOCUS, success_state_transition="Expand and scroll/focus the target section."),
        _command("FOCUS_HYPOTHESIS_CARD", "Show Hypothesis", "hypotheses_summary", "Summary top item exists.", "Target card exists in DOM or section can be expanded.", SCROLL_FOCUS, success_state_transition="Expand section and focus/highlight the target card."),
        _command("OPEN_VALID_ROUTE", "Open", "navigation", "A declared route exists.", "Route is registered in navigation schema.", EXPAND_SECTION, success_state_transition="Use in-app route navigation only."),
        _command("OPEN_IN_PAGE_MODAL", "Open Details", "in_page_modal", "A modal/detail surface exists in the current page.", "Target modal exists in DOM.", IN_PAGE_DETAIL, success_state_transition="Open the target modal/detail surface in-page."),
    ]
    return {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "commands": commands,
        "commands_by_id": {row["command_id"]: row for row in commands},
        **SAFETY,
    }


def command_contract_v1(command_id: str) -> dict[str, Any]:
    registry = command_registry_v1()["commands_by_id"]
    command = registry.get(str(command_id or ""))
    if not command:
        raise KeyError(f"UNKNOWN_COMMAND_ID:{command_id}")
    return command


def validate_command_registry_v1() -> dict[str, Any]:
    registry = command_registry_v1()
    errors: list[str] = []
    for command in registry["commands"]:
        for field in REQUIRED_COMMAND_FIELDS:
            if field not in command:
                errors.append(f"{command.get('command_id')}:missing:{field}")
        if command.get("action_type") == API_COMMAND and command.get("endpoint") not in {"/api/aegis/commands/execute", "/api/aegis/commands"}:
            errors.append(f"{command.get('command_id')}:api_command_missing_router_endpoint")
    return {"ok": not errors, "errors": errors, "command_count": len(registry["commands"]), "registry": registry}


def command_for_hypothesis_status_v1(status: str) -> dict[str, Any]:
    command_id = {
        "Ready to Start": "START_RESEARCH",
        "Monitoring": "START_RESEARCH",
        "Queued": "VIEW_QUEUE",
        "Scheduled": "VIEW_QUEUE",
        "Researching": "VIEW_PROGRESS",
        "Collecting Evidence": "VIEW_PROGRESS",
        "Waiting": "VIEW_WAITING_REASON",
        "Complete": "VIEW_FINDINGS",
        "Blocked": "VIEW_BLOCKER",
        "Recommendation Ready": "VIEW_RECOMMENDATION",
    }.get(str(status or ""), "START_RESEARCH")
    return command_contract_v1(command_id)


def command_instance_v1(command_id: str, *, target_type: str, target_id: str, label: str | None = None, payload: dict[str, Any] | None = None, enabled: bool = True, disabled_reason: str = "") -> dict[str, Any]:
    contract = command_contract_v1(command_id)
    return {
        **contract,
        "target_type": target_type,
        "target_id": target_id,
        "payload": payload or {},
        "label": label or contract["label"],
        "enabled": bool(enabled),
        "disabled_reason": disabled_reason,
    }


def _audit_record(command: dict[str, Any], *, truth_root: Path | str, day_utc: str, request_payload: dict[str, Any], result: dict[str, Any], state_before: dict[str, Any] | None = None, state_after: dict[str, Any] | None = None) -> dict[str, Any]:
    requested_at = utc_now_v1()
    record = {
        "schema_id": "aegis_operator_command_audit",
        "schema_version": "v1",
        "command_id": command["command_id"],
        "target_type": str(request_payload.get("target_type") or ""),
        "target_id": str(request_payload.get("target_id") or request_payload.get("hypothesis_id") or request_payload.get("domain_id") or ""),
        "requested_at": requested_at,
        "requested_by": str(request_payload.get("requested_by") or "operator-ui"),
        "result": result.get("result_status") or ("SUCCEEDED" if result.get("ok") else "FAILED"),
        "state_before": state_before or {},
        "state_after": state_after or {},
        "error": str(result.get("error_message") or ""),
        "request_payload": request_payload,
        "response_summary": {**{key: result.get(key) for key in ("ok", "result_status", "user_message", "next_state", "audit_id")}, "command_result": result.get("command_result") or {}},
        **SAFETY,
    }
    record["audit_id"] = f"operator-command:{record['command_id']}:{stable_hash_v1(record)[:20]}"
    if isinstance(record.get("response_summary"), dict) and isinstance(record["response_summary"].get("command_result"), dict):
        record["response_summary"]["command_result"]["audit_id"] = record["audit_id"]
        record["response_summary"]["command_result"].setdefault("timestamp", record["requested_at"])
    record["content_hash"] = stable_hash_v1(record)
    path = command_audit_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
    return {**record, "path": str(path)}


def _command_result_panel_v1(
    *,
    status_label: str,
    plain_english_result: str,
    next_required_step: str,
    result_status: str,
    job_id: str = "",
    audit_id: str = "",
    timestamp: str = "",
    missing_source_name: str = "",
    expected_source_path: str = "",
    copy_required_path: str = "",
    failure_reason: str = "",
    next_recheck_time: str = "",
) -> dict[str, Any]:
    return {
        "schema_id": "aegis_command_result_panel.v1",
        "status_label": status_label,
        "result_status": result_status,
        "plain_english_result": plain_english_result,
        "next_required_step": next_required_step,
        "job_id": job_id,
        "audit_id": audit_id,
        "timestamp": timestamp or utc_now_v1(),
        "missing_source_name": missing_source_name,
        "expected_source_path": expected_source_path,
        "copy_required_path": copy_required_path or expected_source_path,
        "failure_reason": failure_reason,
        "next_recheck_time": next_recheck_time,
    }


def _empty_response(command: dict[str, Any], *, status: str, message: str, next_state: str = "UNCHANGED", ok: bool = True, command_result: dict[str, Any] | None = None) -> dict[str, Any]:
    fallback_status = "Success" if ok else "Failed"
    return {
        "ok": ok,
        "command_id": command["command_id"],
        "result_status": status,
        "user_message": message,
        "next_state": next_state,
        "audit_id": "",
        "updated_projection_hint": "refresh_current_workspace",
        "error_message": "" if ok else message,
        "command_result": command_result or _command_result_panel_v1(
            status_label=fallback_status,
            plain_english_result=message,
            next_required_step=next_state if next_state != "UNCHANGED" else "No further action is required from this command.",
            result_status=status,
            failure_reason="" if ok else message,
        ),
        **SAFETY,
    }


def _domain_plan(domain_id: str, *, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    report = build_domain_certification_report_v1(truth_root=truth_root, day_utc=day_utc)
    for plan in report.get("domain_repair_actions", []) if isinstance(report.get("domain_repair_actions"), list) else []:
        if str(plan.get("domain_id") or "") == domain_id:
            return plan
    return {}


def execute_aegis_command_v1(
    payload: dict[str, Any],
    *,
    truth_root: Path | str,
    repo_root: Path | str | None = None,
    day_utc: str,
    actor: str = "operator-ui",
    repair_job_runner: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    command_runner: Callable[..., Any] | None = None,
    research_store_root: Path | str | None = None,
) -> dict[str, Any]:
    request = dict(payload or {})
    request.setdefault("requested_by", actor)
    command = command_contract_v1(str(request.get("command_id") or ""))
    target_type = str(request.get("target_type") or "")
    target_id = str(request.get("target_id") or "")
    command_id = command["command_id"]
    result: dict[str, Any]
    state_before: dict[str, Any] = {}
    state_after: dict[str, Any] = {}

    try:
        if command_id == "START_RESEARCH":
            from ops.aegis.research_lab.research_console_v1 import start_research_v1

            body = dict(request.get("payload") or {})
            hypothesis_id = str(body.get("hypothesis_id") or target_id or request.get("hypothesis_id") or "").strip()
            title = str(body.get("title") or body.get("idea") or request.get("title") or hypothesis_id).strip()
            if not hypothesis_id:
                result = _empty_response(command, status="VALIDATION_FAILED", message="Start Research requires a hypothesis_id.", ok=False)
            else:
                start = start_research_v1({"hypothesis_id": hypothesis_id, "idea": title, "symbols": body.get("symbols") or ""}, actor=actor, store_root=Path(research_store_root) if research_store_root else None)
                result = {
                    **_empty_response(command, status="QUEUED", message=start.get("operator_message") or start.get("message") or f"Research started for {title}.", next_state="Queued"),
                    "research_run_id": start.get("research_run_id"),
                    "research_run_status": start.get("research_run_status"),
                    "research_run_trigger_source": start.get("research_run_trigger_source"),
                    "updated_projection_hint": "refresh_hypotheses",
                }
                state_after = {"user_facing_status": "Queued", "research_run_id": start.get("research_run_id")}
        elif command_id == "REPAIR_DOMAIN":
            body = dict(request.get("payload") or {})
            domain_id = str(body.get("domain_id") or target_id or request.get("domain_id") or "").strip()
            plan = _domain_plan(domain_id, truth_root=truth_root, day_utc=day_utc)
            state_before = plan
            if not plan:
                message = f"No delayed repair plan is available for {domain_id}."
                result = _empty_response(command, status="NO_REPAIR_PLAN", message=message, next_state="UNCHANGED", ok=False, command_result=_command_result_panel_v1(status_label="Failed", plain_english_result=message, next_required_step="Refresh the domain certification report and try again if the domain is still delayed.", result_status="NO_REPAIR_PLAN", failure_reason=message))
            elif plan.get("source_setup_required"):
                source = plan.get("required_source_artifact") if isinstance(plan.get("required_source_artifact"), dict) else {}
                label = str(source.get("label") or "required source")
                path = str(source.get("path") or "")
                build = build_domain_source_artifact_v1(truth_root=truth_root, day_utc=day_utc, domain_id=domain_id)
                if build.get("ok") is True:
                    report = build_domain_certification_report_v1(truth_root=truth_root, day_utc=day_utc)
                    paths = write_domain_certification_report_v1(truth_root=truth_root, day_utc=day_utc, payload=report)
                    domain_row = next((row for row in report.get("domains", []) if isinstance(row, dict) and str(row.get("domain_id") or "") == domain_id), {})
                    status = str(domain_row.get("certification_status") or "UNKNOWN")
                    message = f"Built source artifact for {domain_id}. Domain status is {status}."
                    result = {
                        **_empty_response(
                            command,
                            status="BUILT",
                            message=message,
                            next_state="RECERTIFIED",
                            command_result=_command_result_panel_v1(
                                status_label="Source built",
                                plain_english_result=message,
                                next_required_step="Review the refreshed domain certification status.",
                                result_status="BUILT",
                                expected_source_path=str(build.get("artifact_path") or path),
                                copy_required_path=str(build.get("artifact_path") or path),
                            ),
                        ),
                        "repair_plan": plan,
                        "build_result": build,
                        "domain_certification_paths": paths,
                    }
                    state_after = {"domain_repair_state": "BUILT", "domain_status": status, "artifact_path": build.get("artifact_path")}
                else:
                    req = setup_requirements_v1(truth_root=truth_root, day_utc=day_utc, domain_id=domain_id)
                    path = str(build.get("required_output_path") or path or req.get("required_output_path") or "")
                    message = str(build.get("message") or f"Source setup required for {domain_id}: {label}{f' at {path}' if path else ''}.")
                    result = {
                        **_empty_response(
                            command,
                            status=str(build.get("result_status") or "SOURCE_SETUP_REQUIRED"),
                            message=message,
                            next_state="Source setup required",
                            ok=str(build.get("result_status") or "").upper() == "SOURCE_SETUP_REQUIRED",
                            command_result=_command_result_panel_v1(
                                status_label="Source setup required",
                                plain_english_result=message,
                                next_required_step=str(plan.get("repair_action") or req.get("setup_message") or "Provide the missing source, validate it, then re-run domain certification."),
                                result_status=str(build.get("result_status") or "SOURCE_SETUP_REQUIRED"),
                                missing_source_name=label,
                                expected_source_path=path,
                                copy_required_path=path,
                                failure_reason=str(build.get("failure_reason") or ""),
                            ),
                        ),
                        "repair_plan": plan,
                        "build_result": build,
                        "missing_source_name": label,
                        "expected_source_path": path,
                    }
                    state_after = {"domain_repair_state": "SOURCE_SETUP_REQUIRED", "required_source_path": path}
            else:
                job = repair_job_runner({"domain_id": domain_id, "day_utc": day_utc, "repair_plan": plan, "requested_by": actor}) if repair_job_runner else {}
                if not job:
                    message = f"Automatic repair for {domain_id} is not available from this runtime."
                    result = _empty_response(command, status="REPAIR_JOB_UNAVAILABLE", message=message, next_state="UNCHANGED", ok=False, command_result=_command_result_panel_v1(status_label="Failed", plain_english_result=message, next_required_step="Use the listed repair steps or source setup instructions, then re-run certification.", result_status="REPAIR_JOB_UNAVAILABLE", failure_reason=message))
                else:
                    job_id = str(job.get("job_id") or "")
                    next_recheck = str(job.get("next_retry_utc") or job.get("queued_at_utc") or "")
                    job_status = str(job.get("status") or "QUEUED").upper()
                    failed_job = job_status in {"FAILED", "ERROR"}
                    failure_reason = str(job.get("failure_reason") or job.get("failure_detail") or "")
                    message = (
                        f"Repair failed for {domain_id}: {failure_reason or job_status}."
                        if failed_job
                        else f"Repair job queued for {domain_id}: {job_id or 'job recorded'}."
                    )
                    result = {
                        **_empty_response(
                            command,
                            status=job_status,
                            message=message,
                            next_state="Repair failed" if failed_job else "Repair queued",
                            ok=not failed_job,
                            command_result=_command_result_panel_v1(
                                status_label="Failed" if failed_job else "Repair job queued",
                                plain_english_result=message,
                                next_required_step="Review the failure reason, fix provider/source configuration, then queue repair again." if failed_job else "Wait for the queued repair to complete, then refresh or re-run domain certification.",
                                result_status=job_status,
                                job_id=job_id,
                                next_recheck_time=next_recheck,
                                failure_reason=failure_reason if failed_job else "",
                            ),
                        ),
                        "repair_plan": plan,
                        "job": job,
                        "job_id": job.get("job_id"),
                        "next_recheck_time": next_recheck,
                    }
                    state_after = {"domain_repair_state": job_status, "job_id": job.get("job_id"), "failure_reason": failure_reason}
        elif command_id == "UPLOAD_SOURCE":
            body = dict(request.get("payload") or {})
            domain_id = str(body.get("domain_id") or target_id or request.get("domain_id") or "").strip()
            plan = _domain_plan(domain_id, truth_root=truth_root, day_utc=day_utc)
            state_before = plan
            source = plan.get("required_source_artifact") if isinstance(plan.get("required_source_artifact"), dict) else {}
            label = str(source.get("label") or "required source")
            path = str(source.get("path") or "")
            message = f"External source required — not a system failure for {domain_id}: provide {label}{f' at {path}' if path else ''}."
            result = _empty_response(
                command,
                status="SOURCE_SETUP_REQUIRED",
                message=message,
                next_state="WAITING_FOR_INPUT",
                ok=True,
                command_result=_command_result_panel_v1(
                    status_label="External source required — not a system failure",
                    plain_english_result=message,
                    next_required_step="Configure the required source path or place the JSON file at the governed artifact path, then run Validate Source.",
                    result_status="SOURCE_SETUP_REQUIRED",
                    missing_source_name=label,
                    expected_source_path=path,
                    copy_required_path=path,
                ),
            )
            state_after = {"repair_state": "WAITING_FOR_INPUT", "required_source_path": path}
        elif command_id == "DOWNLOAD_SOURCE_TEMPLATE":
            body = dict(request.get("payload") or {})
            domain_id = str(body.get("domain_id") or target_id or request.get("domain_id") or "").strip()
            template = write_domain_source_template_v1(truth_root=Path(truth_root), day_utc=day_utc, domain_id=domain_id)
            if template.get("ok") is True:
                message = str(template.get("message") or f"JSON template written for {domain_id}.")
                result = _empty_response(command, status="TEMPLATE_READY", message=message, next_state="WAITING_FOR_INPUT", command_result=_command_result_panel_v1(status_label="JSON template ready", plain_english_result=message, next_required_step=f"Replace template rows with real governed source data, set {template.get('required_config_key') or 'the required config key'} to the completed file, then run Validate Source.", result_status="TEMPLATE_READY", expected_source_path=str(template.get("template_path") or ""), copy_required_path=str(template.get("template_path") or "")))
                state_after = {"template_path": template.get("template_path"), "required_config_key": template.get("required_config_key"), "required_output_path": template.get("required_output_path")}
            else:
                message = f"No source template is available for {domain_id}."
                result = _empty_response(command, status=str(template.get("result_status") or "UNSUPPORTED_TEMPLATE_DOMAIN"), message=message, next_state="UNCHANGED", ok=False, command_result=_command_result_panel_v1(status_label="Template unavailable", plain_english_result=message, next_required_step="Use the listed source contract requirements for this domain.", result_status=str(template.get("result_status") or "UNSUPPORTED_TEMPLATE_DOMAIN"), failure_reason=message))
                state_after = {"template_available": False}
        elif command_id == "DOWNLOAD_EOD_SOURCE_TEMPLATE":
            from ops.tools.manage_us_equities_eod_source_v1 import template_csv_v1, write_required_universe_v1

            body = dict(request.get("payload") or {})
            domain_id = str(body.get("domain_id") or target_id or request.get("domain_id") or "US_EQUITIES_EOD").strip()
            universe = write_required_universe_v1(truth_root=Path(truth_root), day_utc=day_utc)
            template_path = Path(truth_root).expanduser().resolve() / "reports" / "final_eod_market_data_v1" / day_utc / "us_equities_eod_source_template.csv"
            template_path.parent.mkdir(parents=True, exist_ok=True)
            template_path.write_text(template_csv_v1(day_utc=day_utc, symbols=universe["symbols"]), encoding="utf-8")
            message = f"CSV template written for {domain_id}: {template_path}."
            result = _empty_response(command, status="TEMPLATE_READY", message=message, next_state="WAITING_FOR_INPUT", command_result=_command_result_panel_v1(status_label="CSV template ready", plain_english_result=message, next_required_step="Fill every row with target-day OHLCV and set AEGIS_US_EQUITIES_EOD_SOURCE_FILE to the completed file before certifying.", result_status="TEMPLATE_READY", expected_source_path=str(template_path), copy_required_path=str(template_path)))
            state_after = {"template_path": str(template_path), "required_universe_path": universe["path"], "required_universe_size": universe["count"]}
        elif command_id == "CERTIFY_SOURCE":
            from ops.tools.manage_us_equities_eod_source_v1 import certify_source_file_v1

            body = dict(request.get("payload") or {})
            domain_id = str(body.get("domain_id") or target_id or request.get("domain_id") or "US_EQUITIES_EOD").strip()
            source_file = str(body.get("source_file") or os.environ.get("AEGIS_US_EQUITIES_EOD_SOURCE_FILE") or "").strip()
            if domain_id != "US_EQUITIES_EOD" or not source_file:
                message = "Certify from source file requires AEGIS_US_EQUITIES_EOD_SOURCE_FILE or payload.source_file for US_EQUITIES_EOD."
                result = _empty_response(command, status="SOURCE_SETUP_REQUIRED", message=message, next_state="WAITING_FOR_INPUT", ok=False, command_result=_command_result_panel_v1(status_label="Source setup required", plain_english_result=message, next_required_step="Set AEGIS_US_EQUITIES_EOD_SOURCE_FILE to a complete CSV/JSON file, then retry.", result_status="SOURCE_SETUP_REQUIRED", missing_source_name="AEGIS_US_EQUITIES_EOD_SOURCE_FILE", expected_source_path=source_file, copy_required_path=source_file, failure_reason=message))
                state_after = {"repair_state": "WAITING_FOR_INPUT", "source_exists": False}
            else:
                cert = certify_source_file_v1(source_file=Path(source_file), day_utc=day_utc, truth_root=Path(truth_root))
                build = cert.get("build_result") if isinstance(cert.get("build_result"), dict) else {}
                validation = cert.get("validation") if isinstance(cert.get("validation"), dict) else {}
                if cert.get("ok") is True:
                    message = f"Certified US_EQUITIES_EOD from source file: {source_file}."
                    result = _empty_response(command, status="CERTIFIED", message=message, next_state="RECERTIFIED", command_result=_command_result_panel_v1(status_label="Source certified", plain_english_result=message, next_required_step="Review the refreshed domain certification status.", result_status="CERTIFIED", expected_source_path=str(build.get("artifact_path") or source_file), copy_required_path=str(build.get("artifact_path") or source_file)))
                    state_after = {"repair_state": "CERTIFIED", "source_file": source_file, "artifact_path": build.get("artifact_path"), "certification_status": build.get("certification_status")}
                else:
                    missing = validation.get("missing_symbols") or []
                    stale = validation.get("stale_symbols") or []
                    invalid = validation.get("invalid_ohlcv_rows") or []
                    dupes = validation.get("duplicate_symbols") or []
                    message = f"EOD source failed validation: missing={len(missing)} stale={len(stale)} invalid_ohlcv={len(invalid)} duplicates={len(dupes)}."
                    result = _empty_response(command, status="INVALID_SOURCE", message=message, next_state="WAITING_FOR_INPUT", ok=False, command_result=_command_result_panel_v1(status_label="Source validation failed", plain_english_result=message, next_required_step="Fix the listed source rows and rerun certification.", result_status="INVALID_SOURCE", expected_source_path=source_file, copy_required_path=source_file, failure_reason=message))
                    state_after = {"repair_state": "WAITING_FOR_INPUT", "validation": validation}
        elif command_id == "QUEUE_CERTIFICATION":
            from ops.aegis.dynamic_certification_queue_v1 import build_dynamic_certification_queue_v1, write_dynamic_certification_queue_v1

            body = dict(request.get("payload") or {})
            queue_day = str(body.get("day_utc") or body.get("operational_day") or day_utc)
            queue = build_dynamic_certification_queue_v1(truth_root=truth_root, day_utc=queue_day)
            paths = write_dynamic_certification_queue_v1(truth_root=truth_root, payload=queue)
            requested = queue.get("requested_symbols") if isinstance(queue.get("requested_symbols"), list) else []
            message = f"Dynamic certification queue built for {queue_day}: {len(requested)} symbols requested."
            result = _empty_response(
                command,
                status=str(queue.get("certification_status") or "EMPTY"),
                message=message,
                next_state="READY_TO_CERTIFY" if requested else "NO_DYNAMIC_CERTIFICATION_NEEDED",
                ok=True,
                command_result=_command_result_panel_v1(
                    status_label="Certification queue ready" if requested else "No certification queue",
                    plain_english_result=message,
                    next_required_step="Run Certify Selected Symbols for queued symbols." if requested else "No additional symbols met the dynamic queue policy.",
                    result_status=str(queue.get("certification_status") or "EMPTY"),
                    expected_source_path=paths.get("json", ""),
                    copy_required_path=paths.get("json", ""),
                ),
            )
            result["queue_path"] = paths.get("json", "")
            result["requested_symbols"] = requested
            result["estimated_provider_load"] = queue.get("estimated_provider_load", 0)
            state_after = {"queue_path": paths.get("json", ""), "requested_symbols": requested}
        elif command_id == "CERTIFY_SELECTED_SYMBOLS":
            from ops.aegis.dynamic_certification_queue_v1 import certify_dynamic_queue_symbols_v1

            body = dict(request.get("payload") or {})
            queue_day = str(body.get("day_utc") or body.get("operational_day") or day_utc)
            selected_symbols = body.get("symbols") if isinstance(body.get("symbols"), list) else []
            cert = certify_dynamic_queue_symbols_v1(truth_root=truth_root, day_utc=queue_day, symbols=[str(symbol) for symbol in selected_symbols])
            certified = cert.get("certified_symbols") if isinstance(cert.get("certified_symbols"), list) else []
            failed = cert.get("failed_symbols") if isinstance(cert.get("failed_symbols"), list) else []
            message = str(cert.get("message") or f"Dynamic certification completed: {len(certified)} certified, {len(failed)} failed.")
            result = _empty_response(
                command,
                status=str(cert.get("result_status") or "FAILED"),
                message=message,
                next_state="CERTIFICATION_REFRESHED" if cert.get("ok") else "CERTIFICATION_FAILED_CLOSED",
                ok=bool(cert.get("ok")),
                command_result=_command_result_panel_v1(
                    status_label="Dynamic certification complete" if cert.get("ok") else "Dynamic certification failed closed",
                    plain_english_result=message,
                    next_required_step="Refresh Candidate Funnel and rerun candidate promotion eligibility." if cert.get("ok") else "Review provider failures; failed symbols remain excluded.",
                    result_status=str(cert.get("result_status") or "FAILED"),
                    expected_source_path=str(cert.get("final_eod_artifact_path") or cert.get("queue_path") or ""),
                    copy_required_path=str(cert.get("final_eod_artifact_path") or cert.get("queue_path") or ""),
                    failure_reason="" if cert.get("ok") else message,
                ),
            )
            result.update(cert)
            state_after = {"certified_symbols": certified, "failed_symbols": failed, "queue_path": cert.get("queue_path"), "final_eod_artifact_path": cert.get("final_eod_artifact_path")}
        elif command_id == "VIEW_CERTIFICATION_RESULT":
            from ops.aegis.dynamic_certification_queue_v1 import latest_dynamic_certification_queue_v1

            body = dict(request.get("payload") or {})
            queue_day = str(body.get("day_utc") or body.get("operational_day") or day_utc)
            queue_path, queue = latest_dynamic_certification_queue_v1(truth_root=truth_root, day_utc=queue_day)
            if queue:
                requested = queue.get("requested_symbols") if isinstance(queue.get("requested_symbols"), list) else []
                results = queue.get("certification_results") if isinstance(queue.get("certification_results"), list) else []
                message = f"Dynamic certification status for {queue_day}: {queue.get('certification_status', 'UNKNOWN')} ({len(requested)} requested, {len(results)} results)."
                status = str(queue.get("certification_status") or "UNKNOWN")
                ok = True
            else:
                requested = []
                results = []
                message = f"No dynamic certification queue exists for {queue_day}."
                status = "MISSING"
                ok = False
            result = _empty_response(
                command,
                status=status,
                message=message,
                next_state="UNCHANGED",
                ok=ok,
                command_result=_command_result_panel_v1(
                    status_label="Dynamic certification result" if ok else "No certification result",
                    plain_english_result=message,
                    next_required_step="Review Candidate Funnel recommendations." if ok else "Queue certification first if candidate pressure exists.",
                    result_status=status,
                    expected_source_path=str(queue_path or ""),
                    copy_required_path=str(queue_path or ""),
                    failure_reason="" if ok else message,
                ),
            )
            result["queue_path"] = str(queue_path or "")
            result["requested_symbols"] = requested
            result["certification_results"] = results
            state_after = {"queue_path": str(queue_path or ""), "certification_status": status}

        elif command_id == "RUN_CONTEXT_READINESS_COMMAND":
            body = dict(request.get("payload") or {})
            action_id = str(body.get("action_id") or target_id or "repair_context_readiness").strip()
            command_argv = ["npm", "run", "aegis:repair-context-readiness"]
            if action_id != "repair_context_readiness":
                message = f"Unsupported context readiness action: {action_id}."
                result = _empty_response(
                    command,
                    status="REJECTED",
                    message=message,
                    next_state="UNCHANGED",
                    ok=False,
                    command_result=_command_result_panel_v1(
                        status_label="Rejected",
                        plain_english_result=message,
                        next_required_step="Use the allowlisted repair_context_readiness action.",
                        result_status="REJECTED",
                        failure_reason=message,
                    ),
                )
            else:
                runner = command_runner or subprocess.run
                env = {key: value for key, value in os.environ.items() if key in {"PATH", "HOME", "LANG", "LC_ALL", "TZ"} and value}
                env["TARGET_DAY"] = str(body.get("day_utc") or day_utc)
                env["AEGIS_TRUTH_ROOT"] = str(Path(truth_root).expanduser().resolve())
                env["CI"] = "1"
                try:
                    completed = runner(
                        command_argv,
                        cwd=str(Path(repo_root).resolve() if repo_root else Path(__file__).resolve().parents[2]),
                        env=env,
                        text=True,
                        capture_output=True,
                        timeout=120,
                        check=False,
                        shell=False,
                    )
                    exit_code = int(getattr(completed, "returncode", 1))
                    stdout = str(getattr(completed, "stdout", "") or "")[-4000:]
                    stderr = str(getattr(completed, "stderr", "") or "")[-2000:]
                except subprocess.TimeoutExpired as exc:
                    exit_code = 124
                    stdout = str(exc.stdout or "")[-4000:]
                    stderr = str(exc.stderr or "")[-2000:]
                ok = exit_code == 0
                status = "PASS" if ok else ("TIMEOUT" if exit_code == 124 else "FAILED")
                message = "Context readiness repair completed." if ok else "Context readiness repair did not pass."
                result = {
                    **_empty_response(
                        command,
                        status=status,
                        message=message,
                        next_state="REFRESH_CONTEXT_READINESS" if ok else "UNCHANGED",
                        ok=ok,
                        command_result=_command_result_panel_v1(
                            status_label="Context readiness repaired" if ok else "Context readiness repair failed",
                            plain_english_result=message,
                            next_required_step="Review runtime truth and verified graph outputs. Policy gates remain governed by runtime truth.",
                            result_status=status,
                            failure_reason="" if ok else stderr or stdout,
                        ),
                    ),
                    "portal_action_id": "repair_context_readiness",
                    "command_argv": command_argv,
                    "shell": False,
                    "exit_code": exit_code,
                    "stdout_excerpt": stdout,
                    "stderr_excerpt": stderr,
                    "safety_policy_summary": {
                        "broker_submit_transmit_allowed": False,
                        "broker_execution_allowed": False,
                        "autonomous_execution_allowed": False,
                        "trade_advice_allowed": False,
                        "manual_trade_capture_allowed": False,
                        "promotion_bypass_allowed": False,
                    },
                }
                state_after = {"portal_action_id": "repair_context_readiness", "exit_code": exit_code, "status": status}
        elif command_id == "REFRESH_CANDIDATE_CONTRACTS":
            body = dict(request.get("payload") or {})
            target_day = str(body.get("day_utc") or day_utc)
            command_steps = [
                ["npm", "run", "aegis:refresh-market-data"],
                ["npm", "run", "aegis:data-registry"],
                ["npm", "run", "aegis:candidate-contracts"],
            ]
            runner = command_runner or subprocess.run
            env = {key: value for key, value in os.environ.items() if key in {"PATH", "HOME", "LANG", "LC_ALL", "TZ"} and value}
            env["TARGET_DAY"] = target_day
            env["AEGIS_TRUTH_ROOT"] = str(Path(truth_root).expanduser().resolve())
            env["CI"] = "1"
            step_results = []
            exit_code = 0
            for argv in command_steps:
                try:
                    completed = runner(
                        argv,
                        cwd=str(Path(repo_root).resolve() if repo_root else Path(__file__).resolve().parents[2]),
                        env=env,
                        text=True,
                        capture_output=True,
                        timeout=180,
                        check=False,
                        shell=False,
                    )
                    step_exit = int(getattr(completed, "returncode", 1))
                    stdout = str(getattr(completed, "stdout", "") or "")[-2000:]
                    stderr = str(getattr(completed, "stderr", "") or "")[-1000:]
                except subprocess.TimeoutExpired as exc:
                    step_exit = 124
                    stdout = str(exc.stdout or "")[-2000:]
                    stderr = str(exc.stderr or "")[-1000:]
                step_results.append({"argv": argv, "shell": False, "exit_code": step_exit, "stdout_excerpt": stdout, "stderr_excerpt": stderr})
                if step_exit != 0:
                    exit_code = step_exit
                    break
            ok = exit_code == 0
            status = "PASS" if ok else ("TIMEOUT" if exit_code == 124 else "FAILED")
            message = f"Candidate contracts refreshed for {target_day}." if ok else f"Candidate contracts refresh failed for {target_day}."
            result = {
                **_empty_response(
                    command,
                    status=status,
                    message=message,
                    next_state="REFRESH_CANDIDATE_CONTRACTS" if ok else "UNCHANGED",
                    ok=ok,
                    command_result=_command_result_panel_v1(
                        status_label="Candidate contracts refreshed" if ok else "Candidate contracts refresh failed",
                        plain_english_result=message,
                        next_required_step="Rerun candidate diagnostics/canonical projection only after reviewing refreshed contract counts. Policy gates remain governed by runtime truth.",
                        result_status=status,
                        failure_reason="" if ok else (step_results[-1].get("stderr_excerpt") or step_results[-1].get("stdout_excerpt") if step_results else message),
                    ),
                ),
                "portal_action_id": "refresh_candidate_contracts",
                "command_argv_sequence": command_steps,
                "shell": False,
                "exit_code": exit_code,
                "step_results": step_results,
                "safety_policy_summary": {
                    "broker_submit_transmit_allowed": False,
                    "broker_execution_allowed": False,
                    "autonomous_execution_allowed": False,
                    "trade_advice_allowed": False,
                    "manual_trade_capture_allowed": False,
                    "promotion_bypass_allowed": False,
                },
            }
            state_after = {"portal_action_id": "refresh_candidate_contracts", "exit_code": exit_code, "status": status}
        elif command_id == "REFRESH_CANDIDATE_PROJECTION":
            body = dict(request.get("payload") or {})
            target_day = str(body.get("day_utc") or day_utc)
            command_steps = [
                ["npm", "run", "aegis:signal-evidence-graph"],
                ["npm", "run", "aegis:candidate-contracts"],
                ["npm", "run", "aegis:signal-death-report"],
                ["npm", "run", "aegis:candidate-diagnostics"],
                ["npm", "run", "aegis:paper:review-queue"],
                ["npm", "run", "aegis:roll-candidate-state"],
                ["npm", "run", "aegis:run-history"],
                ["npm", "run", "aegis:canonical-operator-state"],
                ["npm", "run", "aegis:audit"],
                ["npm", "run", "aegis:portal-smoke"],
            ]
            runner = command_runner or subprocess.run
            env = {key: value for key, value in os.environ.items() if key in {"PATH", "HOME", "LANG", "LC_ALL", "TZ"} and value}
            env["TARGET_DAY"] = target_day
            env["AEGIS_TRUTH_ROOT"] = str(Path(truth_root).expanduser().resolve())
            env["CI"] = "1"
            step_results = []
            exit_code = 0
            for argv in command_steps:
                try:
                    completed = runner(
                        argv,
                        cwd=str(Path(repo_root).resolve() if repo_root else Path(__file__).resolve().parents[2]),
                        env=env,
                        text=True,
                        capture_output=True,
                        timeout=180,
                        check=False,
                        shell=False,
                    )
                    step_exit = int(getattr(completed, "returncode", 1))
                    stdout = str(getattr(completed, "stdout", "") or "")[-2000:]
                    stderr = str(getattr(completed, "stderr", "") or "")[-1000:]
                except subprocess.TimeoutExpired as exc:
                    step_exit = 124
                    stdout = str(exc.stdout or "")[-2000:]
                    stderr = str(exc.stderr or "")[-1000:]
                step_results.append({"argv": argv, "shell": False, "exit_code": step_exit, "stdout_excerpt": stdout, "stderr_excerpt": stderr})
                if step_exit != 0:
                    exit_code = step_exit
                    break
            ok = exit_code == 0
            status = "PASS" if ok else ("TIMEOUT" if exit_code == 124 else "FAILED")
            message = f"Candidate projection refreshed for {target_day}." if ok else f"Candidate projection refresh failed for {target_day}."
            canonical_path = Path(truth_root).expanduser().resolve() / "reports" / "aegis_canonical_operator_state_v1" / target_day / "canonical_operator_state.v1.json"
            try:
                canonical_payload = json.loads(canonical_path.read_text(encoding="utf-8"))
            except Exception:
                canonical_payload = {}
            projection = canonical_payload.get("candidate_ui_projection") if isinstance(canonical_payload.get("candidate_ui_projection"), dict) else {}
            projection_source_paths = {
                "canonical_operator_state": str(canonical_path),
                "candidate_diagnostics": str(projection.get("candidate_diagnostics_path") or ""),
                "candidate_contracts": str(projection.get("candidate_contracts_path") or ""),
                "candidate_review_packet": str(projection.get("candidate_review_packet_path") or ""),
                "paper_review_queue": str(projection.get("paper_review_queue_path") or ""),
            }
            result = {
                **_empty_response(
                    command,
                    status=status,
                    message=message,
                    next_state="REFRESH_CANDIDATE_PROJECTION" if ok else "UNCHANGED",
                    ok=ok,
                    command_result=_command_result_panel_v1(
                        status_label="Candidate projection refreshed" if ok else "Candidate projection refresh failed",
                        plain_english_result=message,
                        next_required_step="Reload the Candidate page and review source paths/hashes. Policy gates remain governed by runtime truth.",
                        result_status=status,
                        failure_reason="" if ok else (step_results[-1].get("stderr_excerpt") or step_results[-1].get("stdout_excerpt") if step_results else message),
                    ),
                ),
                "portal_action_id": "refresh_candidate_projection",
                "command_argv_sequence": command_steps,
                "shell": False,
                "exit_code": exit_code,
                "step_results": step_results,
                "candidate_projection_counts": {
                    "has_candidate_ui_projection": bool(projection),
                    "candidate_contract_count": int(projection.get("candidate_contract_count") or 0),
                    "awaiting_review_count": int(projection.get("awaiting_review_count") or 0),
                    "reviewable_candidate_count": int(projection.get("reviewable_candidate_count") or 0),
                    "diagnostics_status": str(projection.get("diagnostics_status") or "UNKNOWN"),
                    "paper_review_queue_status": str(projection.get("paper_review_queue_status") or "UNKNOWN"),
                    "projection_status": str(projection.get("projection_status") or "MISSING"),
                },
                "source_paths": projection_source_paths,
                "safety_policy_summary": {
                    "broker_submit_transmit_allowed": False,
                    "broker_execution_allowed": False,
                    "autonomous_execution_allowed": False,
                    "trade_advice_allowed": False,
                    "manual_trade_capture_allowed": False,
                    "promotion_bypass_allowed": False,
                },
            }
            state_after = {"portal_action_id": "refresh_candidate_projection", "exit_code": exit_code, "status": status}
        elif command_id in {"APPROVE_PAPER_CANDIDATE", "REJECT_PAPER_CANDIDATE", "RECORD_PAPER_ENTRY", "PAPER_TRADE_CANDIDATE", "RECORD_PAPER_EXIT"}:
            from ops.aegis.canonical_operator_state_v1 import build_canonical_operator_state_v1, write_canonical_operator_state_v1
            from ops.aegis.human_reviewed_paper_mode_v1 import (
                paper_review_decisions_path_v1,
                paper_trade_outcomes_path_v1,
                paper_trade_receipts_path_v1,
                record_paper_review_decision_v1,
                record_paper_trade_exit_v1,
                record_paper_trade_receipt_v1,
            )

            body = dict(request.get("payload") or {})
            candidate_id = str(body.get("candidate_id") or target_id or "").strip()
            if not candidate_id:
                result = _empty_response(command, status="VALIDATION_FAILED", message="Candidate id is required.", ok=False)
            elif command_id in {"APPROVE_PAPER_CANDIDATE", "REJECT_PAPER_CANDIDATE"}:
                decision = "APPROVE" if command_id == "APPROVE_PAPER_CANDIDATE" else "REJECT"
                event = record_paper_review_decision_v1(
                    truth_root=Path(truth_root),
                    day_utc=day_utc,
                    candidate_id=candidate_id,
                    decision=decision,
                    reason=str(body.get("reason") or ("APPROVED_FOR_PAPER" if decision == "APPROVE" else "REJECTED_BY_OPERATOR")),
                    operator=str(body.get("operator") or actor or "operator-ui"),
                )
                canonical = build_canonical_operator_state_v1(truth_root=truth_root, repo_root=repo_root or Path(__file__).resolve().parents[2], day_utc=day_utc)
                canonical_paths = write_canonical_operator_state_v1(truth_root=truth_root, day_utc=day_utc, payload=canonical)
                status = str(event.get("status") or "PAPER_REVIEW_DECISION_RECORDED")
                message = f"{command['label']} recorded for {candidate_id}."
                result = {
                    **_empty_response(command, status=status, message=message, next_state=status, ok=True, command_result=_command_result_panel_v1(status_label=command["label"], plain_english_result=message, next_required_step="Record paper entry if approved; otherwise no further action for rejected candidates.", result_status=status, expected_source_path=str(paper_review_decisions_path_v1(truth_root=Path(truth_root), day_utc=day_utc)))),
                    "event": event,
                    "canonical_operator_state": canonical_paths,
                    "workflow_state": status,
                    "row_update_only": True,
                    "paper_only": True,
                    "live_trade_eligible": False,
                    "broker_submit_transmit_called": False,
                }
                state_after = {"candidate_id": candidate_id, "workflow_state": status}
            elif command_id in {"RECORD_PAPER_ENTRY", "PAPER_TRADE_CANDIDATE"}:
                if command_id == "PAPER_TRADE_CANDIDATE":
                    record_paper_review_decision_v1(
                        truth_root=Path(truth_root),
                        day_utc=day_utc,
                        candidate_id=candidate_id,
                        decision="APPROVE",
                        reason=str(body.get("reason") or "OPERATOR_PAPER_TRADE_CONFIRMED"),
                        operator=str(body.get("operator") or actor or "operator-ui"),
                    )
                receipt = record_paper_trade_receipt_v1(
                    truth_root=Path(truth_root),
                    day_utc=day_utc,
                    candidate_id=candidate_id,
                    action=str(body.get("action") or "BUY"),
                    paper_entry_price=str(body.get("paper_entry_price") or body.get("entry_price") or ""),
                    quantity=str(body.get("quantity") or ""),
                    notional=str(body.get("notional") or ""),
                    timestamp_utc=str(body.get("timestamp") or body.get("timestamp_utc") or ""),
                    operator=str(body.get("operator") or actor or "operator-ui"),
                    notes=str(body.get("notes") or ""),
                )
                canonical = build_canonical_operator_state_v1(truth_root=truth_root, repo_root=repo_root or Path(__file__).resolve().parents[2], day_utc=day_utc)
                canonical_paths = write_canonical_operator_state_v1(truth_root=truth_root, day_utc=day_utc, payload=canonical)
                message = f"SIMULATED_PAPER paper trade recorded for {candidate_id}." if command_id == "PAPER_TRADE_CANDIDATE" else f"SIMULATED_PAPER entry recorded for {candidate_id}."
                result = {
                    **_empty_response(command, status="SIMULATED_PAPER_RECEIPT_RECORDED", message=message, next_state="PAPER_POSITION_OPEN", ok=True, command_result=_command_result_panel_v1(status_label="Paper trade recorded" if command_id == "PAPER_TRADE_CANDIDATE" else "Paper entry recorded", plain_english_result=message, next_required_step="Monitor the simulated paper position and record exit when closed.", result_status="SIMULATED_PAPER_RECEIPT_RECORDED", expected_source_path=str(paper_trade_receipts_path_v1(truth_root=Path(truth_root), day_utc=day_utc)))),
                    "receipt": receipt,
                    "receipt_type": "SIMULATED_PAPER",
                    "canonical_operator_state": canonical_paths,
                    "workflow_state": "PAPER_POSITION_OPEN",
                    "row_update_only": True,
                    "paper_only": True,
                    "live_trade_eligible": False,
                    "broker_submit_transmit_called": False,
                }
                state_after = {"candidate_id": candidate_id, "workflow_state": "PAPER_POSITION_OPEN", "receipt_type": "SIMULATED_PAPER"}
            else:
                receipt = record_paper_trade_exit_v1(
                    truth_root=Path(truth_root),
                    day_utc=day_utc,
                    candidate_id=candidate_id,
                    paper_exit_price=str(body.get("paper_exit_price") or body.get("exit_price") or ""),
                    timestamp_utc=str(body.get("timestamp") or body.get("timestamp_utc") or ""),
                    operator=str(body.get("operator") or actor or "operator-ui"),
                    notes=str(body.get("notes") or ""),
                    exit_reason_selected_by_operator=str(body.get("exit_reason_selected_by_operator") or body.get("exit_reason") or ""),
                )
                canonical = build_canonical_operator_state_v1(truth_root=truth_root, repo_root=repo_root or Path(__file__).resolve().parents[2], day_utc=day_utc)
                canonical_paths = write_canonical_operator_state_v1(truth_root=truth_root, day_utc=day_utc, payload=canonical)
                message = f"SIMULATED_PAPER exit recorded for {candidate_id}."
                result = {
                    **_empty_response(command, status="PAPER_OUTCOME_CLOSED", message=message, next_state="PAPER_POSITION_CLOSED", ok=True, command_result=_command_result_panel_v1(status_label="Paper exit recorded", plain_english_result=message, next_required_step="Review closed simulated outcome. No broker execution occurred.", result_status="PAPER_OUTCOME_CLOSED", expected_source_path=str(paper_trade_outcomes_path_v1(truth_root=Path(truth_root), day_utc=day_utc)))),
                    "receipt": receipt,
                    "receipt_type": "SIMULATED_PAPER",
                    "canonical_operator_state": canonical_paths,
                    "workflow_state": "PAPER_POSITION_CLOSED",
                    "row_update_only": True,
                    "paper_only": True,
                    "live_trade_eligible": False,
                    "broker_submit_transmit_called": False,
                }
                state_after = {"candidate_id": candidate_id, "workflow_state": "PAPER_POSITION_CLOSED", "receipt_type": "SIMULATED_PAPER"}

        elif command_id == "VALIDATE_SOURCE":
            body = dict(request.get("payload") or {})
            domain_id = str(body.get("domain_id") or target_id or request.get("domain_id") or "").strip()
            plan = _domain_plan(domain_id, truth_root=truth_root, day_utc=day_utc)
            state_before = plan
            source = plan.get("required_source_artifact") if isinstance(plan.get("required_source_artifact"), dict) else {}
            label = str(source.get("label") or "required source")
            if domain_id == "US_EQUITIES_EOD" and (body.get("source_file") or os.environ.get("AEGIS_US_EQUITIES_EOD_SOURCE_FILE")):
                from ops.tools.manage_us_equities_eod_source_v1 import validate_source_file_v1

                source_file = str(body.get("source_file") or os.environ.get("AEGIS_US_EQUITIES_EOD_SOURCE_FILE") or "")
                validation = validate_source_file_v1(source_file=Path(source_file), day_utc=day_utc, truth_root=Path(truth_root))
                ok = validation.get("validation_status") == "VALID"
                message = f"EOD source validation {'passed' if ok else 'failed'}: coverage {validation.get('coverage_percentage')}%."
                result = _empty_response(command, status="VALIDATED" if ok else "INVALID_SOURCE", message=message, next_state="READY_TO_CERTIFY" if ok else "WAITING_FOR_INPUT", ok=ok, command_result=_command_result_panel_v1(status_label="Source validated" if ok else "Source validation failed", plain_english_result=message, next_required_step="Run Certify from source file." if ok else "Fix missing/stale/invalid/duplicate rows, then validate again.", result_status="VALIDATED" if ok else "INVALID_SOURCE", expected_source_path=source_file, copy_required_path=source_file, failure_reason="" if ok else message))
                state_after = {"repair_state": "VALIDATING", "source_exists": Path(source_file).exists(), "validation": validation}
            else:
                build = build_domain_source_artifact_v1(truth_root=truth_root, day_utc=day_utc, domain_id=domain_id)
                path = str(build.get("artifact_path") or build.get("required_output_path") or source.get("path") or "")
                if build.get("ok") is True:
                    report = build_domain_certification_report_v1(truth_root=truth_root, day_utc=day_utc)
                    paths = write_domain_certification_report_v1(truth_root=truth_root, day_utc=day_utc, payload=report)
                    domain_row = next((row for row in report.get("domains", []) if isinstance(row, dict) and str(row.get("domain_id") or "") == domain_id), {})
                    status = str(domain_row.get("certification_status") or "UNKNOWN")
                    message = f"Source validated for {domain_id}: {path}. Domain status is {status}."
                    result = _empty_response(command, status="VALIDATED", message=message, next_state="RECERTIFIED", command_result=_command_result_panel_v1(status_label="Source validated", plain_english_result=message, next_required_step="Review the refreshed domain certification status.", result_status="VALIDATED", expected_source_path=path, copy_required_path=path))
                    state_after = {"repair_state": "VALIDATING", "source_exists": True, "domain_status": status, "written_paths": paths}
                else:
                    message = str(build.get("message") or f"Required source for {domain_id} is still missing: {label}{f' at {path}' if path else ''}.")
                    result = _empty_response(command, status=str(build.get("result_status") or "SOURCE_MISSING"), message=message, next_state="WAITING_FOR_INPUT", ok=False, command_result=_command_result_panel_v1(status_label="Source setup required", plain_english_result=message, next_required_step="Provide the missing source, then run Validate Source again.", result_status=str(build.get("result_status") or "SOURCE_MISSING"), missing_source_name=label, expected_source_path=path, copy_required_path=path, failure_reason=message))
                    state_after = {"repair_state": "WAITING_FOR_INPUT", "source_exists": False}
        elif command_id == "RECHECK_DOMAIN":
            body = dict(request.get("payload") or {})
            domain_id = str(body.get("domain_id") or target_id or request.get("domain_id") or "").strip()
            before_plan = _domain_plan(domain_id, truth_root=truth_root, day_utc=day_utc)
            state_before = before_plan
            build = build_domain_source_artifact_v1(truth_root=truth_root, day_utc=day_utc, domain_id=domain_id) if domain_id in {"US_EQUITIES_EOD", "MACRO_CALENDAR", "EARNINGS_EVENTS", "CORPORATE_ACTIONS"} else {}
            report = build_domain_certification_report_v1(truth_root=truth_root, day_utc=day_utc)
            paths = write_domain_certification_report_v1(truth_root=truth_root, day_utc=day_utc, payload=report)
            after_plan = next((row for row in report.get("domain_repair_actions", []) if isinstance(row, dict) and str(row.get("domain_id") or "") == domain_id), {})
            domain_row = next((row for row in report.get("domains", []) if isinstance(row, dict) and str(row.get("domain_id") or "") == domain_id), {})
            status = str(domain_row.get("certification_status") or "UNKNOWN")
            if after_plan:
                message = f"Rechecked {domain_id}. Domain remains {status}: {after_plan.get('reason') or 'repair still open'}."
                result_status = "RECERTIFICATION_STILL_DELAYED"
                next_step = str(after_plan.get("repair_action") or "Continue the listed repair steps.")
            else:
                message = f"Rechecked {domain_id}. Domain status is {status}; no open repair plan remains."
                result_status = "COMPLETED"
                next_step = "No further repair action is required for this domain."
            result = _empty_response(command, status=result_status, message=message, next_state="RECERTIFYING", ok=not bool(after_plan), command_result=_command_result_panel_v1(status_label="Domain rechecked" if after_plan else "Repair completed", plain_english_result=message, next_required_step=next_step, result_status=result_status, failure_reason="" if not after_plan else message))
            state_after = {"repair_state": result_status, "domain_status": status, "written_paths": paths, "build_result": build}
        elif command_id == "GENERATE_BACKFILLED_EXIT_PLAN":
            from ops.aegis.trade_lifecycle.trade_evaluation_projection_v1 import build_paper_trade_evaluation_projection_v1
            from ops.aegis.trade_lifecycle.trade_intent_ledger_v1 import generate_backfilled_exit_plan_v1
            from ops.aegis.trade_lifecycle.daily_exit_review_v1 import build_daily_exit_review_v1, write_daily_exit_review_v1
            from ops.aegis.trade_lifecycle.exit_review_projection_v1 import build_exit_review_projection_v1, write_exit_review_projection_v1

            body = dict(request.get("payload") or {})
            wanted_trade = str(body.get("trade_id") or target_id or "")
            wanted_position = str(body.get("position_id") or target_id or "")
            projection = build_paper_trade_evaluation_projection_v1(truth_root=truth_root, day_utc=day_utc)
            def _exit_target_matches(row: dict) -> bool:
                trade_id = str(row.get("trade_id") or "")
                position_id = str(row.get("position_id") or trade_id.replace(":", "_"))
                symbol = str(row.get("symbol") or "")
                return bool(
                    trade_id == wanted_trade
                    or trade_id == wanted_position
                    or trade_id.replace(":", "_") in {wanted_trade, wanted_position}
                    or position_id in {wanted_trade, wanted_position}
                    or symbol == wanted_trade
                )
            trade = next((row for row in projection.get("open_trades") or [] if isinstance(row, dict) and _exit_target_matches(row)), {})
            if not trade:
                message = f"No open trade was found for {target_id}."
                result = _empty_response(command, status="TRADE_NOT_FOUND", message=message, next_state="UNCHANGED", ok=False, command_result=_command_result_panel_v1(status_label="Backfill unavailable", plain_english_result=message, next_required_step="Refresh Performance/Exit Review and try again for a visible open trade.", result_status="TRADE_NOT_FOUND", failure_reason=message))
                state_after = {"backfill_status": "TRADE_NOT_FOUND"}
            else:
                backfill = generate_backfilled_exit_plan_v1(truth_root=truth_root, day_utc=day_utc, trade=trade, operator_or_system=actor)
                daily = build_daily_exit_review_v1(truth_root=truth_root, day_utc=day_utc)
                daily_paths = write_daily_exit_review_v1(truth_root=truth_root, day_utc=day_utc, payload=daily)
                exit_projection = build_exit_review_projection_v1(truth_root=truth_root, day_utc=day_utc)
                exit_paths = write_exit_review_projection_v1(truth_root=truth_root, day_utc=day_utc, payload=exit_projection)
                message = "Backfilled exit intent created." if backfill.get("status") == "BACKFILLED_INTENT_CREATED" else "Backfilled exit intent already exists."
                result = _empty_response(command, status=str(backfill.get("status") or "BACKFILLED"), message=message, next_state="EXIT_REVIEW_REFRESHED", ok=True, command_result=_command_result_panel_v1(status_label="Backfilled exit plan ready", plain_english_result=message, next_required_step="Review Original Exit Plan and Current Exit Plan in Exit Review. Thesis details were not invented.", result_status=str(backfill.get("status") or "BACKFILLED"), expected_source_path=str(daily_paths.get("daily_exit_review") or exit_paths.get("exit_review_projection") or ""), copy_required_path=str(daily_paths.get("daily_exit_review") or "")))
                result["backfill"] = backfill
                result["daily_exit_review_paths"] = daily_paths
                result["exit_review_paths"] = exit_paths
                state_after = {"backfill_status": backfill.get("status"), "daily_exit_review": daily_paths.get("daily_exit_review"), "exit_review_projection": exit_paths.get("exit_review_projection")}
        elif command_id == "ADD_MANUAL_RECEIPT":
            body = dict(request.get("payload") or {})
            receipt_result = _write_manual_receipt_from_trade_v1(truth_root=truth_root, day_utc=day_utc, target_id=target_id, payload=body, actor=actor)
            if receipt_result.get("ok") is True:
                refreshed = receipt_result.get("refreshed_trade") if isinstance(receipt_result.get("refreshed_trade"), dict) else {}
                evidence_status = str(refreshed.get("evidence_status") or "")
                message = f"Manual receipt recorded for {body.get('symbol') or refreshed.get('symbol') or target_id}."
                result = {
                    **_empty_response(
                        command,
                        status="RECEIPT_RECORDED",
                        message=message,
                        next_state=evidence_status or "RECEIPT_RECORDED",
                        ok=True,
                        command_result=_command_result_panel_v1(
                            status_label="Receipt recorded",
                            plain_english_result=message,
                            next_required_step="Review the refreshed Performance and Exit Review projections.",
                            result_status="RECEIPT_RECORDED",
                            expected_source_path=str(receipt_result.get("receipt_path") or ""),
                            copy_required_path=str(receipt_result.get("receipt_path") or ""),
                        ),
                    ),
                    **receipt_result,
                    "updated_projection_hint": "refresh_performance_and_exit_review",
                }
                state_after = {
                    "receipt_id": receipt_result.get("receipt_id"),
                    "receipt_path": receipt_result.get("receipt_path"),
                    "evidence_status": evidence_status,
                    "exit_decision": (receipt_result.get("refreshed_exit_review") or {}).get("exit_decision") if isinstance(receipt_result.get("refreshed_exit_review"), dict) else "",
                }
            else:
                errors = receipt_result.get("errors") if isinstance(receipt_result.get("errors"), list) else []
                message = "Manual receipt could not be recorded: " + ("; ".join(str(item) for item in errors) if errors else "receipt fields are incomplete")
                result = _empty_response(
                    command,
                    status="RECEIPT_VALIDATION_FAILED",
                    message=message,
                    next_state="MISSING_RECEIPT",
                    ok=False,
                    command_result=_command_result_panel_v1(status_label="Receipt validation failed", plain_english_result=message, next_required_step="Provide symbol, side, quantity, fill price, fill time, account/source, and operator attestation.", result_status="RECEIPT_VALIDATION_FAILED", failure_reason=message),
                )
                state_after = {"receipt_state": "MISSING_RECEIPT", "errors": errors}
        elif command_id == "MARK_CAPTURE_COMPLETE":
            result = _empty_response(command, status="USE_MANUAL_CAPTURE_FORM", message="Use the manual IB capture form. The command contract is registered, but this endpoint does not synthesize capture records without the validated form payload.", next_state="UNCHANGED", ok=False)
        elif command["action_type"] in {IN_PAGE_DETAIL, SCROLL_FOCUS, EXPAND_SECTION, EXTERNAL_LINK}:
            result = _empty_response(command, status="IN_PAGE_ACTION", message=f"{command['label']} is handled by the in-page command surface.", next_state="UNCHANGED")
        else:
            result = _empty_response(command, status="HANDLER_NOT_IMPLEMENTED", message=f"Command {command_id} has no handler.", next_state="UNCHANGED", ok=False)
    except Exception as exc:
        result = _empty_response(command, status="FAILED", message=f"{command['label']} failed: {exc}", next_state="UNCHANGED", ok=False)

    audit = _audit_record(command, truth_root=truth_root, day_utc=day_utc, request_payload=request, result=result, state_before=state_before, state_after=state_after)
    command_result = dict(result.get("command_result") or {})
    command_result["audit_id"] = audit["audit_id"]
    command_result.setdefault("timestamp", audit.get("requested_at") or utc_now_v1())
    return {**result, "audit_id": audit["audit_id"], "audit_record": audit, "command_result": command_result}
