from __future__ import annotations

import json
from typing import Any

from .scheduler_events import is_supported_trigger

FORBIDDEN_AUTHORITY_MARKERS = [
    "LiveTrade",
    "TradeRecommendation",
    "CapitalAllocation",
    "SleeveDeployment",
    "ProductionCandidatePromotion",
    "PortfolioRecommendation",
    "PositionSizing",
    "broker_submit",
    "broker_transmit",
    "allocate_capital",
    "promote_candidate",
    "deploy_sleeve",
    "construct_portfolio",
    "size_position",
]

ALLOWED_EXECUTION_FUNCTION = "run_bounded_research_once"


def validate_scheduler_trigger_allowed(trigger: dict[str, Any]) -> dict[str, Any]:
    details: list[str] = []
    trigger_type = str(trigger.get("trigger_type") or "")
    if not is_supported_trigger(trigger_type):
        details.append(f"unsupported trigger type: {trigger_type}")
    if trigger.get("source", {}).get("trading_authority") is True:
        details.append("trigger source requested trading authority")
    return gate("scheduler_trigger_allowed", details)


def validate_scheduler_execution_allowed(payload: dict[str, Any]) -> dict[str, Any]:
    details: list[str] = []
    if payload.get("execution_function") != ALLOWED_EXECUTION_FUNCTION:
        details.append("scheduler may only invoke run_bounded_research_once")
    if payload.get("continuous_loop") is True or payload.get("daemon") is True:
        details.append("scheduler execution must be a single trigger handling pass")
    return gate("scheduler_execution_allowed", details)


def validate_scheduler_no_authority_escalation(payload: dict[str, Any]) -> dict[str, Any]:
    text = json.dumps(payload, sort_keys=True)
    details = [f"forbidden authority marker present: {marker}" for marker in FORBIDDEN_AUTHORITY_MARKERS if marker in text]
    return gate("scheduler_no_authority_escalation", details)


def validate_scheduler_result_governed(result: dict[str, Any]) -> dict[str, Any]:
    details: list[str] = []
    if result.get("status") == "EXECUTION_COMPLETED":
        execution = result.get("execution_result", {}) or {}
        if execution.get("governance_result", {}).get("status") == "FAIL":
            details.append("bounded execution governance failed")
        if execution.get("lineage_result", {}).get("status") == "FAIL":
            details.append("bounded execution lineage failed")
        if execution.get("certification_result", {}).get("status") == "FAIL":
            details.append("bounded execution certification failed")
    return gate("scheduler_result_governed", details)


def validate_scheduler_governance(trigger: dict[str, Any], execution_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    gates = [
        validate_scheduler_trigger_allowed(trigger),
        validate_scheduler_execution_allowed(execution_payload or {"execution_function": ALLOWED_EXECUTION_FUNCTION}),
        validate_scheduler_no_authority_escalation({"trigger": trigger, "execution": execution_payload or {}}),
    ]
    details = [detail for item in gates if item["result"] != "PASS" for detail in item.get("details", [])]
    return {"status": "PASS" if not details else "FAIL", "gates": gates, "details": details}


def gate(gate_id: str, details: list[str]) -> dict[str, Any]:
    return {"gate_id": gate_id, "result": "PASS" if not details else "FAIL", "details": details}
