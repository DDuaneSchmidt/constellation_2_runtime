from __future__ import annotations

import json
from typing import Any


ALLOWED_RECOMMENDATIONS = {
    "Review failed worker run.",
    "Inspect blocked backlog item.",
    "Check repeated lineage failure.",
    "Review governance block.",
    "Rerun after fixing missing dependency.",
}

FORBIDDEN_RECOMMENDATION_FRAGMENTS = {
    "trade this",
    "promote candidate",
    "ignore governance block",
    "bypass certification",
    "allocate capital",
}

FORBIDDEN_AUTHORITY_FRAGMENTS = {
    "approve failed runs",
    "override governance",
    "override certification",
    "promote candidates",
    "authorize paper trading",
    "authorize live trading",
    "authorize capital use",
    "broker_submit",
    "broker_transmit",
    "live_trading",
    "capital_allocation",
    "position_sizing",
    "candidate_promotion",
}


def validate_failure_observatory_allowed(payload: Any | None = None) -> dict[str, Any]:
    escalation = validate_failure_observatory_no_authority_escalation(payload or {})
    if escalation["status"] != "PASS":
        return escalation
    return {
        "status": "PASS",
        "allowed_actions": ["record failures", "summarize failures", "recommend human review"],
        "forbidden_actions": [
            "approve failed runs",
            "override governance",
            "override certification",
            "promote candidates",
            "authorize paper trading",
            "authorize live trading",
            "authorize capital use",
        ],
    }


def validate_failure_observatory_no_authority_escalation(payload: Any) -> dict[str, Any]:
    text = _flatten(payload).lower()
    failures = [fragment for fragment in sorted(FORBIDDEN_AUTHORITY_FRAGMENTS) if fragment in text]
    return {"status": "FAIL" if failures else "PASS", "failures": failures}


def validate_failure_observatory_no_forbidden_recommendations(recommendations: list[str]) -> dict[str, Any]:
    failures: list[str] = []
    for recommendation in recommendations:
        lowered = recommendation.lower()
        if recommendation not in ALLOWED_RECOMMENDATIONS:
            failures.append(f"recommendation not allowlisted: {recommendation}")
        for fragment in FORBIDDEN_RECOMMENDATION_FRAGMENTS:
            if fragment in lowered:
                failures.append(f"forbidden recommendation fragment: {fragment}")
    return {"status": "FAIL" if failures else "PASS", "failures": failures}


def _flatten(payload: Any) -> str:
    if isinstance(payload, str):
        return payload
    try:
        return json.dumps(payload, sort_keys=True)
    except TypeError:
        return str(payload)
