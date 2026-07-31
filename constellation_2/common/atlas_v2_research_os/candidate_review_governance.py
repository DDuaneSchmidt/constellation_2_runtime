from __future__ import annotations

from typing import Any

from .candidate_review_models import CANDIDATE_REVIEW_ALLOWED_SCOPE

FORBIDDEN_AUTHORITY_FIELDS = {
    "trade_recommendation_authorized",
    "capital_allocation_authorized",
    "position_sizing_authorized",
    "live_trading_authorized",
    "broker_execution_authorized",
    "trading_authorized",
    "capital_authorized",
    "broker_submit_authorized",
    "broker_transmit_authorized",
    "paper_trade_placement_authorized",
    "automatic_paper_trade_placement_authorized",
}
FORBIDDEN_ACTION_TERMS = [
    "buy ",
    "sell ",
    "short ",
    "go long",
    "go short",
    "allocate capital",
    "position size",
    "sizing recommendation",
    "send broker",
    "submit order",
    "execute trade",
    "live trading",
    "trade recommendation",
]
ACTION_TEXT_FIELDS = {
    "human_action_required",
    "recommended_next_human_reviews",
    "allowed_next_steps",
    "next_steps",
    "recommendation",
}


class CandidateReviewGovernanceError(ValueError):
    pass


def validate_candidate_review_allowed(payload: dict[str, Any]) -> bool:
    validate_candidate_review_authority_boundary(payload)
    validate_no_candidate_review_authority_escalation(payload)
    validate_candidate_review_human_actions(payload)
    return True


def validate_candidate_review_authority_boundary(payload: dict[str, Any]) -> bool:
    boundary = payload.get("authority_boundary", {}) if isinstance(payload.get("authority_boundary"), dict) else {}
    if boundary.get("allowed_scope") != CANDIDATE_REVIEW_ALLOWED_SCOPE:
        raise CandidateReviewGovernanceError("candidate review must be limited to paper-forward observation")
    required_false = {
        "trade_recommendation_authorized",
        "capital_allocation_authorized",
        "position_sizing_authorized",
        "live_trading_authorized",
        "broker_execution_authorized",
    }
    for field in required_false:
        if boundary.get(field) is not False:
            raise CandidateReviewGovernanceError(f"candidate review authority boundary must set {field}=False")
    return True


def validate_no_candidate_review_authority_escalation(payload: Any) -> bool:
    for path, value in _walk(payload):
        key = path[-1] if path else ""
        if key in FORBIDDEN_AUTHORITY_FIELDS and value is True:
            raise CandidateReviewGovernanceError(f"candidate review forbids authority flag: {'.'.join(path)}")
    return True


def validate_candidate_review_human_actions(payload: Any) -> bool:
    for path, value in _walk(payload):
        key = path[-1] if path else ""
        if key not in ACTION_TEXT_FIELDS:
            continue
        values = value if isinstance(value, list) else [value]
        for item in values:
            lowered = str(item).lower()
            if any(term in lowered for term in FORBIDDEN_ACTION_TERMS):
                raise CandidateReviewGovernanceError(f"candidate review action text crosses authority boundary: {'.'.join(path)}")
    return True


def _walk(value: Any, path: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], Any]]:
    rows: list[tuple[tuple[str, ...], Any]] = [(path, value)]
    if isinstance(value, dict):
        for key, child in value.items():
            rows.extend(_walk(child, (*path, str(key))))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            rows.extend(_walk(child, (*path, str(index))))
    return rows
