from __future__ import annotations

from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso

SCHEMA_VERSION = "proposal_priority_score.v1"
BUCKETS = {"high", "medium", "low", "blocked", "watchlist"}


def _governance_score(value: str) -> int:
    return {"core_research": 3, "experimental": 2, "regime_filter": 1, "lottery": -2}.get(value, 0)


def _data_score(value: str) -> int:
    return {"available": 3, "partially_available": 1, "missing_or_future": -3, "unknown": -1}.get(value, 0)


def _confidence_score(value: str) -> int:
    return {"high": 2, "medium": 1}.get(value, 0)


def _bucket(score: int, blocking_items: list[str]) -> str:
    if blocking_items:
        return "blocked"
    if score >= 7:
        return "high"
    if score >= 4:
        return "medium"
    if score >= 1:
        return "low"
    return "watchlist"


def recompute_proposal_priority_score_hash(score: dict[str, Any]) -> str:
    return content_hash(score, exclude={"proposal_priority_score_id", "scored_at", "content_hash"}, sort_lists=False)


def build_proposal_priority_score(*, proposal: dict[str, Any], readiness: dict[str, Any], confidence_level: str = "unknown", scored_at: str | None = None) -> dict[str, Any]:
    governance_component = _governance_score(str(proposal.get("governance_classification") or ""))
    data_component = _data_score(str(readiness.get("data_requirement_status") or proposal.get("data_requirement_status") or "unknown"))
    confidence_component = _confidence_score(str(confidence_level or "unknown"))
    event_component = 2 if readiness.get("event_definition_supported") is True else 0
    cost_component = 1 if readiness.get("cost_model_available") is True else 0
    blocking_items = list(readiness.get("blocking_items") or [])
    blocking_component = -3 if blocking_items else 0
    total = governance_component + data_component + confidence_component + event_component + cost_component + blocking_component
    bucket = _bucket(total, blocking_items)
    if "missing_required_symbols" in blocking_items or "stale_intraday_market_data" in blocking_items:
        action = "refresh_intraday_market_data"
    elif "missing_macro_event_calendar" in blocking_items:
        action = "upload_external_dataset"
    elif blocking_items:
        action = "needs_revision"
    else:
        action = "ready_for_human_review"
    payload = {
        "proposal_priority_score_id": "",
        "hypothesis_proposal_id": str(proposal["hypothesis_proposal_id"]),
        "scored_at": scored_at or utc_now_iso(),
        "score": total,
        "priority_bucket": bucket,
        "score_components": {
            "governance_classification": governance_component,
            "data_requirement_status": data_component,
            "confidence_level": confidence_component,
            "event_definition_supported": event_component,
            "cost_model_available": cost_component,
            "blocking_items": blocking_component,
        },
        "reason": f"Deterministic score {total}; bucket {bucket}; blocking_items={blocking_items}.",
        "recommended_next_action": action,
        "schema_version": SCHEMA_VERSION,
        "research_label": "RESEARCH_ONLY",
        "content_hash": "",
    }
    fingerprint = recompute_proposal_priority_score_hash(payload)
    payload["proposal_priority_score_id"] = f"pps_{short_hash(content_hash({'fingerprint': fingerprint, 'scored_at': payload['scored_at']}), 16)}"
    payload["content_hash"] = fingerprint
    validate_proposal_priority_score(payload)
    return payload


def validate_proposal_priority_score(score: dict[str, Any]) -> None:
    validate_contract("proposal_priority_score", score)
    if score.get("priority_bucket") not in BUCKETS:
        raise ValueError("invalid priority_bucket")
    actual = recompute_proposal_priority_score_hash(score)
    if actual != score.get("content_hash"):
        raise ValueError(f"ProposalPriorityScore content_hash mismatch: expected {score.get('content_hash')}, got {actual}")
