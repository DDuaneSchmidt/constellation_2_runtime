from __future__ import annotations

from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso

SCHEMA_VERSION = "event_hypothesis_proposal.v1"
ALLOWED_PROPOSAL_STATUSES = {"proposed", "accepted_for_research", "rejected", "archived", "watchlist"}
ALLOWED_GOVERNANCE = {"core_research", "experimental", "lottery", "regime_filter", "data_required"}

def recompute_hypothesis_proposal_hash(proposal: dict[str, Any]) -> str:
    return content_hash(proposal, exclude={"hypothesis_proposal_id", "created_at", "content_hash"}, sort_lists=False)

def build_hypothesis_proposal(
    *,
    intent_candidate: dict[str, Any],
    event_family: dict[str, Any],
    template: dict[str, Any],
    proposal_status: str = "proposed",
    created_by: str = "Aegis",
    created_at: str | None = None,
) -> dict[str, Any]:
    status = str(proposal_status)
    if status not in ALLOWED_PROPOSAL_STATUSES:
        raise ValueError(f"invalid proposal_status: {proposal_status}")
    governance = str(template.get("governance_classification") or "experimental")
    if governance not in ALLOWED_GOVERNANCE:
        governance = "experimental"
    data_status = str(event_family.get("default_data_requirement_status") or "unknown")
    if governance == "data_required":
        data_status = "missing_or_future"
    payload = {
        "hypothesis_proposal_id": "",
        "intent_candidate_id": str(intent_candidate["intent_candidate_id"]),
        "event_cluster_id": str(intent_candidate.get("event_cluster_id") or ""),
        "event_family_id": str(event_family["event_family_id"]),
        "title": str(template["title"]),
        "hypothesis": str(template["hypothesis"]),
        "proposed_event_definition": {
            "primary_test": str(template.get("primary_test") or ""),
            "default_event_definitions": list(event_family.get("default_event_definitions") or []),
        },
        "proposed_universe": list(event_family.get("default_symbols") or []),
        "proposed_forward_windows": list(event_family.get("default_forward_windows") or []),
        "proposed_regime_dimensions": list(event_family.get("default_regime_dimensions") or []),
        "required_data": list(intent_candidate.get("required_data") or template.get("required_data") or []),
        "data_requirement_status": data_status,
        "evidence_requirements": [
            "public, reproducible event definition before formal research",
            "validated dataset snapshot before any event study or backtest",
            "hypothetical performance labeling for any future model/backtest outputs",
            "human review before conversion to ResearchPlan",
        ],
        "governance_classification": governance,
        "proposal_status": status,
        "created_at": created_at or utc_now_iso(),
        "created_by": str(created_by or "Aegis"),
        "schema_version": SCHEMA_VERSION,
        "research_label": "RESEARCH_ONLY",
        "hypothetical_performance_label": "Any future backtest or model output is hypothetical research evidence, not achieved portfolio performance under SEC Rule 206(4)-1 framing.",
        "non_approval_assertion": {
            "approved_hypothesis": False,
            "research_plan_created": False,
            "sleeve_created": False,
            "trade_created": False,
            "capital_allocated": False,
            "automatic_promotion": False,
        },
        "content_hash": "",
    }
    fingerprint = recompute_hypothesis_proposal_hash(payload)
    payload["hypothesis_proposal_id"] = f"ehp_{short_hash(content_hash({'fingerprint': fingerprint, 'created_at': payload['created_at']}), 16)}"
    payload["content_hash"] = fingerprint
    validate_hypothesis_proposal(payload)
    return payload

def validate_hypothesis_proposal(proposal: dict[str, Any]) -> None:
    validate_contract("hypothesis_proposal", proposal)
    if proposal.get("proposal_status") not in ALLOWED_PROPOSAL_STATUSES:
        raise ValueError("invalid proposal_status")
    if proposal.get("governance_classification") not in ALLOWED_GOVERNANCE:
        raise ValueError("invalid governance_classification")
    actual = recompute_hypothesis_proposal_hash(proposal)
    if actual != proposal.get("content_hash"):
        raise ValueError(f"HypothesisProposal content_hash mismatch: expected {proposal.get('content_hash')}, got {actual}")
