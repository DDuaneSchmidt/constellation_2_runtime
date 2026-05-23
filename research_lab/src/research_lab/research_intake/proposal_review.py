from __future__ import annotations

from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso

SCHEMA_VERSION = "research_intake_hypothesis_proposal_review.v1"
DECISION_TO_STATUS = {
    "accept_for_research": "accepted_for_research",
    "watchlist": "watchlist",
    "reject": "rejected",
    "archive": "archived",
    "needs_data": "needs_data",
    "needs_revision": "needs_revision",
}


def recompute_hypothesis_proposal_review_hash(review: dict[str, Any]) -> str:
    return content_hash(review, exclude={"hypothesis_proposal_review_id", "reviewed_at", "content_hash"}, sort_lists=False)


def build_hypothesis_proposal_review(*, proposal: dict[str, Any], readiness: dict[str, Any], review_decision: str, review_reason: str, reviewed_by: str, reviewed_at: str | None = None) -> dict[str, Any]:
    if review_decision not in DECISION_TO_STATUS:
        raise RuntimeError(f"review decision invalid: {review_decision}")
    ready = bool(readiness.get("ready_for_research"))
    if review_decision == "accept_for_research" and not ready:
        raise RuntimeError("accept_for_research attempted while ready_for_research=false")
    next_status = DECISION_TO_STATUS[review_decision]
    allowed = ["convert_to_research_plan"] if next_status == "accepted_for_research" else (["enable_required_data", "reassess_readiness"] if next_status == "needs_data" else ["watchlist", "reassess_readiness"])
    payload = {
        "hypothesis_proposal_review_id": "",
        "hypothesis_proposal_id": str(proposal["hypothesis_proposal_id"]),
        "review_decision": review_decision,
        "review_reason": str(review_reason or ""),
        "reviewed_by": str(reviewed_by or "operator"),
        "reviewed_at": reviewed_at or utc_now_iso(),
        "next_status": next_status,
        "allowed_next_actions": allowed,
        "readiness_assessment_id": readiness.get("research_readiness_assessment_id", ""),
        "source_proposal_hash": proposal.get("content_hash", ""),
        "schema_version": SCHEMA_VERSION,
        "research_label": "RESEARCH_ONLY",
        "content_hash": "",
    }
    if not payload["review_reason"].strip():
        raise RuntimeError("review reason required")
    fingerprint = recompute_hypothesis_proposal_review_hash(payload)
    payload["hypothesis_proposal_review_id"] = f"hprev_{short_hash(content_hash({'fingerprint': fingerprint, 'reviewed_at': payload['reviewed_at']}), 16)}"
    payload["content_hash"] = fingerprint
    validate_hypothesis_proposal_review(payload)
    return payload


def validate_hypothesis_proposal_review(review: dict[str, Any]) -> None:
    validate_contract("hypothesis_proposal_review", review)
    if review.get("review_decision") not in DECISION_TO_STATUS:
        raise ValueError("invalid review_decision")
    if review.get("next_status") not in set(DECISION_TO_STATUS.values()):
        raise ValueError("invalid next_status")
    actual = recompute_hypothesis_proposal_review_hash(review)
    if actual != review.get("content_hash"):
        raise ValueError(f"HypothesisProposalReview content_hash mismatch: expected {review.get('content_hash')}, got {actual}")
