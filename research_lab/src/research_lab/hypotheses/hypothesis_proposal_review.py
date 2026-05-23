from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.hypotheses.hypothesis_proposal import load_hypothesis_proposal
from research_lab.storage.hashing import content_hash, short_hash
from research_lab.storage.manifest_io import read_json, read_jsonl
from research_lab.storage.paths import ensure_store_layout


SCHEMA_VERSION = "hypothesis_proposal_review.v1"
REVIEW_VERSION = "hypothesis_proposal_review_gate.v1"
RESEARCH_LABEL = "RESEARCH_ONLY"
ACTIONABILITY_STATUS = "non_actionable_research_only"

ALLOWED_DECISIONS = {
    "reject_proposal",
    "defer_until_more_observation",
    "mark_system_research_item",
    "request_data_enablement",
    "approve_for_future_hypothesis_activation",
}

DISALLOWED_ACTIONS = [
    "create_active_hypothesis_now",
    "create_challenger_now",
    "create_sleeve_now",
    "create_paper_trial_now",
    "trade_or_allocate_capital",
]


def hypothesis_proposal_review_dir(store: Path) -> Path:
    return store / "hypothesis_proposal_reviews"


def hypothesis_proposal_review_path(store: Path, hypothesis_proposal_review_id: str) -> Path:
    return hypothesis_proposal_review_dir(store) / f"{hypothesis_proposal_review_id}.json"


def hypothesis_proposal_review_registry_path(store: Path) -> Path:
    return store / "registries" / "hypothesis_proposal_review_registry.json"


def allowed_decisions_for_proposal(proposal: dict[str, Any], *, allow_system_research_activation_review: bool = False) -> set[str]:
    status = str(proposal.get("proposal_status") or "")
    family = str(proposal.get("proposal_family") or "")
    if status == "blocked":
        return {"reject_proposal", "request_data_enablement", "defer_until_more_observation"}
    if status == "needs_more_observation":
        return {"defer_until_more_observation", "reject_proposal"}
    if status == "system_research_only":
        allowed = {"mark_system_research_item", "reject_proposal", "defer_until_more_observation"}
        if allow_system_research_activation_review and family not in {"research_store_integrity", "evidence_quality"}:
            allowed.add("approve_for_future_hypothesis_activation")
        return allowed
    if status == "proposed_for_review":
        return {"approve_for_future_hypothesis_activation", "reject_proposal", "defer_until_more_observation"}
    return {"reject_proposal", "defer_until_more_observation"}


def auto_review_decision(proposal: dict[str, Any]) -> tuple[str, str]:
    status = str(proposal.get("proposal_status") or "")
    family = str(proposal.get("proposal_family") or "")
    blocker_codes = {str(row.get("blocker_code") or "") for row in proposal.get("blockers") or []}
    if status == "blocked" and family == "market_behavior" and "market_data_unavailable" in blocker_codes:
        return ("request_data_enablement", "Market behavior proposal is blocked until validated market data is available.")
    if status == "needs_more_observation":
        return ("defer_until_more_observation", "Proposal requires more observations before any future activation review.")
    if status == "system_research_only":
        return ("mark_system_research_item", "Proposal is an internal research-system item and is not an active hypothesis.")
    if status == "proposed_for_review":
        return ("defer_until_more_observation", "Default safe review defers proposed items unless a human explicitly records activation eligibility.")
    return ("reject_proposal", "Proposal status is not eligible for safe automatic review.")


def _next_actions(decision: str, proposal: dict[str, Any], review_status: str) -> list[str]:
    if review_status != "recorded":
        return ["collect_more_observations"]
    if decision == "request_data_enablement":
        return ["enable_market_data"]
    if decision == "defer_until_more_observation":
        return ["collect_more_observations"]
    if decision == "mark_system_research_item":
        if proposal.get("proposal_family") == "research_store_integrity":
            return ["resolve_integrity_blocker", "run_system_research_review"]
        return ["run_system_research_review"]
    if decision == "approve_for_future_hypothesis_activation":
        return ["eligible_for_hypothesis_activation_packet"]
    return ["collect_more_observations"]


def build_hypothesis_proposal_review(
    *,
    proposal: dict[str, Any],
    generated_at: str,
    reviewer_id: str,
    review_decision: str,
    review_rationale: str,
    allow_system_research_activation_review: bool = False,
    expected_source_proposal_hash: str | None = None,
) -> dict[str, Any]:
    if review_decision not in ALLOWED_DECISIONS:
        raise ValueError(f"unsupported_review_decision:{review_decision}")
    source_hash = str(proposal.get("immutable_hash") or proposal.get("content_hash") or content_hash(proposal, sort_lists=True))
    if expected_source_proposal_hash and expected_source_proposal_hash != source_hash:
        raise ValueError("source_proposal_hash_mismatch")
    allowed = allowed_decisions_for_proposal(proposal, allow_system_research_activation_review=allow_system_research_activation_review)
    blockers = list(proposal.get("blockers") or [])
    review_status = "recorded"
    if review_decision not in allowed:
        review_status = "blocked"
        blockers.append(
            {
                "blocker_code": "review_decision_not_allowed",
                "blocker_message": f"{review_decision} is not allowed for source proposal status {proposal.get('proposal_status')} and family {proposal.get('proposal_family')}.",
                "severity": "ERROR",
                "recoverable": True,
                "source_artifact_id": proposal.get("hypothesis_proposal_id"),
            }
        )
    if not str(review_rationale or "").strip():
        review_status = "invalid"
        blockers.append(
            {
                "blocker_code": "review_rationale_required",
                "blocker_message": "Review rationale is required.",
                "severity": "ERROR",
                "recoverable": True,
                "source_artifact_id": proposal.get("hypothesis_proposal_id"),
            }
        )
    eligibility_snapshot = {
        "source_proposal_status": proposal.get("proposal_status"),
        "source_proposal_family": proposal.get("proposal_family"),
        "allowed_decisions": sorted(allowed),
        "decision_allowed": review_decision in allowed,
        "allow_system_research_activation_review": allow_system_research_activation_review,
    }
    source_artifact_ids = {
        "hypothesis_proposal_id": proposal.get("hypothesis_proposal_id"),
        "hypothesis_proposal_batch_id": proposal.get("source_artifact_ids", {}).get("hypothesis_proposal_batch_id") or proposal.get("source_hypothesis_proposal_batch_id"),
        "source_observation_cluster_id": proposal.get("source_observation_cluster_id"),
        "source_observation_candidate_ids": proposal.get("source_observation_candidate_ids") or [],
        "latest_integrity_report_id": proposal.get("latest_integrity_report_id"),
        "latest_research_os_status_report_id": proposal.get("latest_research_os_status_report_id"),
    }
    payload = {
        "hypothesis_proposal_review_id": "",
        "generated_at": generated_at,
        "review_version": REVIEW_VERSION,
        "hypothesis_proposal_id": str(proposal.get("hypothesis_proposal_id") or ""),
        "hypothesis_proposal_batch_id": str(proposal.get("source_artifact_ids", {}).get("hypothesis_proposal_batch_id") or proposal.get("source_hypothesis_proposal_batch_id") or ""),
        "reviewer_id": reviewer_id,
        "review_decision": review_decision,
        "review_rationale": review_rationale,
        "review_status": review_status,
        "source_proposal_status": str(proposal.get("proposal_status") or ""),
        "source_proposal_family": str(proposal.get("proposal_family") or ""),
        "source_proposal_hash": source_hash,
        "source_observation_cluster_id": str(proposal.get("source_observation_cluster_id") or ""),
        "source_observation_candidate_ids": list(proposal.get("source_observation_candidate_ids") or []),
        "latest_integrity_report_id": str(proposal.get("latest_integrity_report_id") or ""),
        "latest_research_os_status_report_id": str(proposal.get("latest_research_os_status_report_id") or ""),
        "eligibility_snapshot": eligibility_snapshot,
        "blockers": blockers,
        "allowed_next_research_actions": _next_actions(review_decision, proposal, review_status),
        "disallowed_actions": DISALLOWED_ACTIONS,
        "no_lifecycle_mutation_assertion": {
            "active_hypothesis_created": False,
            "challenger_created": False,
            "sleeve_created": False,
            "paper_trial_created": False,
            "paper_trial_proposal_created": False,
            "candidate_ledger_mutated": False,
            "evidence_mutated": False,
            "integrity_repaired": False,
            "trade_order_created": False,
            "capital_allocation_created": False,
        },
        "non_actionable_research_only_assertion": {
            "research_only": True,
            "proposal_review_only": True,
            "active_hypothesis_created": False,
            "market_action_authorized": False,
            "buy_sell_hold_recommendations_allowed": False,
        },
        "actionability_status": ACTIONABILITY_STATUS,
        "source_artifact_ids": source_artifact_ids,
        "source_artifact_hashes": {
            "source_proposal_hash": source_hash,
            **(proposal.get("source_artifact_hashes") or {}),
        },
        "schema_version": SCHEMA_VERSION,
        "research_label": RESEARCH_LABEL,
        "immutable_hash": "",
        "content_hash": "",
    }
    fingerprint = content_hash(payload, exclude={"hypothesis_proposal_review_id", "generated_at", "immutable_hash", "content_hash"}, sort_lists=True)
    payload["hypothesis_proposal_review_id"] = f"hprev_{short_hash(content_hash({'fingerprint': fingerprint, 'generated_at': generated_at}), 16)}"
    payload["immutable_hash"] = fingerprint
    payload["content_hash"] = fingerprint
    validate_contract("hypothesis_proposal_review", payload)
    return payload


def load_hypothesis_proposal_review(hypothesis_proposal_review_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    return read_json(hypothesis_proposal_review_path(store, hypothesis_proposal_review_id))


def list_hypothesis_proposal_reviews(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    return read_jsonl(hypothesis_proposal_review_registry_path(store))


def latest_hypothesis_proposal_review(*, store_root: Path | None = None) -> dict[str, Any] | None:
    rows = list_hypothesis_proposal_reviews(store_root=store_root)
    if not rows:
        return None
    return load_hypothesis_proposal_review(str(rows[-1]["hypothesis_proposal_review_id"]), store_root=store_root)


def hypothesis_proposal_reviews_for_proposal(hypothesis_proposal_id: str, *, store_root: Path | None = None) -> list[dict[str, Any]]:
    rows = [row for row in list_hypothesis_proposal_reviews(store_root=store_root) if row.get("hypothesis_proposal_id") == hypothesis_proposal_id]
    return [load_hypothesis_proposal_review(str(row["hypothesis_proposal_review_id"]), store_root=store_root) for row in rows]


def build_review_for_proposal_id(
    *,
    hypothesis_proposal_id: str,
    store_root: Path | None = None,
    generated_at: str,
    reviewer_id: str,
    review_decision: str,
    review_rationale: str,
    allow_system_research_activation_review: bool = False,
    expected_source_proposal_hash: str | None = None,
) -> dict[str, Any]:
    proposal = load_hypothesis_proposal(hypothesis_proposal_id, store_root=store_root)
    return build_hypothesis_proposal_review(
        proposal=proposal,
        generated_at=generated_at,
        reviewer_id=reviewer_id,
        review_decision=review_decision,
        review_rationale=review_rationale,
        allow_system_research_activation_review=allow_system_research_activation_review,
        expected_source_proposal_hash=expected_source_proposal_hash,
    )

