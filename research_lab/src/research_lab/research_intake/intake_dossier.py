from __future__ import annotations

from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso

SCHEMA_VERSION = "research_intake_dossier.v1"
RESEARCH_INTAKE_LABEL = "Research intake only. No broker execution. No live trading. No autonomous trading. No sleeve creation. No investment recommendation."


def recompute_research_intake_dossier_hash(dossier: dict[str, Any]) -> str:
    return content_hash(dossier, exclude={"research_intake_dossier_id", "created_at", "content_hash"}, sort_lists=False)


def build_research_intake_dossier(
    *,
    proposal: dict[str, Any],
    readiness: dict[str, Any],
    priority: dict[str, Any],
    observations: list[dict[str, Any]],
    cluster: dict[str, Any],
    intent: dict[str, Any],
    latest_review_decision: str = "",
    created_at: str | None = None,
) -> dict[str, Any]:
    confidence = "unknown"
    ranks = {"unknown": 0, "low": 1, "medium": 2, "high": 3}
    for observation in observations:
        value = str(observation.get("confidence_level") or "unknown")
        if ranks.get(value, 0) > ranks.get(confidence, 0):
            confidence = value
    payload = {
        "research_intake_dossier_id": "",
        "hypothesis_proposal_id": str(proposal["hypothesis_proposal_id"]),
        "event_observation_ids": [str(row["event_observation_id"]) for row in observations],
        "event_cluster_id": str(cluster.get("event_cluster_id") or proposal.get("event_cluster_id") or ""),
        "intent_candidate_id": str(intent.get("intent_candidate_id") or proposal.get("intent_candidate_id") or ""),
        "hypothesis": str(proposal.get("hypothesis") or ""),
        "event_family_id": str(proposal.get("event_family_id") or ""),
        "confidence_level": confidence,
        "governance_classification": str(proposal.get("governance_classification") or ""),
        "data_requirement_status": str(readiness.get("data_requirement_status") or proposal.get("data_requirement_status") or "unknown"),
        "readiness_assessment_id": str(readiness["research_readiness_assessment_id"]),
        "proposal_priority_score_id": str(priority["proposal_priority_score_id"]),
        "proposal_status": str(proposal.get("proposal_status") or "proposed"),
        "recommended_next_action": str(priority.get("recommended_next_action") or "watchlist"),
        "human_review_required": True,
        "research_label": RESEARCH_INTAKE_LABEL,
        "latest_review_decision": latest_review_decision,
        "readiness_summary": {
            "ready_for_research": readiness.get("ready_for_research"),
            "blocking_items": list(readiness.get("blocking_items") or []),
            "missing_symbols": list(readiness.get("missing_symbols") or []),
        },
        "priority_summary": {"score": priority.get("score"), "priority_bucket": priority.get("priority_bucket")},
        "source_titles": {"proposal_title": proposal.get("title"), "cluster_title": cluster.get("cluster_title"), "intent_name": intent.get("intent_name")},
        "created_at": created_at or utc_now_iso(),
        "schema_version": SCHEMA_VERSION,
        "content_hash": "",
    }
    fingerprint = recompute_research_intake_dossier_hash(payload)
    payload["research_intake_dossier_id"] = f"rid_{short_hash(content_hash({'fingerprint': fingerprint, 'created_at': payload['created_at']}), 16)}"
    payload["content_hash"] = fingerprint
    validate_research_intake_dossier(payload)
    return payload


def render_research_intake_dossier_markdown(dossier: dict[str, Any], *, proposal: dict[str, Any], readiness: dict[str, Any], priority: dict[str, Any]) -> str:
    lines = [
        f"# Research Intake Dossier: {dossier['hypothesis_proposal_id']}",
        "",
        dossier["research_label"],
        "",
        f"- Hypothesis: {dossier['hypothesis']}",
        f"- Event family: {dossier['event_family_id']}",
        f"- Confidence level: {dossier['confidence_level']}",
        f"- Governance classification: {dossier['governance_classification']}",
        f"- Data requirement status: {dossier['data_requirement_status']}",
        f"- Ready for research: {readiness['ready_for_research']}",
        f"- Blocking items: {', '.join(readiness.get('blocking_items') or []) or 'none'}",
        f"- Missing symbols: {', '.join(readiness.get('missing_symbols') or []) or 'none'}",
        f"- Priority bucket: {priority['priority_bucket']}",
        f"- Recommended next action: {dossier['recommended_next_action']}",
        "",
        "No research run, sleeve, trade, allocation, or promotion is created by this dossier.",
    ]
    return "\n".join(lines) + "\n"


def validate_research_intake_dossier(dossier: dict[str, Any]) -> None:
    validate_contract("research_intake_dossier", dossier)
    if dossier.get("research_label") != RESEARCH_INTAKE_LABEL:
        raise ValueError("research intake label missing")
    actual = recompute_research_intake_dossier_hash(dossier)
    if actual != dossier.get("content_hash"):
        raise ValueError(f"ResearchIntakeDossier content_hash mismatch: expected {dossier.get('content_hash')}, got {actual}")
