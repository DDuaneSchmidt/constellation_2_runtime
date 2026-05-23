from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.event_intake.event_intake_registry import load_hypothesis_proposal, list_hypothesis_proposals
from research_lab.research_intake.intake_registry import latest_hypothesis_proposal_review, latest_priority_score, latest_readiness_assessment, proposal_context, max_confidence
from research_lab.storage.hashing import content_hash
from research_lab.storage.paths import ensure_store_layout

LANES = ["proposed", "watchlist", "needs_data", "accepted_for_research", "rejected", "archived", "needs_revision"]


def _queue_row(row: dict[str, Any], *, store_root: Path | None = None) -> dict[str, Any]:
    proposal = load_hypothesis_proposal(str(row["hypothesis_proposal_id"]), store_root=store_root)
    readiness = latest_readiness_assessment(proposal["hypothesis_proposal_id"], store_root=store_root) or {}
    priority = latest_priority_score(proposal["hypothesis_proposal_id"], store_root=store_root) or {}
    review = latest_hypothesis_proposal_review(proposal["hypothesis_proposal_id"], store_root=store_root) or {}
    context = proposal_context(proposal["hypothesis_proposal_id"], store_root=store_root)
    effective_status = str(review.get("next_status") or proposal.get("proposal_status") or "proposed")
    return {
        "hypothesis_proposal_id": proposal["hypothesis_proposal_id"],
        "event_family_id": proposal["event_family_id"],
        "title": proposal["title"],
        "proposal_status": effective_status,
        "governance_classification": proposal["governance_classification"],
        "data_requirement_status": readiness.get("data_requirement_status") or proposal.get("data_requirement_status"),
        "priority_bucket": priority.get("priority_bucket") or "watchlist",
        "ready_for_research": bool(readiness.get("ready_for_research")) if readiness else False,
        "recommended_next_action": priority.get("recommended_next_action") or ("needs_data" if readiness.get("blocking_items") else "watchlist"),
        "confidence_level": max_confidence(context["observations"]),
        "latest_review_decision": review.get("review_decision") or "",
        "next_allowed_actions": readiness.get("next_allowed_actions") or ["assess_readiness", "build_dossier", "review"],
        "research_label_present": proposal.get("research_label") == "RESEARCH_ONLY",
    }


def hypothesis_proposal_queue(*, status: str | None = None, store_root: Path | None = None, actor: str = "Aegis", audit_view: bool = False) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    rows = [_queue_row(row, store_root=store) for row in list_hypothesis_proposals(status=None, store_root=store)]
    if status:
        rows = [row for row in rows if row.get("proposal_status") == status]
    lanes = {lane: [row for row in rows if row.get("proposal_status") == lane] for lane in LANES}
    payload = {"ok": True, "read_only": True, "hypothesis_proposals": rows, "queue": rows, "lanes": lanes, "count": len(rows)}
    if audit_view:
        view_hash = content_hash({"status": status or "", "proposal_ids": [row["hypothesis_proposal_id"] for row in rows]}, sort_lists=False)
        payload["audit_event"] = write_audit_event(actor=actor, entity_type="hypothesis_proposal_queue", entity_id=status or "all", action="hypothesis_proposal_queue_viewed", previous_state_hash="", new_state_hash=view_hash, reason="Viewed read-only hypothesis proposal queue.", metadata={"status": status, "count": len(rows)}, store_root=store)
    return payload
