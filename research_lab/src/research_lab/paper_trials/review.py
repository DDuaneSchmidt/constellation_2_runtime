from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import validate_contract
from research_lab.paper_trials.paper_trial_registry import build_paper_trial_summary, paper_trial_dir, write_paper_trial_summary
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


VALID_REVIEW_DECISIONS = {"continue", "pause", "complete", "cancel", "promote_to_sleeve_review_candidate"}


def validate_paper_trial_review(review: dict[str, Any]) -> None:
    validate_contract("paper_trial_review", review)


def _next_state(decision: str) -> str:
    return {
        "continue": "active",
        "pause": "paused",
        "complete": "completed",
        "cancel": "cancelled",
        "promote_to_sleeve_review_candidate": "active",
    }[decision]


def record_paper_trial_review(
    *,
    paper_trial_id: str,
    review_decision: str,
    review_reason: str,
    reviewed_by: str,
    store_root: Path | None = None,
) -> dict[str, Any]:
    decision = review_decision.strip().lower()
    if decision not in VALID_REVIEW_DECISIONS:
        raise RuntimeError(f"Invalid paper trial review decision: {review_decision}")
    if not review_reason.strip():
        raise RuntimeError("Paper trial review reason is required")
    store = ensure_store_layout(store_root)
    summary = build_paper_trial_summary(paper_trial_id, store_root=store)
    payload = {
        "paper_trial_id": paper_trial_id,
        "review_date": utc_now_iso()[:10],
        "review_decision": decision,
        "review_reason": review_reason,
        "reviewed_by": reviewed_by,
        "metrics_summary": {
            "observation_count": summary["observation_count"],
            "candidate_count_total": summary["candidate_count_total"],
            "measured_outcome_count": summary["measured_outcome_count"],
            "zero_candidate_observation_count": summary["zero_candidate_observation_count"],
        },
        "next_state": _next_state(decision),
        "created_at": utc_now_iso(),
        "schema_version": "paper_trial_review.v1",
        "research_label": "Paper trial review is append-only observation. It does not mutate sleeves or authorize trading.",
    }
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    payload["paper_trial_review_id"] = f"ptrr_{paper_trial_id}_{short_hash(payload['content_hash'], 10)}"
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    validate_paper_trial_review(payload)
    write_json(paper_trial_dir(paper_trial_id, store_root=store) / "reviews" / f"{payload['paper_trial_review_id']}.json", payload, overwrite=False)
    row = {
        "paper_trial_review_id": payload["paper_trial_review_id"],
        "paper_trial_id": paper_trial_id,
        "review_decision": decision,
        "next_state": payload["next_state"],
        "content_hash": payload["content_hash"],
        "created_at": payload["created_at"],
        "schema_version": payload["schema_version"],
    }
    append_jsonl(store / "registries" / "paper_trial_reviews.jsonl", row)
    write_audit_event(actor=reviewed_by, entity_type="paper_trial_review", entity_id=payload["paper_trial_review_id"], action="paper_trial_review_recorded", new_state_hash=payload["content_hash"], reason="Recorded append-only paper trial review.", metadata={"registry_row": row}, store_root=store)
    write_paper_trial_summary(paper_trial_id, store_root=store)
    return payload

