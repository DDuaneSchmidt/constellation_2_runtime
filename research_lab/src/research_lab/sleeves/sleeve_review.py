from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import validate_contract
from research_lab.sleeves.sleeve_challenger import load_sleeve_challenge
from research_lab.sleeves.sleeve_registry import (
    append_registry_row,
    load_sleeve_definition,
    load_sleeve_version,
    read_registry,
    sleeve_review_path,
)
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import read_json, write_json
from research_lab.storage.paths import ensure_store_layout


VALID_REVIEW_DECISIONS = {"continue", "revise", "defer", "retire", "promote_to_paper_trial_candidate"}


def validate_sleeve_review(review: dict[str, Any]) -> None:
    validate_contract("sleeve_review", review)


def _next_state(decision: str) -> str:
    return {
        "continue": "draft",
        "revise": "under_review",
        "defer": "under_review",
        "retire": "retired",
        "promote_to_paper_trial_candidate": "under_review",
    }[decision]


def build_sleeve_review(
    *,
    sleeve_id: str,
    sleeve_version_id: str,
    sleeve_challenge_id: str,
    review_decision: str,
    review_reason: str,
    reviewed_by: str,
    reviewed_at: str | None = None,
    store_root: Path | None = None,
) -> dict[str, Any]:
    decision = review_decision.strip().lower()
    if decision not in VALID_REVIEW_DECISIONS:
        raise RuntimeError(f"Invalid sleeve review decision: {review_decision}")
    if not review_reason.strip():
        raise RuntimeError("Sleeve review reason is required")
    if not reviewed_by.strip():
        raise RuntimeError("Sleeve reviewer is required")
    load_sleeve_definition(sleeve_id, store_root=store_root)
    load_sleeve_version(sleeve_id, sleeve_version_id, store_root=store_root)
    load_sleeve_challenge(sleeve_id, sleeve_challenge_id, store_root=store_root)
    payload = {
        "sleeve_id": sleeve_id,
        "sleeve_version_id": sleeve_version_id,
        "sleeve_challenge_id": sleeve_challenge_id,
        "review_decision": decision,
        "review_reason": review_reason,
        "reviewed_by": reviewed_by,
        "reviewed_at": reviewed_at or utc_now_iso(),
        "next_state": _next_state(decision),
        "schema_version": "sleeve_review.v1",
        "compliance_label": "Review is append-only governance. It does not mutate sleeve definitions, activate sleeves, or authorize trading.",
    }
    payload["content_hash"] = content_hash(payload)
    payload["sleeve_review_id"] = f"slvr_{sleeve_id}_{short_hash(payload['content_hash'], 12)}"
    payload["content_hash"] = content_hash(payload)
    validate_sleeve_review(payload)
    return payload


def store_sleeve_review(review: dict[str, Any], *, store_root: Path | None = None, actor: str = "operator") -> dict[str, Any]:
    validate_sleeve_review(review)
    store = ensure_store_layout(store_root)
    write_json(sleeve_review_path(review["sleeve_id"], review["sleeve_review_id"], store_root=store), review, overwrite=False)
    row = {
        "sleeve_review_id": review["sleeve_review_id"],
        "sleeve_id": review["sleeve_id"],
        "sleeve_version_id": review["sleeve_version_id"],
        "sleeve_challenge_id": review["sleeve_challenge_id"],
        "review_decision": review["review_decision"],
        "next_state": review["next_state"],
        "content_hash": review["content_hash"],
        "reviewed_at": review["reviewed_at"],
        "schema_version": review["schema_version"],
    }
    append_registry_row("sleeve_reviews.jsonl", row, store_root=store)
    write_audit_event(actor=actor, entity_type="sleeve_review", entity_id=review["sleeve_review_id"], action="sleeve_review_recorded", new_state_hash=review["content_hash"], reason="Recorded append-only sleeve governance review.", metadata={"registry_row": row}, store_root=store)
    return row


def load_sleeve_review(sleeve_id: str, sleeve_review_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    return read_json(sleeve_review_path(sleeve_id, sleeve_review_id, store_root=store_root))


def sleeve_summary(sleeve_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    definition = load_sleeve_definition(sleeve_id, store_root=store)
    versions = [row for row in read_registry("sleeve_versions.jsonl", store_root=store) if row["sleeve_id"] == sleeve_id]
    health = [row for row in read_registry("sleeve_health_snapshots.jsonl", store_root=store) if row["sleeve_id"] == sleeve_id]
    challenges = [row for row in read_registry("sleeve_challenges.jsonl", store_root=store) if row["sleeve_id"] == sleeve_id]
    reviews = [row for row in read_registry("sleeve_reviews.jsonl", store_root=store) if row["sleeve_id"] == sleeve_id]
    return {
        "sleeve_definition": definition,
        "versions": versions,
        "health_snapshots": health,
        "challenges": challenges,
        "reviews": reviews,
        "latest_health": health[-1] if health else None,
        "latest_challenge": challenges[-1] if challenges else None,
        "latest_review": reviews[-1] if reviews else None,
        "compliance_label": "Sleeve summary is governance-only. Linked backtests/model outputs remain hypothetical research evidence.",
        "schema_version": "sleeve_summary.v1",
    }

