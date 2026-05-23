from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import validate_contract
from research_lab.hypotheses.hypothesis_proposal import load_hypothesis_proposal
from research_lab.hypotheses.hypothesis_proposal_batch import latest_hypothesis_proposal_batch, load_hypothesis_proposal_batch
from research_lab.hypotheses.hypothesis_proposal_review import (
    auto_review_decision,
    build_hypothesis_proposal_review,
    hypothesis_proposal_review_path,
    hypothesis_proposal_review_registry_path,
    load_hypothesis_proposal_review,
)
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


SCHEMA_VERSION = "hypothesis_proposal_review_batch.v1"
REVIEW_VERSION = "hypothesis_proposal_review_gate.v1"
RESEARCH_LABEL = "RESEARCH_ONLY"


def hypothesis_proposal_review_batch_dir(store: Path) -> Path:
    return store / "hypothesis_proposal_review_batches"


def hypothesis_proposal_review_batch_path(store: Path, hypothesis_proposal_review_batch_id: str) -> Path:
    return hypothesis_proposal_review_batch_dir(store) / f"{hypothesis_proposal_review_batch_id}.json"


def hypothesis_proposal_review_batch_registry_path(store: Path) -> Path:
    return store / "registries" / "hypothesis_proposal_review_batch_registry.json"


def _counts(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    return dict(sorted(Counter(str(row.get(key) or "UNKNOWN") for row in rows).items()))


def build_hypothesis_proposal_review_batch(
    *,
    reviews: list[dict[str, Any]],
    source_hypothesis_proposal_batch_id: str,
    latest_integrity_report_id: str,
    latest_research_os_status_report_id: str,
    generated_at: str,
    review_run_id: str,
) -> dict[str, Any]:
    decision_counts = _counts(reviews, "review_decision")
    status_counts = _counts(reviews, "review_status")
    payload = {
        "hypothesis_proposal_review_batch_id": "",
        "generated_at": generated_at,
        "review_run_id": review_run_id,
        "review_version": REVIEW_VERSION,
        "source_hypothesis_proposal_batch_id": source_hypothesis_proposal_batch_id,
        "hypothesis_proposal_review_ids": [row["hypothesis_proposal_review_id"] for row in reviews],
        "review_count": len(reviews),
        "review_decision_counts": decision_counts,
        "review_status_counts": status_counts,
        "latest_integrity_report_id": latest_integrity_report_id,
        "latest_research_os_status_report_id": latest_research_os_status_report_id,
        "non_actionable_research_only_assertion": {
            "research_only": True,
            "proposal_review_only": True,
            "active_hypothesis_created": False,
            "market_action_authorized": False,
            "buy_sell_hold_recommendations_allowed": False,
        },
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
        "schema_version": SCHEMA_VERSION,
        "research_label": RESEARCH_LABEL,
        "immutable_hash": "",
        "content_hash": "",
    }
    fingerprint_payload = dict(payload)
    fingerprint_payload["hypothesis_proposal_review_ids"] = [row["immutable_hash"] for row in reviews]
    fingerprint = content_hash(fingerprint_payload, exclude={"hypothesis_proposal_review_batch_id", "generated_at", "review_run_id", "immutable_hash", "content_hash"}, sort_lists=True)
    payload["hypothesis_proposal_review_batch_id"] = f"hprb_{short_hash(content_hash({'fingerprint': fingerprint, 'generated_at': generated_at}), 16)}"
    payload["immutable_hash"] = fingerprint
    payload["content_hash"] = fingerprint
    validate_contract("hypothesis_proposal_review_batch", payload)
    return payload


def _load_source_batch(store: Path, source_batch_id: str | None) -> dict[str, Any]:
    if source_batch_id:
        return load_hypothesis_proposal_batch(source_batch_id, store_root=store)
    batch = latest_hypothesis_proposal_batch(store_root=store)
    if not batch:
        raise FileNotFoundError("hypothesis_proposal_batch_missing")
    return batch


def build_reviews_for_batch(
    *,
    store_root: Path | None = None,
    source_batch_id: str | None = None,
    generated_at: str,
    reviewer_id: str,
    auto_review_safe: bool = True,
    decision: str | None = None,
    rationale: str | None = None,
    proposal_id: str | None = None,
    allow_system_research_activation_review: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    store = ensure_store_layout(store_root)
    source_batch = _load_source_batch(store, source_batch_id)
    proposal_ids = [proposal_id] if proposal_id else list(source_batch.get("hypothesis_proposal_ids") or [])
    reviews: list[dict[str, Any]] = []
    for item_id in proposal_ids:
        proposal = load_hypothesis_proposal(str(item_id), store_root=store)
        enriched = dict(proposal)
        enriched["source_hypothesis_proposal_batch_id"] = source_batch.get("hypothesis_proposal_batch_id")
        if auto_review_safe and not decision:
            selected_decision, selected_rationale = auto_review_decision(enriched)
        else:
            if not decision:
                raise ValueError("manual_decision_required")
            selected_decision = decision
            selected_rationale = rationale or ""
        reviews.append(
            build_hypothesis_proposal_review(
                proposal=enriched,
                generated_at=generated_at,
                reviewer_id=reviewer_id,
                review_decision=selected_decision,
                review_rationale=selected_rationale,
                allow_system_research_activation_review=allow_system_research_activation_review,
            )
        )
    return reviews, source_batch


def write_hypothesis_proposal_review_batch(
    *,
    store_root: Path | None = None,
    source_batch_id: str | None = None,
    generated_at: str | None = None,
    reviewer_id: str = "system_packet28",
    auto_review_safe: bool = True,
    decision: str | None = None,
    rationale: str | None = None,
    proposal_id: str | None = None,
    allow_system_research_activation_review: bool = False,
    actor: str = "Aegis",
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    generated_at = generated_at or utc_now_iso()
    reviews, source_batch = build_reviews_for_batch(
        store_root=store,
        source_batch_id=source_batch_id,
        generated_at=generated_at,
        reviewer_id=reviewer_id,
        auto_review_safe=auto_review_safe,
        decision=decision,
        rationale=rationale,
        proposal_id=proposal_id,
        allow_system_research_activation_review=allow_system_research_activation_review,
    )
    review_results = []
    for review in reviews:
        path = hypothesis_proposal_review_path(store, review["hypothesis_proposal_review_id"])
        if path.exists():
            raise FileExistsError(f"Refusing to overwrite immutable hypothesis proposal review: {path}")
        write_json(path, review, overwrite=False)
        row = {
            "hypothesis_proposal_review_id": review["hypothesis_proposal_review_id"],
            "generated_at": review["generated_at"],
            "hypothesis_proposal_id": review["hypothesis_proposal_id"],
            "hypothesis_proposal_batch_id": review["hypothesis_proposal_batch_id"],
            "review_decision": review["review_decision"],
            "review_status": review["review_status"],
            "reviewer_id": review["reviewer_id"],
            "latest_integrity_report_id": review["latest_integrity_report_id"],
            "latest_research_os_status_report_id": review["latest_research_os_status_report_id"],
            "immutable_hash": review["immutable_hash"],
        }
        append_jsonl(hypothesis_proposal_review_registry_path(store), row)
        audit = write_audit_event(
            actor=actor,
            entity_type="hypothesis_proposal_review",
            entity_id=review["hypothesis_proposal_review_id"],
            action="hypothesis_proposal_review_recorded",
            new_state_hash=review["immutable_hash"],
            reason="Recorded read-only review decision for hypothesis proposal.",
            metadata={
                "hypothesis_proposal_review_id": review["hypothesis_proposal_review_id"],
                "hypothesis_proposal_review_batch_id": "",
                "hypothesis_proposal_id": review["hypothesis_proposal_id"],
                "review_decision": review["review_decision"],
                "review_status": review["review_status"],
                "reviewer_id": review["reviewer_id"],
                "latest_integrity_report_id": review["latest_integrity_report_id"],
                "latest_research_os_status_report_id": review["latest_research_os_status_report_id"],
                "immutable_hash": review["immutable_hash"],
            },
            store_root=store,
        )
        review_results.append({"review": review, "registry_row": row, "audit_event": audit, "json_path": str(path)})
    run_id = f"hprun_{short_hash(content_hash({'generated_at': generated_at, 'source_batch_id': source_batch.get('hypothesis_proposal_batch_id')}), 16)}"
    batch = build_hypothesis_proposal_review_batch(
        reviews=reviews,
        source_hypothesis_proposal_batch_id=str(source_batch.get("hypothesis_proposal_batch_id") or ""),
        latest_integrity_report_id=str(source_batch.get("latest_integrity_report_id") or ""),
        latest_research_os_status_report_id=str(source_batch.get("latest_research_os_status_report_id") or ""),
        generated_at=generated_at,
        review_run_id=run_id,
    )
    path = hypothesis_proposal_review_batch_path(store, batch["hypothesis_proposal_review_batch_id"])
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite immutable hypothesis proposal review batch: {path}")
    write_json(path, batch, overwrite=False)
    row = {
        "hypothesis_proposal_review_batch_id": batch["hypothesis_proposal_review_batch_id"],
        "generated_at": batch["generated_at"],
        "review_run_id": batch["review_run_id"],
        "review_version": batch["review_version"],
        "source_hypothesis_proposal_batch_id": batch["source_hypothesis_proposal_batch_id"],
        "review_count": batch["review_count"],
        "immutable_hash": batch["immutable_hash"],
    }
    append_jsonl(hypothesis_proposal_review_batch_registry_path(store), row)
    audit = write_audit_event(
        actor=actor,
        entity_type="hypothesis_proposal_review_batch",
        entity_id=batch["hypothesis_proposal_review_batch_id"],
        action="hypothesis_proposal_review_batch_generated",
        new_state_hash=batch["immutable_hash"],
        reason="Generated read-only hypothesis proposal review batch.",
        metadata={
            "hypothesis_proposal_review_batch_id": batch["hypothesis_proposal_review_batch_id"],
            "source_hypothesis_proposal_batch_id": batch["source_hypothesis_proposal_batch_id"],
            "review_count": batch["review_count"],
            "review_decision_counts": batch["review_decision_counts"],
            "review_status_counts": batch["review_status_counts"],
            "latest_integrity_report_id": batch["latest_integrity_report_id"],
            "latest_research_os_status_report_id": batch["latest_research_os_status_report_id"],
            "immutable_hash": batch["immutable_hash"],
        },
        store_root=store,
    )
    return {"batch": batch, "reviews": review_results, "registry_row": row, "audit_event": audit, "json_path": str(path)}


def load_hypothesis_proposal_review_batch(hypothesis_proposal_review_batch_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    return read_json(hypothesis_proposal_review_batch_path(store, hypothesis_proposal_review_batch_id))


def list_hypothesis_proposal_review_batches(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    return read_jsonl(hypothesis_proposal_review_batch_registry_path(store))


def latest_hypothesis_proposal_review_batch(*, store_root: Path | None = None) -> dict[str, Any] | None:
    rows = list_hypothesis_proposal_review_batches(store_root=store_root)
    if not rows:
        return None
    return load_hypothesis_proposal_review_batch(str(rows[-1]["hypothesis_proposal_review_batch_id"]), store_root=store_root)


def latest_hypothesis_proposal_reviews_for_batch(*, store_root: Path | None = None, limit: int = 20) -> list[dict[str, Any]]:
    batch = latest_hypothesis_proposal_review_batch(store_root=store_root)
    if not batch:
        return []
    return [load_hypothesis_proposal_review(str(review_id), store_root=store_root) for review_id in batch.get("hypothesis_proposal_review_ids", [])[:limit]]

