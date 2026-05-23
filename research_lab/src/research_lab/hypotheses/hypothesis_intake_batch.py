from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import validate_contract
from research_lab.hypotheses.hypothesis_intake import (
    build_hypothesis_intake_for_review_id,
    hypothesis_intake_decision_path,
    hypothesis_intake_decision_registry_path,
    load_hypothesis_intake_decision,
)
from research_lab.hypotheses.hypothesis_proposal_review_batch import (
    latest_hypothesis_proposal_review_batch,
    load_hypothesis_proposal_review_batch,
)
from research_lab.hypotheses.research_hypothesis import (
    research_hypothesis_path,
    research_hypothesis_registry_path,
)
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


SCHEMA_VERSION = "hypothesis_intake_batch.v1"
INTAKE_VERSION = "hypothesis_intake_gate.v1"
RESEARCH_LABEL = "RESEARCH_ONLY"


def hypothesis_intake_batch_dir(store: Path) -> Path:
    return store / "hypothesis_intake_batches"


def hypothesis_intake_batch_path(store: Path, hypothesis_intake_batch_id: str) -> Path:
    return hypothesis_intake_batch_dir(store) / f"{hypothesis_intake_batch_id}.json"


def hypothesis_intake_batch_registry_path(store: Path) -> Path:
    return store / "registries" / "hypothesis_intake_batch_registry.json"


def _counts(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    return dict(sorted(Counter(str(row.get(key) or "UNKNOWN") for row in rows).items()))


def build_hypothesis_intake_batch(
    *,
    decisions: list[dict[str, Any]],
    source_hypothesis_proposal_review_batch_id: str,
    created_research_hypothesis_ids: list[str],
    latest_integrity_report_id: str,
    latest_research_os_status_report_id: str,
    generated_at: str,
    intake_run_id: str,
) -> dict[str, Any]:
    status_counts = _counts(decisions, "intake_status")
    payload = {
        "hypothesis_intake_batch_id": "",
        "generated_at": generated_at,
        "intake_run_id": intake_run_id,
        "intake_version": INTAKE_VERSION,
        "source_hypothesis_proposal_review_batch_id": source_hypothesis_proposal_review_batch_id,
        "hypothesis_intake_decision_ids": [row["hypothesis_intake_decision_id"] for row in decisions],
        "created_research_hypothesis_ids": created_research_hypothesis_ids,
        "intake_count": len(decisions),
        "accepted_count": status_counts.get("accepted_inactive_research_only", 0),
        "blocked_count": status_counts.get("blocked", 0),
        "deferred_count": status_counts.get("deferred", 0),
        "intake_status_counts": status_counts,
        "latest_integrity_report_id": latest_integrity_report_id,
        "latest_research_os_status_report_id": latest_research_os_status_report_id,
        "non_actionable_research_only_assertion": {
            "research_only": True,
            "intake_gate_only": True,
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
    fingerprint_payload["hypothesis_intake_decision_ids"] = [row["immutable_hash"] for row in decisions]
    fingerprint = content_hash(fingerprint_payload, exclude={"hypothesis_intake_batch_id", "generated_at", "intake_run_id", "immutable_hash", "content_hash"}, sort_lists=True)
    payload["hypothesis_intake_batch_id"] = f"hib_{short_hash(content_hash({'fingerprint': fingerprint, 'generated_at': generated_at}), 16)}"
    payload["immutable_hash"] = fingerprint
    payload["content_hash"] = fingerprint
    validate_contract("hypothesis_intake_batch", payload)
    return payload


def _load_source_review_batch(store: Path, source_batch_id: str | None) -> dict[str, Any]:
    if source_batch_id:
        return load_hypothesis_proposal_review_batch(source_batch_id, store_root=store)
    batch = latest_hypothesis_proposal_review_batch(store_root=store)
    if not batch:
        raise FileNotFoundError("hypothesis_proposal_review_batch_missing")
    return batch


def build_intake_for_review_batch(
    *,
    store_root: Path | None = None,
    source_batch_id: str | None = None,
    review_id: str | None = None,
    generated_at: str,
    allow_red_status: bool = False,
    allow_unapproved_review: bool = False,
    override_reason: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    store = ensure_store_layout(store_root)
    source_batch = _load_source_review_batch(store, source_batch_id)
    review_ids = [review_id] if review_id else list(source_batch.get("hypothesis_proposal_review_ids") or [])
    decisions: list[dict[str, Any]] = []
    hypotheses: list[dict[str, Any]] = []
    for item_id in review_ids:
        decision, hypothesis = build_hypothesis_intake_for_review_id(
            hypothesis_proposal_review_id=str(item_id),
            generated_at=generated_at,
            store_root=store,
            allow_red_status=allow_red_status,
            allow_unapproved_review=allow_unapproved_review,
            override_reason=override_reason,
        )
        decisions.append(decision)
        if hypothesis:
            hypotheses.append(hypothesis)
    return decisions, hypotheses, source_batch


def write_hypothesis_intake_batch(
    *,
    store_root: Path | None = None,
    source_batch_id: str | None = None,
    review_id: str | None = None,
    generated_at: str | None = None,
    allow_red_status: bool = False,
    allow_unapproved_review: bool = False,
    override_reason: str | None = None,
    actor: str = "Aegis",
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    generated_at = generated_at or utc_now_iso()
    decisions, hypotheses, source_batch = build_intake_for_review_batch(
        store_root=store,
        source_batch_id=source_batch_id,
        review_id=review_id,
        generated_at=generated_at,
        allow_red_status=allow_red_status,
        allow_unapproved_review=allow_unapproved_review,
        override_reason=override_reason,
    )

    hypothesis_results = []
    for hypothesis in hypotheses:
        path = research_hypothesis_path(store, hypothesis["research_hypothesis_id"])
        if path.exists():
            raise FileExistsError(f"Refusing to overwrite immutable research hypothesis: {path}")
        write_json(path, hypothesis, overwrite=False)
        row = {
            "research_hypothesis_id": hypothesis["research_hypothesis_id"],
            "generated_at": hypothesis["generated_at"],
            "hypothesis_status": hypothesis["hypothesis_status"],
            "hypothesis_family": hypothesis["hypothesis_family"],
            "source_hypothesis_proposal_id": hypothesis["source_hypothesis_proposal_id"],
            "source_hypothesis_proposal_review_id": hypothesis["source_hypothesis_proposal_review_id"],
            "latest_integrity_report_id": hypothesis["latest_integrity_report_id"],
            "latest_research_os_status_report_id": hypothesis["latest_research_os_status_report_id"],
            "immutable_hash": hypothesis["immutable_hash"],
        }
        append_jsonl(research_hypothesis_registry_path(store), row)
        audit = write_audit_event(
            actor=actor,
            entity_type="research_hypothesis",
            entity_id=hypothesis["research_hypothesis_id"],
            action="research_hypothesis_created",
            new_state_hash=hypothesis["immutable_hash"],
            reason="Created inactive research-only hypothesis from approved proposal review.",
            metadata={
                "research_hypothesis_id": hypothesis["research_hypothesis_id"],
                "hypothesis_status": hypothesis["hypothesis_status"],
                "source_hypothesis_proposal_id": hypothesis["source_hypothesis_proposal_id"],
                "source_hypothesis_proposal_review_id": hypothesis["source_hypothesis_proposal_review_id"],
                "immutable_hash": hypothesis["immutable_hash"],
            },
            store_root=store,
        )
        hypothesis_results.append({"research_hypothesis": hypothesis, "registry_row": row, "audit_event": audit, "json_path": str(path)})

    decision_results = []
    for decision in decisions:
        path = hypothesis_intake_decision_path(store, decision["hypothesis_intake_decision_id"])
        if path.exists():
            raise FileExistsError(f"Refusing to overwrite immutable hypothesis intake decision: {path}")
        write_json(path, decision, overwrite=False)
        row = {
            "hypothesis_intake_decision_id": decision["hypothesis_intake_decision_id"],
            "generated_at": decision["generated_at"],
            "source_hypothesis_proposal_review_id": decision["source_hypothesis_proposal_review_id"],
            "source_hypothesis_proposal_id": decision["source_hypothesis_proposal_id"],
            "intake_status": decision["intake_status"],
            "intake_decision": decision["intake_decision"],
            "created_research_hypothesis_id": decision.get("created_research_hypothesis_id"),
            "latest_integrity_report_id": decision["latest_integrity_report_id"],
            "latest_research_os_status_report_id": decision["latest_research_os_status_report_id"],
            "immutable_hash": decision["immutable_hash"],
        }
        append_jsonl(hypothesis_intake_decision_registry_path(store), row)
        audit = write_audit_event(
            actor=actor,
            entity_type="hypothesis_intake_decision",
            entity_id=decision["hypothesis_intake_decision_id"],
            action="hypothesis_intake_decision_recorded",
            new_state_hash=decision["immutable_hash"],
            reason="Recorded governed inactive research hypothesis intake decision.",
            metadata={
                "hypothesis_intake_decision_id": decision["hypothesis_intake_decision_id"],
                "source_hypothesis_proposal_review_id": decision["source_hypothesis_proposal_review_id"],
                "source_hypothesis_proposal_id": decision["source_hypothesis_proposal_id"],
                "intake_status": decision["intake_status"],
                "intake_decision": decision["intake_decision"],
                "created_research_hypothesis_id": decision.get("created_research_hypothesis_id"),
                "latest_integrity_report_id": decision["latest_integrity_report_id"],
                "latest_research_os_status_report_id": decision["latest_research_os_status_report_id"],
                "immutable_hash": decision["immutable_hash"],
            },
            store_root=store,
        )
        decision_results.append({"decision": decision, "registry_row": row, "audit_event": audit, "json_path": str(path)})

    run_id = f"hirun_{short_hash(content_hash({'generated_at': generated_at, 'source_batch_id': source_batch.get('hypothesis_proposal_review_batch_id'), 'review_id': review_id or ''}), 16)}"
    latest_integrity_report_id = str((decisions[0] if decisions else source_batch).get("latest_integrity_report_id") or source_batch.get("latest_integrity_report_id") or "")
    latest_research_os_status_report_id = str((decisions[0] if decisions else source_batch).get("latest_research_os_status_report_id") or source_batch.get("latest_research_os_status_report_id") or "")
    batch = build_hypothesis_intake_batch(
        decisions=decisions,
        source_hypothesis_proposal_review_batch_id=str(source_batch.get("hypothesis_proposal_review_batch_id") or ""),
        created_research_hypothesis_ids=[row["research_hypothesis"]["research_hypothesis_id"] for row in hypothesis_results],
        latest_integrity_report_id=latest_integrity_report_id,
        latest_research_os_status_report_id=latest_research_os_status_report_id,
        generated_at=generated_at,
        intake_run_id=run_id,
    )
    path = hypothesis_intake_batch_path(store, batch["hypothesis_intake_batch_id"])
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite immutable hypothesis intake batch: {path}")
    write_json(path, batch, overwrite=False)
    row = {
        "hypothesis_intake_batch_id": batch["hypothesis_intake_batch_id"],
        "generated_at": batch["generated_at"],
        "intake_run_id": batch["intake_run_id"],
        "intake_version": batch["intake_version"],
        "source_hypothesis_proposal_review_batch_id": batch["source_hypothesis_proposal_review_batch_id"],
        "intake_count": batch["intake_count"],
        "accepted_count": batch["accepted_count"],
        "blocked_count": batch["blocked_count"],
        "deferred_count": batch["deferred_count"],
        "immutable_hash": batch["immutable_hash"],
    }
    append_jsonl(hypothesis_intake_batch_registry_path(store), row)
    audit = write_audit_event(
        actor=actor,
        entity_type="hypothesis_intake_batch",
        entity_id=batch["hypothesis_intake_batch_id"],
        action="hypothesis_intake_batch_generated",
        new_state_hash=batch["immutable_hash"],
        reason="Generated governed inactive research hypothesis intake batch.",
        metadata={
            "hypothesis_intake_batch_id": batch["hypothesis_intake_batch_id"],
            "source_hypothesis_proposal_review_batch_id": batch["source_hypothesis_proposal_review_batch_id"],
            "intake_count": batch["intake_count"],
            "accepted_count": batch["accepted_count"],
            "blocked_count": batch["blocked_count"],
            "deferred_count": batch["deferred_count"],
            "created_research_hypothesis_ids": batch["created_research_hypothesis_ids"],
            "latest_integrity_report_id": batch["latest_integrity_report_id"],
            "latest_research_os_status_report_id": batch["latest_research_os_status_report_id"],
            "immutable_hash": batch["immutable_hash"],
        },
        store_root=store,
    )
    return {
        "batch": batch,
        "decisions": decision_results,
        "research_hypotheses": hypothesis_results,
        "registry_row": row,
        "audit_event": audit,
        "json_path": str(path),
    }


def load_hypothesis_intake_batch(hypothesis_intake_batch_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    return read_json(hypothesis_intake_batch_path(store, hypothesis_intake_batch_id))


def list_hypothesis_intake_batches(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    return read_jsonl(hypothesis_intake_batch_registry_path(store))


def latest_hypothesis_intake_batch(*, store_root: Path | None = None) -> dict[str, Any] | None:
    rows = list_hypothesis_intake_batches(store_root=store_root)
    if not rows:
        return None
    return load_hypothesis_intake_batch(str(rows[-1]["hypothesis_intake_batch_id"]), store_root=store_root)


def latest_hypothesis_intake_decisions_for_batch(*, store_root: Path | None = None, limit: int = 20) -> list[dict[str, Any]]:
    batch = latest_hypothesis_intake_batch(store_root=store_root)
    if not batch:
        return []
    return [load_hypothesis_intake_decision(str(decision_id), store_root=store_root) for decision_id in batch.get("hypothesis_intake_decision_ids", [])[:limit]]
