from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import validate_contract
from research_lab.hypotheses.hypothesis_proposal import (
    hypothesis_proposal_path,
    hypothesis_proposal_registry_path,
    load_hypothesis_proposal,
)
from research_lab.hypotheses.hypothesis_proposal_engine import PROPOSAL_ENGINE_VERSION, generate_hypothesis_proposals
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


SCHEMA_VERSION = "hypothesis_proposal_batch.v1"
RESEARCH_LABEL = "RESEARCH_ONLY"


def hypothesis_proposal_batch_dir(store: Path) -> Path:
    return store / "hypothesis_proposal_batches"


def hypothesis_proposal_batch_path(store: Path, hypothesis_proposal_batch_id: str) -> Path:
    return hypothesis_proposal_batch_dir(store) / f"{hypothesis_proposal_batch_id}.json"


def hypothesis_proposal_batch_registry_path(store: Path) -> Path:
    return store / "registries" / "hypothesis_proposal_batch_registry.json"


def _counts(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    return dict(sorted(Counter(str(row.get(key) or "UNKNOWN") for row in rows).items()))


def build_hypothesis_proposal_batch(
    *,
    proposals: list[dict[str, Any]],
    source_observation_cluster_batch_id: str,
    latest_integrity_report_id: str,
    latest_research_os_status_report_id: str,
    generated_at: str,
) -> dict[str, Any]:
    status_counts = _counts(proposals, "proposal_status")
    family_counts = _counts(proposals, "proposal_family")
    payload = {
        "hypothesis_proposal_batch_id": "",
        "generated_at": generated_at,
        "proposal_engine_version": PROPOSAL_ENGINE_VERSION,
        "source_observation_cluster_batch_id": source_observation_cluster_batch_id,
        "hypothesis_proposal_ids": [row["hypothesis_proposal_id"] for row in proposals],
        "proposal_count": len(proposals),
        "proposal_status_counts": status_counts,
        "proposal_family_counts": family_counts,
        "blocked_count": status_counts.get("blocked", 0),
        "proposed_for_review_count": status_counts.get("proposed_for_review", 0),
        "system_research_only_count": status_counts.get("system_research_only", 0),
        "latest_integrity_report_id": latest_integrity_report_id,
        "latest_research_os_status_report_id": latest_research_os_status_report_id,
        "non_actionable_research_only_assertion": {
            "research_only": True,
            "proposal_only": True,
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
            "buy_sell_hold_recommendations_allowed": False,
        },
        "schema_version": SCHEMA_VERSION,
        "research_label": RESEARCH_LABEL,
        "immutable_hash": "",
        "content_hash": "",
    }
    fingerprint_payload = dict(payload)
    fingerprint_payload["hypothesis_proposal_ids"] = [row["immutable_hash"] for row in proposals]
    fingerprint = content_hash(fingerprint_payload, exclude={"hypothesis_proposal_batch_id", "generated_at", "immutable_hash", "content_hash"}, sort_lists=True)
    payload["hypothesis_proposal_batch_id"] = f"hpb_{short_hash(content_hash({'fingerprint': fingerprint, 'generated_at': generated_at}), 16)}"
    payload["immutable_hash"] = fingerprint
    payload["content_hash"] = fingerprint
    validate_contract("hypothesis_proposal_batch", payload)
    return payload


def write_hypothesis_proposal_batch(
    *,
    store_root: Path | None = None,
    observation_cluster_batch_id: str | None = None,
    min_priority_score: float = 0.60,
    max_proposals: int = 25,
    generated_at: str | None = None,
    actor: str = "Aegis",
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    generated_at = generated_at or utc_now_iso()
    proposals, source_batch, integrity, research_status = generate_hypothesis_proposals(
        store_root=store,
        observation_cluster_batch_id=observation_cluster_batch_id,
        min_priority_score=min_priority_score,
        max_proposals=max_proposals,
        generated_at=generated_at,
    )
    proposal_results = []
    for proposal in proposals:
        path = hypothesis_proposal_path(store, proposal["hypothesis_proposal_id"])
        if path.exists():
            raise FileExistsError(f"Refusing to overwrite immutable hypothesis proposal: {path}")
        write_json(path, proposal, overwrite=False)
        row = {
            "hypothesis_proposal_id": proposal["hypothesis_proposal_id"],
            "generated_at": proposal["generated_at"],
            "proposal_status": proposal["proposal_status"],
            "proposal_family": proposal["proposal_family"],
            "source_observation_cluster_id": proposal["source_observation_cluster_id"],
            "source_observation_cluster_batch_id": proposal["source_observation_cluster_batch_id"],
            "latest_integrity_report_id": proposal["latest_integrity_report_id"],
            "latest_research_os_status_report_id": proposal["latest_research_os_status_report_id"],
            "immutable_hash": proposal["immutable_hash"],
        }
        append_jsonl(hypothesis_proposal_registry_path(store), row)
        audit = write_audit_event(
            actor=actor,
            entity_type="hypothesis_proposal",
            entity_id=proposal["hypothesis_proposal_id"],
            action="hypothesis_proposal_generated",
            new_state_hash=proposal["immutable_hash"],
            reason="Generated read-only hypothesis proposal from observation cluster.",
            metadata={
                "hypothesis_proposal_id": proposal["hypothesis_proposal_id"],
                "hypothesis_proposal_batch_id": "",
                "proposal_status": proposal["proposal_status"],
                "proposal_family": proposal["proposal_family"],
                "source_observation_cluster_id": proposal["source_observation_cluster_id"],
                "latest_integrity_report_id": proposal["latest_integrity_report_id"],
                "latest_research_os_status_report_id": proposal["latest_research_os_status_report_id"],
                "immutable_hash": proposal["immutable_hash"],
            },
            store_root=store,
        )
        proposal_results.append({"proposal": proposal, "registry_row": row, "audit_event": audit, "json_path": str(path)})
    batch = build_hypothesis_proposal_batch(
        proposals=proposals,
        source_observation_cluster_batch_id=str(source_batch.get("observation_cluster_batch_id") or ""),
        latest_integrity_report_id=str((integrity or {}).get("integrity_report_id") or source_batch.get("latest_integrity_report_id") or ""),
        latest_research_os_status_report_id=str((research_status or {}).get("research_os_status_report_id") or source_batch.get("latest_research_os_status_report_id") or ""),
        generated_at=generated_at,
    )
    path = hypothesis_proposal_batch_path(store, batch["hypothesis_proposal_batch_id"])
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite immutable hypothesis proposal batch: {path}")
    write_json(path, batch, overwrite=False)
    row = {
        "hypothesis_proposal_batch_id": batch["hypothesis_proposal_batch_id"],
        "generated_at": batch["generated_at"],
        "proposal_engine_version": batch["proposal_engine_version"],
        "source_observation_cluster_batch_id": batch["source_observation_cluster_batch_id"],
        "proposal_count": batch["proposal_count"],
        "blocked_count": batch["blocked_count"],
        "proposed_for_review_count": batch["proposed_for_review_count"],
        "system_research_only_count": batch["system_research_only_count"],
        "immutable_hash": batch["immutable_hash"],
    }
    append_jsonl(hypothesis_proposal_batch_registry_path(store), row)
    audit = write_audit_event(
        actor=actor,
        entity_type="hypothesis_proposal_batch",
        entity_id=batch["hypothesis_proposal_batch_id"],
        action="hypothesis_proposal_batch_generated",
        new_state_hash=batch["immutable_hash"],
        reason="Generated read-only hypothesis proposal batch.",
        metadata={
            "hypothesis_proposal_batch_id": batch["hypothesis_proposal_batch_id"],
            "proposal_count": batch["proposal_count"],
            "proposal_status_counts": batch["proposal_status_counts"],
            "proposal_family_counts": batch["proposal_family_counts"],
            "latest_integrity_report_id": batch["latest_integrity_report_id"],
            "latest_research_os_status_report_id": batch["latest_research_os_status_report_id"],
            "immutable_hash": batch["immutable_hash"],
        },
        store_root=store,
    )
    return {"batch": batch, "proposals": proposal_results, "registry_row": row, "audit_event": audit, "json_path": str(path)}


def load_hypothesis_proposal_batch(hypothesis_proposal_batch_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    return read_json(hypothesis_proposal_batch_path(store, hypothesis_proposal_batch_id))


def list_hypothesis_proposal_batches(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    return read_jsonl(hypothesis_proposal_batch_registry_path(store))


def latest_hypothesis_proposal_batch(*, store_root: Path | None = None) -> dict[str, Any] | None:
    rows = list_hypothesis_proposal_batches(store_root=store_root)
    if not rows:
        return None
    return load_hypothesis_proposal_batch(str(rows[-1]["hypothesis_proposal_batch_id"]), store_root=store_root)


def latest_hypothesis_proposals_for_batch(*, store_root: Path | None = None, limit: int = 20) -> list[dict[str, Any]]:
    batch = latest_hypothesis_proposal_batch(store_root=store_root)
    if not batch:
        return []
    return [load_hypothesis_proposal(str(proposal_id), store_root=store_root) for proposal_id in batch.get("hypothesis_proposal_ids", [])[:limit]]

