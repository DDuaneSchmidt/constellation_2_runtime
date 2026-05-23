from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash
from research_lab.storage.manifest_io import read_json, read_jsonl
from research_lab.storage.paths import ensure_store_layout


SCHEMA_VERSION = "hypothesis_proposal.v1"
PROPOSAL_VERSION = "observation_to_hypothesis_proposal.v1"
RESEARCH_LABEL = "RESEARCH_ONLY"
ACTIONABILITY_STATUS = "non_actionable_research_only"


def hypothesis_proposal_dir(store: Path) -> Path:
    return store / "hypothesis_proposals"


def hypothesis_proposal_path(store: Path, hypothesis_proposal_id: str) -> Path:
    return hypothesis_proposal_dir(store) / f"{hypothesis_proposal_id}.json"


def hypothesis_proposal_registry_path(store: Path) -> Path:
    return store / "registries" / "hypothesis_proposal_registry.json"


def build_hypothesis_proposal(
    *,
    generated_at: str,
    proposal_status: str,
    proposal_family: str,
    proposal_label: str,
    proposed_hypothesis_statement: str,
    proposed_research_question: str,
    source_observation_cluster_id: str,
    source_observation_cluster_batch_id: str,
    source_observation_candidate_ids: list[str],
    cluster_family: str,
    cluster_status: str,
    research_priority_score: float,
    eligibility_status: str,
    eligibility_checks: dict[str, Any],
    blockers: list[dict[str, Any]],
    required_next_evidence: list[str],
    proposed_test_design: dict[str, Any],
    proposed_event_window: str,
    proposed_universe: str,
    proposed_benchmark: str,
    latest_integrity_report_id: str,
    latest_research_os_status_report_id: str,
    source_artifact_ids: dict[str, Any],
    source_artifact_hashes: dict[str, Any],
) -> dict[str, Any]:
    payload = {
        "hypothesis_proposal_id": "",
        "generated_at": generated_at,
        "proposal_version": PROPOSAL_VERSION,
        "proposal_status": proposal_status,
        "proposal_family": proposal_family,
        "proposal_label": proposal_label,
        "proposed_hypothesis_statement": proposed_hypothesis_statement,
        "proposed_research_question": proposed_research_question,
        "source_observation_cluster_id": source_observation_cluster_id,
        "source_observation_cluster_batch_id": source_observation_cluster_batch_id,
        "source_observation_candidate_ids": source_observation_candidate_ids,
        "cluster_family": cluster_family,
        "cluster_status": cluster_status,
        "research_priority_score": float(research_priority_score),
        "eligibility_status": eligibility_status,
        "eligibility_checks": eligibility_checks,
        "blockers": blockers,
        "required_next_evidence": required_next_evidence,
        "proposed_test_design": proposed_test_design,
        "proposed_event_window": proposed_event_window,
        "proposed_universe": proposed_universe,
        "proposed_benchmark": proposed_benchmark,
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
        "actionability_status": ACTIONABILITY_STATUS,
        "latest_integrity_report_id": latest_integrity_report_id,
        "latest_research_os_status_report_id": latest_research_os_status_report_id,
        "source_artifact_ids": source_artifact_ids,
        "source_artifact_hashes": source_artifact_hashes,
        "schema_version": SCHEMA_VERSION,
        "research_label": RESEARCH_LABEL,
        "immutable_hash": "",
        "content_hash": "",
    }
    fingerprint = content_hash(payload, exclude={"hypothesis_proposal_id", "generated_at", "immutable_hash", "content_hash"}, sort_lists=True)
    payload["hypothesis_proposal_id"] = f"hprop_{short_hash(content_hash({'fingerprint': fingerprint, 'generated_at': generated_at}), 16)}"
    payload["immutable_hash"] = fingerprint
    payload["content_hash"] = fingerprint
    validate_contract("hypothesis_proposal", payload)
    return payload


def load_hypothesis_proposal(hypothesis_proposal_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    return read_json(hypothesis_proposal_path(store, hypothesis_proposal_id))


def list_hypothesis_proposals(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    return read_jsonl(hypothesis_proposal_registry_path(store))


def latest_hypothesis_proposal(*, store_root: Path | None = None) -> dict[str, Any] | None:
    rows = list_hypothesis_proposals(store_root=store_root)
    if not rows:
        return None
    return load_hypothesis_proposal(str(rows[-1]["hypothesis_proposal_id"]), store_root=store_root)


def hypothesis_proposals_for_cluster(observation_cluster_id: str, *, store_root: Path | None = None) -> list[dict[str, Any]]:
    rows = [row for row in list_hypothesis_proposals(store_root=store_root) if row.get("source_observation_cluster_id") == observation_cluster_id]
    return [load_hypothesis_proposal(str(row["hypothesis_proposal_id"]), store_root=store_root) for row in rows]

