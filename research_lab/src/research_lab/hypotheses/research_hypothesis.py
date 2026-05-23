from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash
from research_lab.storage.manifest_io import read_json, read_jsonl
from research_lab.storage.paths import ensure_store_layout


SCHEMA_VERSION = "research_hypothesis.v1"
HYPOTHESIS_VERSION = "inactive_research_hypothesis.v1"
RESEARCH_LABEL = "RESEARCH_ONLY"
ACTIONABILITY_STATUS = "non_actionable_research_only"


def research_hypothesis_dir(store: Path) -> Path:
    return store / "research_hypotheses"


def research_hypothesis_path(store: Path, research_hypothesis_id: str) -> Path:
    return research_hypothesis_dir(store) / f"{research_hypothesis_id}.json"


def research_hypothesis_registry_path(store: Path) -> Path:
    return store / "registries" / "research_hypothesis_registry.json"


def build_research_hypothesis(
    *,
    review: dict[str, Any],
    proposal: dict[str, Any],
    generated_at: str,
    blockers: list[dict[str, Any]] | None = None,
    latest_integrity_report_id: str = "",
    latest_research_os_status_report_id: str = "",
) -> dict[str, Any]:
    test_design = proposal.get("proposed_test_design") or {}
    payload = {
        "research_hypothesis_id": "",
        "generated_at": generated_at,
        "hypothesis_version": HYPOTHESIS_VERSION,
        "hypothesis_status": "inactive_research_only",
        "hypothesis_family": str(proposal.get("proposal_family") or ""),
        "hypothesis_label": str(proposal.get("proposal_label") or ""),
        "hypothesis_statement": str(proposal.get("proposed_hypothesis_statement") or ""),
        "research_question": str(proposal.get("proposed_research_question") or ""),
        "source_hypothesis_proposal_id": str(proposal.get("hypothesis_proposal_id") or ""),
        "source_hypothesis_proposal_review_id": str(review.get("hypothesis_proposal_review_id") or ""),
        "source_observation_cluster_id": str(proposal.get("source_observation_cluster_id") or review.get("source_observation_cluster_id") or ""),
        "source_observation_candidate_ids": list(proposal.get("source_observation_candidate_ids") or review.get("source_observation_candidate_ids") or []),
        "proposed_test_design": test_design,
        "required_data": list(test_design.get("required_data") or []),
        "minimum_observation_count": int(test_design.get("minimum_observation_count") or 0),
        "proposed_event_window": str(proposal.get("proposed_event_window") or test_design.get("event_window") or ""),
        "proposed_universe": str(proposal.get("proposed_universe") or ""),
        "proposed_benchmark": str(proposal.get("proposed_benchmark") or test_design.get("benchmark") or ""),
        "blockers": blockers or [],
        "latest_integrity_report_id": latest_integrity_report_id or str(proposal.get("latest_integrity_report_id") or review.get("latest_integrity_report_id") or ""),
        "latest_research_os_status_report_id": latest_research_os_status_report_id or str(proposal.get("latest_research_os_status_report_id") or review.get("latest_research_os_status_report_id") or ""),
        "non_actionable_research_only_assertion": {
            "research_only": True,
            "inactive_only": True,
            "active_hypothesis_created": False,
            "market_action_authorized": False,
            "buy_sell_hold_recommendations_allowed": False,
        },
        "no_challenger_created_assertion": True,
        "no_sleeve_created_assertion": True,
        "no_capital_allocation_assertion": True,
        "actionability_status": ACTIONABILITY_STATUS,
        "source_artifact_ids": {
            "hypothesis_proposal_id": proposal.get("hypothesis_proposal_id"),
            "hypothesis_proposal_review_id": review.get("hypothesis_proposal_review_id"),
            "hypothesis_proposal_batch_id": review.get("hypothesis_proposal_batch_id") or (proposal.get("source_artifact_ids") or {}).get("hypothesis_proposal_batch_id"),
            "source_observation_cluster_id": proposal.get("source_observation_cluster_id") or review.get("source_observation_cluster_id"),
            "source_observation_candidate_ids": list(proposal.get("source_observation_candidate_ids") or review.get("source_observation_candidate_ids") or []),
            "latest_integrity_report_id": latest_integrity_report_id or proposal.get("latest_integrity_report_id") or review.get("latest_integrity_report_id"),
            "latest_research_os_status_report_id": latest_research_os_status_report_id or proposal.get("latest_research_os_status_report_id") or review.get("latest_research_os_status_report_id"),
        },
        "source_artifact_hashes": {
            "source_proposal_hash": proposal.get("immutable_hash") or proposal.get("content_hash"),
            "source_review_hash": review.get("immutable_hash") or review.get("content_hash"),
            **(proposal.get("source_artifact_hashes") or {}),
        },
        "schema_version": SCHEMA_VERSION,
        "research_label": RESEARCH_LABEL,
        "immutable_hash": "",
        "content_hash": "",
    }
    fingerprint = content_hash(payload, exclude={"research_hypothesis_id", "generated_at", "immutable_hash", "content_hash"}, sort_lists=True)
    payload["research_hypothesis_id"] = f"rhyp_{short_hash(content_hash({'fingerprint': fingerprint, 'generated_at': generated_at}), 16)}"
    payload["immutable_hash"] = fingerprint
    payload["content_hash"] = fingerprint
    validate_contract("research_hypothesis", payload)
    return payload


def load_research_hypothesis(research_hypothesis_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    return read_json(research_hypothesis_path(store, research_hypothesis_id))


def list_research_hypotheses(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    return read_jsonl(research_hypothesis_registry_path(store))


def latest_research_hypothesis(*, store_root: Path | None = None) -> dict[str, Any] | None:
    rows = list_research_hypotheses(store_root=store_root)
    if not rows:
        return None
    return load_research_hypothesis(str(rows[-1]["research_hypothesis_id"]), store_root=store_root)
