from __future__ import annotations

from typing import Any

from research_lab.candidates.candidate_batch import CANDIDATE_GENERATOR_NAME, CANDIDATE_GENERATOR_VERSION
from research_lab.candidates.candidate_scoring import RANKING_POLICY_VERSION
from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso


def validate_paper_trial(trial: dict[str, Any]) -> None:
    validate_contract("paper_trial", trial)


def build_paper_trial(
    *,
    sleeve_id: str,
    sleeve_version_id: str,
    hypothesis_id: str,
    dataset_snapshot_id: str,
    regime_snapshot_id: str,
    cost_model_snapshot_id: str,
    source_evidence_package_ids: list[str],
    threshold: float,
    observation_frequency: str = "manual",
    outcome_windows: list[int] | None = None,
    status: str = "draft",
    created_by: str = "Aegis",
    created_at: str | None = None,
) -> dict[str, Any]:
    payload = {
        "sleeve_id": sleeve_id,
        "sleeve_version_id": sleeve_version_id,
        "hypothesis_id": hypothesis_id,
        "dataset_snapshot_id": dataset_snapshot_id,
        "regime_snapshot_id": regime_snapshot_id,
        "cost_model_snapshot_id": cost_model_snapshot_id,
        "source_evidence_package_ids": sorted(source_evidence_package_ids),
        "candidate_generator_name": CANDIDATE_GENERATOR_NAME,
        "candidate_generator_version": CANDIDATE_GENERATOR_VERSION,
        "ranking_policy_version": RANKING_POLICY_VERSION,
        "threshold": float(threshold),
        "observation_frequency": observation_frequency,
        "outcome_windows": sorted({int(window) for window in (outcome_windows or [1, 2, 5, 10, 20])}),
        "status": status,
        "started_at": "",
        "created_at": created_at or utc_now_iso(),
        "created_by": created_by,
        "schema_version": "paper_trial.v1",
        "research_label": "Forward observational paper trial; no broker execution; no live trading; not achieved portfolio performance.",
    }
    seed = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    payload["paper_trial_id"] = f"ptr_{sleeve_id}_{short_hash(seed, 12)}"
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    validate_paper_trial(payload)
    return payload

