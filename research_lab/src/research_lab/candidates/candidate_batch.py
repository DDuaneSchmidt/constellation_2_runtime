from __future__ import annotations

from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash


CANDIDATE_BATCH_SCHEMA_VERSION = "candidate_batch.v1"
CANDIDATE_SCHEMA_VERSION = "candidate.v1"
CANDIDATE_GENERATOR_NAME = "drop_reversion_candidate_generator_v1"
CANDIDATE_GENERATOR_VERSION = "drop_reversion_candidate_generator_v1.0"


def validate_candidate_batch(batch: dict[str, Any]) -> None:
    validate_contract("candidate_batch", batch)


def validate_candidate(candidate: dict[str, Any]) -> None:
    validate_contract("candidate", candidate)


def build_candidate_batch(
    *,
    hypothesis_id: str,
    source_evidence_package_id: str,
    dataset_snapshot_id: str,
    regime_snapshot_id: str,
    cost_model_snapshot_id: str,
    symbols: list[str],
    as_of_date: str,
    threshold: float,
    created_at: str,
    created_by: str,
    ranking_policy_version: str,
) -> dict[str, Any]:
    payload = {
        "hypothesis_id": hypothesis_id,
        "source_evidence_package_id": source_evidence_package_id,
        "dataset_snapshot_id": dataset_snapshot_id,
        "regime_snapshot_id": regime_snapshot_id,
        "cost_model_snapshot_id": cost_model_snapshot_id,
        "candidate_generator_name": CANDIDATE_GENERATOR_NAME,
        "candidate_generator_version": CANDIDATE_GENERATOR_VERSION,
        "ranking_policy_version": ranking_policy_version,
        "symbols": sorted({symbol.upper() for symbol in symbols}),
        "as_of_date": as_of_date,
        "threshold": float(threshold),
        "created_at": created_at,
        "created_by": created_by,
        "schema_version": CANDIDATE_BATCH_SCHEMA_VERSION,
    }
    stable_hash = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    payload["candidate_batch_id"] = (
        f"cb_{hypothesis_id}_{as_of_date.replace('-', '')}_{short_hash(stable_hash, 10)}"
    )
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    validate_candidate_batch(payload)
    return payload

