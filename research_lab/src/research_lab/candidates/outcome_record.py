from __future__ import annotations

from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso


OUTCOME_SCHEMA_VERSION = "outcome_record.v1"


def validate_outcome_record(record: dict[str, Any]) -> None:
    validate_contract("outcome_record", record)


def build_pending_outcome_record(
    *,
    candidate_id: str,
    candidate_batch_id: str,
    source_evidence_package_id: str = "",
    dataset_snapshot_id: str = "",
    regime_snapshot_id: str = "",
    cost_model_snapshot_id: str = "",
    symbol: str = "",
    benchmark_symbol: str = "SPY",
    signal_date: str = "",
    as_of_date: str = "",
    outcome_window: str,
    outcome_start_date: str,
    outcome_end_date: str,
) -> dict[str, Any]:
    payload = {
        "candidate_id": candidate_id,
        "candidate_batch_id": candidate_batch_id,
        "source_evidence_package_id": source_evidence_package_id,
        "dataset_snapshot_id": dataset_snapshot_id,
        "regime_snapshot_id": regime_snapshot_id,
        "cost_model_snapshot_id": cost_model_snapshot_id,
        "symbol": symbol,
        "benchmark_symbol": benchmark_symbol,
        "signal_date": signal_date,
        "as_of_date": as_of_date,
        "outcome_window": outcome_window,
        "outcome_start_date": outcome_start_date,
        "outcome_end_date": outcome_end_date,
        "gross_return": None,
        "post_cost_return": None,
        "benchmark_return": None,
        "excess_return": None,
        "outcome_status": "pending",
        "created_at": utc_now_iso(),
        "schema_version": OUTCOME_SCHEMA_VERSION,
    }
    payload["content_hash"] = content_hash(payload)
    payload["outcome_record_id"] = f"out_{short_hash(payload['content_hash'], 16)}"
    payload["content_hash"] = content_hash(payload)
    validate_outcome_record(payload)
    return payload
