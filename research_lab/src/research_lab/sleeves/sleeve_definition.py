from __future__ import annotations

from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso


SLEEVE_DEFINITION_SCHEMA_VERSION = "sleeve_definition.v1"
SLEEVE_VERSION_SCHEMA_VERSION = "sleeve_version.v1"


def validate_sleeve_definition(definition: dict[str, Any]) -> None:
    validate_contract("sleeve_definition", definition)


def validate_sleeve_version(version: dict[str, Any]) -> None:
    validate_contract("sleeve_version", version)


def build_sleeve_definition(
    *,
    sleeve_id: str,
    name: str,
    hypothesis_id: str,
    sleeve_type: str = "research_only",
    description: str = "",
    status: str = "draft",
    created_by: str = "Aegis",
    created_at: str | None = None,
) -> dict[str, Any]:
    payload = {
        "sleeve_id": sleeve_id,
        "name": name,
        "description": description or f"Research-only governance sleeve for {hypothesis_id}.",
        "hypothesis_id": hypothesis_id,
        "sleeve_type": sleeve_type,
        "status": status,
        "created_at": created_at or utc_now_iso(),
        "created_by": created_by,
        "schema_version": SLEEVE_DEFINITION_SCHEMA_VERSION,
        "compliance_label": "Governance metadata only. Linked backtests/model outputs are hypothetical research evidence, not achieved portfolio performance.",
    }
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    validate_sleeve_definition(payload)
    return payload


def build_sleeve_version(
    *,
    sleeve_id: str,
    version: str,
    hypothesis_id: str,
    event_study_evidence_package_id: str,
    backtest_evidence_package_id: str,
    candidate_batch_id: str,
    dataset_snapshot_id: str,
    regime_snapshot_id: str,
    cost_model_snapshot_id: str,
    created_by: str = "Aegis",
    created_at: str | None = None,
) -> dict[str, Any]:
    linked_evidence = sorted({event_study_evidence_package_id, backtest_evidence_package_id})
    payload = {
        "sleeve_id": sleeve_id,
        "version": version,
        "hypothesis_id": hypothesis_id,
        "linked_evidence_package_ids": linked_evidence,
        "linked_backtest_evidence_package_id": backtest_evidence_package_id,
        "linked_event_study_evidence_package_id": event_study_evidence_package_id,
        "linked_candidate_batch_ids": [candidate_batch_id],
        "dataset_snapshot_id": dataset_snapshot_id,
        "regime_snapshot_id": regime_snapshot_id,
        "cost_model_snapshot_id": cost_model_snapshot_id,
        "candidate_generator_version": "drop_reversion_candidate_generator_v1.0",
        "ranking_policy_version": "candidate_ranking_policy_v1",
        "signal_rule": {"type": "daily_return_below_threshold", "params": {"return_column": "adj_close", "threshold": -0.02}},
        "execution_model": {
            "label": "research_simulation_only",
            "entry_timing": "next_open",
            "exit_timing": "close_after_n_sessions",
            "holding_period_sessions": 5,
            "broker_execution_allowed": False,
            "live_trading_allowed": False,
        },
        "portfolio_model": {
            "label": "research_simulation_only",
            "weighting": "equal_weight",
            "max_positions": 5,
            "portfolio_optimizer_allowed": False,
        },
        "risk_notes": [
            "Positive post-cost backtest result materially underperformed SPY benchmark.",
            "No live trading, broker execution, or sleeve mutation is authorized by this version.",
        ],
        "limitations": [
            "Backtest/model output is hypothetical research evidence, not achieved portfolio performance.",
            "Candidate/outcome sample is not yet sufficient for operational confidence.",
        ],
        "created_at": created_at or utc_now_iso(),
        "created_by": created_by,
        "schema_version": SLEEVE_VERSION_SCHEMA_VERSION,
    }
    seed_hash = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    payload["sleeve_version_id"] = f"slvv_{sleeve_id}_{version}_{short_hash(seed_hash, 10)}"
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    validate_sleeve_version(payload)
    return payload

