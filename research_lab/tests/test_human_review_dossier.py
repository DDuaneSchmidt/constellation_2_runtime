from __future__ import annotations

from pathlib import Path

import pytest

from ops.aegis.research_lab.research_lab_routes import research_lab_human_review_dossier_v1, research_lab_human_review_dossiers_v1
from research_lab.challengers.challenger_comparison import write_challenger_comparison_report
from research_lab.challengers.challenger_evidence import write_challenger_evidence_batch
from research_lab.challengers.human_review_dossier import (
    ALLOWED_HUMAN_DECISION_OPTIONS,
    build_human_review_dossier,
    write_human_review_dossier,
)
from research_lab.contracts.schemas import validate_contract
from research_lab.storage.manifest_io import read_jsonl
from research_lab.tests.test_challenger_evidence_batch import _write_fixture


def _write_comparison(store: Path) -> str:
    track_id = _write_fixture(store)
    batch = write_challenger_evidence_batch(challenger_track_id=track_id, store_root=store)["batch"]
    report = write_challenger_comparison_report(challenger_evidence_batch_id=batch["challenger_evidence_batch_id"], store_root=store)["report"]
    return report["challenger_comparison_report_id"]


def _files(store: Path, rel: str) -> list[str]:
    root = store / rel
    return sorted(path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file()) if root.exists() else []


def test_human_review_dossier_creation_and_source_ids(tmp_path: Path) -> None:
    store = tmp_path / "store"
    comparison_id = _write_comparison(store)

    dossier = build_human_review_dossier(challenger_comparison_report_id=comparison_id, store_root=store)

    validate_contract("human_review_dossier", dossier)
    assert dossier["dossier_type"] == "CHALLENGER_REVIEW"
    assert dossier["source_report_ids"]["challenger_comparison_report_id"] == comparison_id
    assert dossier["source_report_ids"]["expectancy_drift_report_id"] == "edr_fixture"
    assert dossier["source_report_ids"]["regime_fragility_report_id"] == "rfr_fixture"
    assert dossier["research_label"] == "RESEARCH_ONLY"


def test_summary_context_candidates_and_blocked_items(tmp_path: Path) -> None:
    store = tmp_path / "store"
    comparison_id = _write_comparison(store)

    dossier = build_human_review_dossier(challenger_comparison_report_id=comparison_id, store_root=store)

    assert dossier["executive_summary"]["comparison_sufficiency"] == "comparison_ready"
    assert dossier["incumbent_context"]["sleeve_id"] == "slv_fixture"
    assert dossier["challenger_context"]["active_challenger_count"] == 6
    assert len(dossier["review_candidates"]) == 6
    assert len(dossier["blocked_or_excluded_items"]) == 1
    assert dossier["blocked_or_excluded_items"][0]["variant_name"] == "cost_sensitivity_variant"
    assert dossier["blocked_or_excluded_items"][0]["failure_reason"] == "cost_sensitivity_requires_supported_cost_model_snapshot"


def test_no_winner_or_promotion_or_retirement_recommendation(tmp_path: Path) -> None:
    store = tmp_path / "store"
    comparison_id = _write_comparison(store)

    dossier = build_human_review_dossier(challenger_comparison_report_id=comparison_id, store_root=store)
    text = str(dossier).lower()

    assert "winner" not in text
    assert dossier["governance_constraints"]["automatic_promotion_allowed"] is False
    assert dossier["governance_constraints"]["automatic_retirement_allowed"] is False
    assert all(option in ALLOWED_HUMAN_DECISION_OPTIONS for option in dossier["required_human_decision_options"])
    assert "automatic_promotion" not in dossier["required_human_decision_options"]
    assert "automatic_retirement" not in dossier["required_human_decision_options"]


def test_expected_recommended_next_action(tmp_path: Path) -> None:
    store = tmp_path / "store"
    comparison_id = _write_comparison(store)

    dossier = build_human_review_dossier(challenger_comparison_report_id=comparison_id, store_root=store)

    assert dossier["recommended_next_action"] == "open_challenger_paper_trial"


def test_persistence_registry_audit_immutability_and_no_mutations(tmp_path: Path) -> None:
    store = tmp_path / "store"
    comparison_id = _write_comparison(store)
    sleeve_before = _files(store, "sleeves")
    paper_before = _files(store, "paper_trials")
    candidate_before = _files(store, "candidate_batches")

    result = write_human_review_dossier(challenger_comparison_report_id=comparison_id, store_root=store)

    assert read_jsonl(store / "registries" / "human_review_dossiers.jsonl")[-1]["human_review_dossier_id"] == result["dossier"]["human_review_dossier_id"]
    assert any(row["entity_id"] == result["dossier"]["human_review_dossier_id"] for row in read_jsonl(store / "audit_log" / "audit_events.jsonl"))
    assert _files(store, "sleeves") == sleeve_before
    assert _files(store, "paper_trials") == paper_before
    assert _files(store, "candidate_batches") == candidate_before
    with pytest.raises(FileExistsError):
        write_human_review_dossier(challenger_comparison_report_id=comparison_id, store_root=store)


def test_no_execution_optimizer_ml_or_capital_allocation_objects(tmp_path: Path) -> None:
    store = tmp_path / "store"
    comparison_id = _write_comparison(store)

    dossier = build_human_review_dossier(challenger_comparison_report_id=comparison_id, store_root=store)
    keys = {str(key).lower() for key in dossier}

    assert dossier["governance_constraints"]["broker_execution_allowed"] is False
    assert dossier["governance_constraints"]["order_management_allowed"] is False
    assert dossier["governance_constraints"]["portfolio_optimizer_allowed"] is False
    assert dossier["governance_constraints"]["ml_ranking_allowed"] is False
    assert dossier["governance_constraints"]["capital_allocation_allowed"] is False
    assert "broker_order" not in keys
    assert "order_management_object" not in keys
    assert "capital_allocation_object" not in keys
    assert "ml_ranking_model" not in keys


def test_api_routes_are_read_only_projection(tmp_path: Path) -> None:
    store = tmp_path / "store"
    comparison_id = _write_comparison(store)
    result = write_human_review_dossier(challenger_comparison_report_id=comparison_id, store_root=store)

    listing = research_lab_human_review_dossiers_v1(store_root=store)
    detail = research_lab_human_review_dossier_v1(human_review_dossier_id=result["dossier"]["human_review_dossier_id"], store_root=store)

    assert listing["read_only"] is True
    assert detail["read_only"] is True
    assert listing["human_review_dossiers"][0]["human_review_dossier_id"] == result["dossier"]["human_review_dossier_id"]
    text = str(detail).lower()
    assert "canonical_ohlcv" not in text
    assert "trading_instructions" not in text
    assert "order_instructions" not in text
    assert "capital_allocation_instructions" not in text


def test_repeat_run_stable_fields(tmp_path: Path) -> None:
    store = tmp_path / "store"
    comparison_id = _write_comparison(store)

    first = build_human_review_dossier(challenger_comparison_report_id=comparison_id, store_root=store)
    second = build_human_review_dossier(challenger_comparison_report_id=comparison_id, store_root=store)

    assert first["executive_summary"] == second["executive_summary"]
    assert first["review_candidates"] == second["review_candidates"]
    assert first["blocked_or_excluded_items"] == second["blocked_or_excluded_items"]
    assert first["human_review_dossier_id"] == second["human_review_dossier_id"]
