from __future__ import annotations

from pathlib import Path

from constellation_2.common.portfolio_research_os.full_model_transition import (
    DATA_REQUIRED,
    USE_PORTFOLIO123_BRIDGE,
    build_full_model_transition,
    run_full_model_transition,
)


def test_full_model_factors_remain_blocked_without_pit_fundamentals() -> None:
    report = build_full_model_transition(created_at="2026-06-07T00:00:00Z")

    assert report["full_model_readiness_decision"]["classification"] == DATA_REQUIRED
    validators = {row["factor"]: row for row in report["factor_import_validators"]}
    for factor in ["growth", "quality", "valuation", "PEG", "PEGY"]:
        assert validators[factor]["status"] == DATA_REQUIRED
        assert "PIT fundamentals" in validators[factor]["blocker"]


def test_portfolio123_evidence_is_external_manual_not_proof() -> None:
    report = build_full_model_transition(created_at="2026-06-07T00:00:00Z")

    exports = report["portfolio123_export_contract"]
    assert all(row["evidence_label"] == "EXTERNAL_MANUAL_NOT_PROOF" for row in exports)
    assert "not proof" in report["required_conclusions"][1]


def test_small_cap_quality_is_not_treated_as_proof_of_atlas() -> None:
    report = build_full_model_transition(created_at="2026-06-07T00:00:00Z")

    review = report["small_cap_quality_review"]
    assert any(row["dimension"] == "benchmark excess" and "not proof" in row["atlas_implication"] for row in review)
    assert report["full_model_evidence_review"]["decision"] == USE_PORTFOLIO123_BRIDGE


def test_sharadar_contract_includes_filing_dates_and_delisted_securities() -> None:
    report = build_full_model_transition(created_at="2026-06-07T00:00:00Z")

    datasets = {row["dataset"] for row in report["sharadar_import_contract"]}
    assert "filing dates" in datasets
    assert "delisted status" in datasets
    contract_text = " ".join(row["required_fields"] for row in report["sharadar_import_contract"])
    assert "filing_date" in contract_text
    assert "delisted" in contract_text


def test_no_capital_recommendation_emitted() -> None:
    report = build_full_model_transition(created_at="2026-06-07T00:00:00Z")

    boundary = report["authority_boundary"]
    assert boundary["research_only"] is True
    assert boundary["recommendations_authorized"] is False
    assert boundary["capital_allocation_authorized"] is False
    assert boundary["trades_authorized"] is False
    assert boundary["broker_execution_authorized"] is False
    assert "No trades. No recommendations." in report["authority"]


def test_run_full_model_transition_writes_required_outputs(tmp_path: Path) -> None:
    report = run_full_model_transition(tmp_path, created_at="2026-06-07T00:00:00Z")
    out = tmp_path / "full_model_data_transition"

    assert report["status"] == DATA_REQUIRED
    for name in [
        "latest.json",
        "latest_summary.md",
        "full_model_gap_report.csv",
        "sharadar_import_contract.csv",
        "portfolio123_export_contract.csv",
        "small_cap_quality_review.csv",
        "portfolio_atlas_v1_p123_test_plan.csv",
        "full_model_readiness_decision.csv",
    ]:
        assert (out / name).exists()
