from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.portfolio_research_os.decision_grade_validation import (
    AUTHORITY,
    run_decision_grade_validation,
    run_monte_carlo_retirement_comparison,
    run_oak_harvest_replacement_evidence_matrix,
    run_portfolio_atlas_kill_test_decision_grade,
    run_portfolio_review_board_vote,
    run_shadow_portfolio_starter,
    run_strict_holdout_run,
    run_tax_turnover_proxy_run,
)


def test_holdout_is_not_retuned(tmp_path: Path) -> None:
    report = run_strict_holdout_run(tmp_path, created_at="2026-06-06T00:00:00Z")

    result = report["holdout_results"][0]
    assert result["train_start"] == "2010-01-01"
    assert result["train_end"] == "2016-12-31"
    assert result["validation_start"] == "2017-01-01"
    assert result["validation_end"] == "2020-12-31"
    assert result["holdout_start"] == "2021-01-01"
    assert result["holdout_end"] == "2024-12-31"
    assert result["tuning_after_holdout"] == "FALSE"
    assert all(row["holdout_used_for_tuning"] == "FALSE" for row in report["holdout_integrity_audit"])


def test_tax_outputs_are_proxy_only(tmp_path: Path) -> None:
    report = run_tax_turnover_proxy_run(tmp_path, created_at="2026-06-06T00:00:00Z")

    assert report["not_tax_advice"] is True
    assert report["tax_turnover_proxy"][0]["proxy_label"] == "RESEARCH_ESTIMATE_ONLY_NOT_TAX_ADVICE"
    assert report["classification"] == "DATA_REQUIRED"


def test_retirement_engine_blocks_missing_personal_inputs(tmp_path: Path) -> None:
    report = run_monte_carlo_retirement_comparison(tmp_path, created_at="2026-06-06T00:00:00Z")

    assert report["blocked"] is True
    assert report["classification"] == "DATA_REQUIRED"
    assert len(report["missing_inputs"]) == 8
    assert {row["portfolio"] for row in report["simulation_results"]} == {"Portfolio Atlas", "VTI", "60/40", "Oak Harvest proxy"}


def test_shadow_mode_uses_no_capital(tmp_path: Path) -> None:
    report = run_shadow_portfolio_starter(tmp_path, created_at="2026-06-06T00:00:00Z")

    assert report["uses_real_capital"] is False
    assert report["shadow_initial_state"][0]["actual_followed_status"] == "NOT_FOLLOWED_NO_CAPITAL"
    assert report["classification"] == "DATA_REQUIRED"


def test_replacement_recommendation_forbidden_without_complete_evidence(tmp_path: Path) -> None:
    report = run_oak_harvest_replacement_evidence_matrix(tmp_path, created_at="2026-06-06T00:00:00Z")

    assert report["classification"] == "INSUFFICIENT_EVIDENCE"
    assert report["decision"][0]["replacement_recommendation"] == "FORBIDDEN"
    assert report["decision"][0]["evidence_complete"] == "FALSE"


def test_kill_test_can_reject_atlas(tmp_path: Path) -> None:
    report = run_portfolio_atlas_kill_test_decision_grade(
        tmp_path,
        created_at="2026-06-06T00:00:00Z",
        evidence={"cannot beat VTI": False},
    )

    assert report["classification"] == "PORTFOLIO_ATLAS_REJECTED"
    assert any(row["classification"] == "FAILURE_CONFIRMED" for row in report["failure_modes"])
    assert report["kill_decision"][0]["real_capital_action"] == "FORBIDDEN"


def test_board_vote_is_deterministic(tmp_path: Path) -> None:
    first = run_portfolio_review_board_vote(tmp_path, created_at="2026-06-06T00:00:00Z")
    second = run_portfolio_review_board_vote(tmp_path, created_at="2026-06-06T00:00:00Z")

    assert first["board_votes"] == second["board_votes"]
    assert [row["reviewer"] for row in first["board_votes"]] == ["AQR", "Lopez de Prado", "Dalio", "Buffett", "Swensen"]
    assert {row["vote"] for row in first["board_votes"]} == {"NO_SHADOW_YET"}


def test_no_trading_broker_or_recommendation_authority_emitted(tmp_path: Path) -> None:
    report = run_decision_grade_validation(tmp_path, created_at="2026-06-06T00:00:00Z")

    assert report["campaign"]["classification"] == "DATA_REQUIRED"
    assert report["funding"]["classification"] == "IMPROVE_DATA_FIRST"
    assert report["phase_review"]["classification"] == "IMPROVE_DATA_FIRST"
    assert report["shadow_starter"]["uses_real_capital"] is False

    for dirname in [
        "validation_campaign_003",
        "decision_grade_funding_development_decision",
        "portfolio_atlas_phase_review",
    ]:
        latest = json.loads((tmp_path / dirname / "latest.json").read_text())
        assert latest["authority_boundary"] == AUTHORITY
        text = json.dumps(latest).lower()
        assert "broker execution" in text
        assert "no live portfolio" in text
        assert "no replacement recommendation" in text
