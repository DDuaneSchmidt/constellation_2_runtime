from __future__ import annotations

import pytest

from src.factors.forensic import compare_forensic_quality, distinct_family_assessment, forensic_quality_score


def _clean_current() -> dict[str, float | str]:
    return {
        "ticker": "CLEAN",
        "net_income": 100.0,
        "operating_cash_flow": 128.0,
        "free_cash_flow": 92.0,
        "total_assets": 1000.0,
        "receivables": 92.0,
        "inventory": 105.0,
        "revenue": 1200.0,
        "gross_margin": 0.42,
        "current_assets": 420.0,
        "ppe": 280.0,
        "shares_outstanding": 100.0,
    }


def _clean_previous() -> dict[str, float | str]:
    return {
        "ticker": "CLEAN",
        "net_income": 88.0,
        "operating_cash_flow": 105.0,
        "free_cash_flow": 78.0,
        "total_assets": 960.0,
        "receivables": 88.0,
        "inventory": 100.0,
        "revenue": 1120.0,
        "gross_margin": 0.41,
        "current_assets": 395.0,
        "ppe": 275.0,
        "shares_outstanding": 100.0,
    }


def _risky_current() -> dict[str, float | str]:
    return {
        "ticker": "RISKY",
        "net_income": 100.0,
        "operating_cash_flow": 25.0,
        "free_cash_flow": -10.0,
        "total_assets": 1550.0,
        "receivables": 240.0,
        "inventory": 220.0,
        "revenue": 1210.0,
        "gross_margin": 0.31,
        "current_assets": 500.0,
        "ppe": 260.0,
        "shares_outstanding": 124.0,
    }


def test_forensic_score_rewards_cash_conversion_and_low_accruals() -> None:
    result = forensic_quality_score(_clean_current(), _clean_previous())

    assert result["ticker"] == "CLEAN"
    assert result["factor_family"] == "forensic_quality"
    assert result["forensic_quality_score"] >= 80.0
    assert result["component_scores"]["accrual_ratio"] == 100.0
    assert result["warnings"] == ()
    assert "clean forensic profile" in result["interpretation"]


def test_forensic_score_penalizes_accrual_growth_dilution_and_cash_shortfall() -> None:
    result = forensic_quality_score(_risky_current(), _clean_previous())

    assert result["forensic_quality_score"] < 45.0
    assert result["component_scores"]["ocf_to_net_income"] < 20.0
    assert result["component_scores"]["dilution_rate"] == 0.0
    assert "elevated forensic risk profile" in result["interpretation"]


def test_compare_reports_universe_median_and_crazy_returns_europe_spread() -> None:
    comparison = compare_forensic_quality(
        [_clean_current(), _risky_current()],
        [_clean_current()],
        previous_universe_rows=[_clean_previous(), {**_clean_previous(), "ticker": "RISKY"}],
        previous_crazy_returns_europe_rows=[_clean_previous()],
    )

    assert comparison["universe_median"] is not None
    assert comparison["crazy_returns_europe_median"] is not None
    assert comparison["crazy_returns_europe_vs_universe_spread"] > 0.0
    assert comparison["distinct_family_assessment"]["verdict"] == "distinct_but_adjacent_to_quality"


def test_missing_core_inputs_fail_closed() -> None:
    current = _clean_current()
    del current["operating_cash_flow"]

    with pytest.raises(ValueError, match="operating_cash_flow"):
        forensic_quality_score(current, _clean_previous())


def test_distinct_family_assessment_requires_incremental_validation() -> None:
    assessment = distinct_family_assessment()

    assert assessment["verdict"] == "distinct_but_adjacent_to_quality"
    assert "incremental rank information" in assessment["validation_requirement"]

