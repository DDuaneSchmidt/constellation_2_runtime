from __future__ import annotations

import pytest

import src.factors.candidate_factors as candidate_factors
from src.factors.candidate_factors import candidate_factor_library, factors_by_family, get_candidate_factor, research_bench_summary


EXPECTED_COUNTS = {
    "value": 4,
    "quality": 4,
    "forensic": 5,
    "stability": 3,
    "momentum": 3,
}


def test_candidate_factor_library_contains_yuval_research_families() -> None:
    library = candidate_factor_library()

    assert len(library) == 19
    assert research_bench_summary()["family_counts"] == EXPECTED_COUNTS
    assert {factor["family"] for factor in library} == set(EXPECTED_COUNTS)


def test_every_factor_is_research_only_and_not_ranked() -> None:
    for factor in candidate_factor_library():
        assert factor["activation_status"] == "research_only_not_ranked"
        assert factor["factor_id"]
        assert factor["name"]
        assert factor["required_inputs"]
        assert factor["formula"]
        assert factor["rationale"]

    summary = research_bench_summary()
    assert summary["ranking_activation_allowed"] is False
    assert summary["activation_status"] == "research_only_not_ranked"


def test_expected_factor_names_are_present_by_family() -> None:
    names_by_family = {
        family: {factor["name"] for factor in factors_by_family(family)}
        for family in EXPECTED_COUNTS
    }

    assert names_by_family["value"] == {"FCF Yield", "Earnings Yield", "Shareholder Yield", "EV/EBIT"}
    assert names_by_family["quality"] == {"Gross Profitability", "FCF Margin", "ROIC", "ROA"}
    assert names_by_family["forensic"] == {"Accruals", "Cash Conversion", "Asset Growth", "Receivables Growth", "Inventory Growth"}
    assert names_by_family["stability"] == {"Sales Stability", "EPS Stability", "Margin Stability"}
    assert names_by_family["momentum"] == {"12-1 Momentum", "Industry Momentum", "Sharpe Momentum"}


def test_get_candidate_factor_returns_defensive_copy() -> None:
    factor = get_candidate_factor("value_fcf_yield_v1")
    factor["name"] = "Mutated"

    assert get_candidate_factor("value_fcf_yield_v1")["name"] == "FCF Yield"


def test_unknown_factor_fails_closed() -> None:
    with pytest.raises(ValueError, match="Unknown candidate factor"):
        get_candidate_factor("activated_rank_factor")


def test_no_ranking_or_external_surface_is_exposed() -> None:
    module_source = candidate_factors.__loader__.get_source(candidate_factors.__name__) or ""

    assert "requests" not in module_source
    assert "urllib" not in module_source
    assert "rank_securities" not in module_source
    assert "activate" not in {name.lower() for name in dir(candidate_factors)}

