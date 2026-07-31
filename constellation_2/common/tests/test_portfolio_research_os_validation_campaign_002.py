from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.portfolio_research_os.validation_campaign_002 import (
    REQUIRED_RETIREMENT_INPUTS,
    run_monte_carlo_retirement_engine,
    run_oos_holdout_validation,
    run_portfolio_atlas_kill_test,
    run_tax_turnover_proxy,
    run_validation_campaign_002,
    run_walk_forward_portfolio_validation,
    run_weekly_review_package,
)


def test_walk_forward_deterministic(tmp_path: Path) -> None:
    first = run_walk_forward_portfolio_validation(tmp_path, created_at="2026-06-06T00:00:00Z")
    second = run_walk_forward_portfolio_validation(tmp_path, created_at="2026-06-06T00:00:00Z")

    assert first["windows"] == second["windows"]
    assert first["classification"] == "DATA_REQUIRED"
    assert {"train_start", "train_end", "validation_start", "validation_end", "test_start", "test_end", "rebalance_frequency", "embargo_days"} <= set(first["windows"][0])
    assert (tmp_path / "walk_forward_portfolio_validation" / "walk_forward_windows.csv").exists()


def test_holdout_does_not_tune_after_holdout(tmp_path: Path) -> None:
    report = run_oos_holdout_validation(tmp_path, created_at="2026-06-06T00:00:00Z")

    row = report["holdout_results"][0]
    assert row["train_start"] == "2010-01-01"
    assert row["validation_start"] == "2017-01-01"
    assert row["holdout_start"] == "2021-01-01"
    assert row["tuning_after_holdout"] == "FALSE"
    assert all(item["holdout_used_for_tuning"] == "FALSE" for item in report["holdout_assumption_audit"])


def test_tax_output_clearly_labeled_proxy(tmp_path: Path) -> None:
    report = run_tax_turnover_proxy(tmp_path, created_at="2026-06-06T00:00:00Z")

    assert report["not_tax_advice"] is True
    assert report["turnover_tax_proxy"][0]["proxy_label"] == "RESEARCH_ESTIMATE_ONLY_NOT_TAX_ADVICE"
    summary = (tmp_path / "tax_turnover_proxy" / "latest_summary.md").read_text()
    assert "Research-only" in summary


def test_retirement_engine_blocks_if_inputs_missing(tmp_path: Path) -> None:
    report = run_monte_carlo_retirement_engine(tmp_path, created_at="2026-06-06T00:00:00Z")

    assert report["blocked"] is True
    assert sorted(report["missing_inputs"]) == sorted(REQUIRED_RETIREMENT_INPUTS)
    assert {row["portfolio"] for row in report["simulation_results"]} == {"Portfolio Atlas", "VTI", "60/40", "Oak Harvest proxy"}
    assert all(row["classification"] == "BLOCKED_INPUTS_MISSING" for row in report["simulation_results"])


def test_weekly_package_is_research_only(tmp_path: Path) -> None:
    run_weekly_review_package(tmp_path, created_at="2026-06-06T00:00:00Z")

    sample = (tmp_path / "weekly_review_package" / "weekly_review_sample.md").read_text()
    assert "Research-only" in sample
    assert "not a recommendation" in sample
    assert "No trade instruction" in sample


def test_kill_test_can_reject_atlas(tmp_path: Path) -> None:
    report = run_portfolio_atlas_kill_test(
        tmp_path,
        created_at="2026-06-06T00:00:00Z",
        evidence={
            "atlas_beats_vti": False,
            "atlas_beats_oak_harvest_proxy": True,
            "tax_drag_advantage_survives": True,
            "turnover_acceptable": True,
            "drawdown_acceptable": True,
            "complexity_justified": True,
            "bias_controlled": True,
        },
    )

    assert report["classification"] == "PORTFOLIO_ATLAS_REJECTED"
    assert any(row["classification"] == "FAILURE_CONFIRMED" for row in report["failure_modes"])
    assert report["kill_decision"][0]["real_capital_recommendation"] == "FORBIDDEN"


def test_campaign_outputs_and_funding_do_not_recommend_real_capital(tmp_path: Path) -> None:
    report = run_validation_campaign_002(tmp_path, created_at="2026-06-06T00:00:00Z")

    assert report["validation_campaign_002"]["classification"] == "DATA_REQUIRED"
    assert report["funding_decision"]["classification"] == "IMPROVE_DATA_FIRST"
    assert report["funding_decision"]["funding_decision"][0]["real_capital_recommendation"] == "FORBIDDEN"

    for dirname in [
        "walk_forward_portfolio_validation",
        "oos_holdout_validation",
        "regime_performance_analysis",
        "tax_turnover_proxy",
        "behavioral_stress_test",
        "retirement_simulation_inputs",
        "monte_carlo_retirement_engine",
        "oak_harvest_replacement_standard",
        "shadow_portfolio_design",
        "weekly_review_package",
        "portfolio_explainability",
        "complexity_benefit_review",
        "portfolio_atlas_kill_test",
        "validation_campaign_002",
        "implementation_funding_decision",
    ]:
        assert (tmp_path / dirname / "latest.json").exists()
        json.loads((tmp_path / dirname / "latest.json").read_text())

    rows = _read_csv(tmp_path / "implementation_funding_decision" / "funding_decision.csv")
    assert rows[0]["real_capital_recommendation"] == "FORBIDDEN"


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))
