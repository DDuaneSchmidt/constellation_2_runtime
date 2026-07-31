from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.portfolio_relevance_estimate import (
    build_portfolio_relevance_estimate,
    run_portfolio_relevance_estimate,
    write_portfolio_relevance_estimate,
)

NOW = "2026-06-05T00:00:00Z"


def _write_latest(root: Path, name: str, payload: dict) -> None:
    path = root / name / "latest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _family(family_id: str, classification: str = "HIGH_PRIORITY_FAMILY", expectancy: float = 0.004) -> dict:
    return {
        "family_id": family_id,
        "family_name": f"Family {family_id}",
        "classification": classification,
        "average_expectancy": expectancy,
        "candidate_count": 2,
        "duplicate_or_distinct_assessment": "GENUINELY_DISTINCT",
    }


def _candidate(candidate_id: str, family_id: str, expectancy: float = 0.004) -> dict:
    return {
        "candidate_id": candidate_id,
        "family_id": family_id,
        "campaign_rank": 1,
        "mechanism": "BREAKOUT",
        "regime": "CHOP",
        "expectancy": expectancy,
        "sample_size": 120,
        "candidate_symbols": ["SPY", "QQQ"],
    }


def _seed_inputs(root: Path) -> None:
    _write_latest(
        root,
        "candidate_family_discovery",
        {
            "report_type": "CANDIDATE_FAMILY_DISCOVERY",
            "summary": {
                "top_8_family_count": 3,
                "genuinely_distinct_family_count": 4,
                "near_duplicate_group_count": 1,
            },
            "families": [
                _family("family_a"),
                _family("family_b"),
                _family("family_c", "PROMISING_FAMILY", 0.003),
                _family("family_d", "WEAK_FAMILY", 0.001),
            ],
        },
    )
    _write_latest(
        root,
        "focused_observation_campaign",
        {
            "report_type": "FOCUSED_OBSERVATION_CAMPAIGN",
            "summary": {
                "campaign_candidate_count": 3,
                "biggest_remaining_risk": "All selected candidates still depend on SPY daily proxy evidence until candidate-specific data is supplied.",
            },
            "campaign_candidates": [
                _candidate("candidate_a", "family_a"),
                _candidate("candidate_b", "family_b", 0.0035),
                _candidate("candidate_c", "family_c", 0.003),
            ],
        },
    )


def test_portfolio_relevance_estimate_builds_required_scenarios_and_guardrails(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)

    report = build_portfolio_relevance_estimate(root=tmp_path, created_at=NOW)

    assert report["report_type"] == "PORTFOLIO_RELEVANCE_ESTIMATE"
    assert set(report["scenarios"]) == {"conservative", "base", "optimistic"}
    assert report["scenarios"]["base"]["number_of_independent_families"]["estimate"] == 3
    assert report["scenarios"]["base"]["expected_net_edge_after_haircut"]["net_expectancy_proxy"] > 0
    assert report["scenarios"]["base"]["whether_signal_magnitude_could_plausibly_matter"] is False
    assert report["summary"]["missing_required_input_count"] == 2
    assert "No investment advice." in report["guardrails"]
    assert "could_this_eventually_matter_for_portfolio_returns" in report["required_conclusion"]


def test_portfolio_relevance_estimate_writes_latest_and_dated_outputs(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    report = build_portfolio_relevance_estimate(root=tmp_path, created_at=NOW)

    paths = write_portfolio_relevance_estimate(report, root=tmp_path)

    assert paths["json"].name == "portfolio_relevance_estimate_report.json"
    assert paths["summary"].name == "portfolio_relevance_estimate_summary.md"
    assert (tmp_path / "portfolio_relevance_estimate" / "latest.json").exists()
    assert (tmp_path / "portfolio_relevance_estimate" / "latest_summary.md").exists()
    assert "No investment advice" in paths["summary"].read_text(encoding="utf-8")


def test_run_portfolio_relevance_estimate_returns_report_and_writes_files(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)

    report = run_portfolio_relevance_estimate(root=tmp_path, created_at=NOW)

    assert report["summary"]["independent_family_estimate_base"] == 3
    assert (tmp_path / "portfolio_relevance_estimate" / "latest.json").exists()
