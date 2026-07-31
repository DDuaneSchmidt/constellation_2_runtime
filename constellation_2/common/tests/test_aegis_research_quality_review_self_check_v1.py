from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.research_quality_review_self_check_v1 import (
    build_research_quality_review_self_check_v1,
    write_research_quality_review_self_check_v1,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_truth(truth_root: Path, day: str) -> None:
    _write_json(
        truth_root / "reports" / "aegis_research_portfolio_v1" / day / "research_portfolio.v1.json",
        {
            "theses": [{"thesis_id": "THESIS_ALPHA_V1"}],
            "hypotheses": [{"hypothesis_id": "HYP_ALPHA_V1"}],
        },
    )
    _write_json(
        truth_root / "reports" / "aegis_research_capital_allocation_v1" / day / "research_capital_allocation.v1.json",
        {"programs": [{"research_program_id": "PROGRAM_ALPHA_V1"}]},
    )


def _seed_docs(repo_root: Path, *, include_hypothesis: bool = True) -> None:
    docs = repo_root / "docs"
    docs.mkdir(parents=True, exist_ok=True)
    hypothesis_text = "HYP_ALPHA_V1" if include_hypothesis else "HYP_MISSING_REVIEW"
    (docs / "aegis_thesis_hypothesis_quality_review_v1.md").write_text(
        f"# Quality Review\n\nTHESIS_ALPHA_V1\n{hypothesis_text}\n", encoding="utf-8"
    )
    (docs / "aegis_inactive_sleeve_explanation_review_v1.md").write_text(
        "\n".join(
            [
                "# Inactive Sleeves",
                "C2_DEFENSIVE_TAIL_V1",
                "C2_EVENT_DISLOCATION_V1",
                "C2_MARKET_NEUTRAL_SPREAD_V1",
                "C2_MEAN_REVERSION_EQ_V1",
                "C2_VOL_INCOME_DEFINED_RISK_V1",
            ]
        ),
        encoding="utf-8",
    )
    (docs / "aegis_retirement_criteria_policy_v1.md").write_text(
        "Hypothesis Retirement Criteria\nSleeve Retirement Criteria\nResearch Program Retirement Criteria\nmanual_review_required_before_retire",
        encoding="utf-8",
    )
    (docs / "aegis_research_program_grouping_review_v1.md").write_text(
        "PROGRAM_ALPHA_V1", encoding="utf-8"
    )
    (docs / "aegis_regime_tag_taxonomy_v1.md").write_text(
        " ".join(
            [
                "RISK_ON",
                "RISK_OFF",
                "LOW_VOL",
                "HIGH_VOL",
                "RATES_RISING",
                "DOLLAR_STRENGTH",
                "BROAD_PARTICIPATION",
                "CROSS_ASSET_CONFIRMATION",
                "EARNINGS_SEASON",
                "OPTIONS_EXPIRATION_WEEK",
            ]
        ),
        encoding="utf-8",
    )


def test_research_quality_review_self_check_passes_for_complete_review(tmp_path: Path) -> None:
    day = "2026-06-01"
    repo_root = tmp_path / "repo"
    truth_root = tmp_path / "truth"
    _seed_truth(truth_root, day)
    _seed_docs(repo_root)

    payload = build_research_quality_review_self_check_v1(
        repo_root=repo_root, truth_root=truth_root, day_utc=day
    )

    assert payload["ok"] is True
    assert payload["failure_count"] == 0
    assert payload["summary"]["hypotheses_expected"] == 1

    output_path = write_research_quality_review_self_check_v1(
        repo_root=repo_root, truth_root=truth_root, day_utc=day, payload=payload
    )
    assert output_path.exists()


def test_research_quality_review_self_check_fails_for_unreviewed_hypothesis(tmp_path: Path) -> None:
    day = "2026-06-01"
    repo_root = tmp_path / "repo"
    truth_root = tmp_path / "truth"
    _seed_truth(truth_root, day)
    _seed_docs(repo_root, include_hypothesis=False)

    payload = build_research_quality_review_self_check_v1(
        repo_root=repo_root, truth_root=truth_root, day_utc=day
    )

    assert payload["ok"] is False
    assert {failure["failure_code"] for failure in payload["failures"]} == {"HYPOTHESIS_NOT_REVIEWED"}
