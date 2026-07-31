from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.cli import main
from constellation_2.common.atlas_v2_research_os.holdout_aware_family_ranking import (
    build_holdout_aware_family_ranking,
    run_holdout_aware_family_ranking,
    write_holdout_aware_family_ranking,
)

NOW = "2026-06-05T00:00:00Z"


def _write_latest(root: Path, name: str, payload: dict) -> None:
    path = root / name / "latest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _family(family_id: str, rank: int, *, direct_status: str = "mostly_insufficient", duplicate: str = "GENUINELY_DISTINCT") -> dict:
    return {
        "family_id": family_id,
        "family_name": f"Family {family_id}",
        "best_rank": rank,
        "classification": "HIGH_PRIORITY_FAMILY",
        "average_expectancy": 0.004,
        "average_profit_factor": 1.8,
        "candidate_count": 2,
        "direct_validation_status": direct_status,
        "duplicate_or_distinct_assessment": duplicate,
    }


def _seed_inputs(root: Path, *, include_holdout: bool = False) -> None:
    families = [
        _family("family_a", 1, direct_status="confirmed"),
        _family("family_b", 2, direct_status="confirmed"),
        _family("family_c", 3, duplicate="REPEATED_FAMILY_VARIANTS"),
    ]
    _write_latest(root, "candidate_family_discovery", {"report_type": "CANDIDATE_FAMILY_DISCOVERY", "families": families})
    _write_latest(
        root,
        "family_robustness_review",
        {
            "report_type": "family_robustness_review",
            "family_reviews": [
                {
                    "family_id": row["family_id"],
                    "family_name": row["family_name"],
                    "best_rank": row["best_rank"],
                    "duplicate_risk": {"level": row["duplicate_or_distinct_assessment"]},
                    "proxy_data_dependence": {"level": "LOW", "requires_direct_data_validation": False},
                    "sample_size_adequacy": {"summary": {"average": 120}},
                }
                for row in families
            ],
        },
    )
    _write_latest(
        root,
        "edge_magnitude_estimation",
        {
            "report_type": "EDGE_MAGNITUDE_ESTIMATION",
            "families": [
                {
                    "family_id": "family_a",
                    "family_name": "Family family_a",
                    "direct_validation_status": "confirmed",
                    "average_expectancy": 0.004,
                    "profit_factor": 1.8,
                    "sample_size": 120,
                    "proxy_dependence": "LOW",
                },
                {
                    "family_id": "family_b",
                    "family_name": "Family family_b",
                    "direct_validation_status": "confirmed",
                    "average_expectancy": 0.004,
                    "profit_factor": 1.8,
                    "sample_size": 120,
                    "proxy_dependence": "LOW",
                },
                {
                    "family_id": "family_c",
                    "family_name": "Family family_c",
                    "direct_validation_status": "mostly_insufficient",
                    "average_expectancy": 0.004,
                    "profit_factor": 1.8,
                    "sample_size": 120,
                    "proxy_dependence": "HIGH",
                },
            ],
        },
    )
    _write_latest(
        root,
        "search_overfit_guardrail",
        {
            "report_type": "SEARCH_OVERFIT_GUARDRAIL",
            "summary": {"confidence_increase_from_search_selection_allowed": False},
            "candidate_family_reviews": [
                {"family_id": "family_a", "family_name": "Family family_a", "proxy_data_risk": "LOW", "duplicate_family_risk": "LOW"},
                {"family_id": "family_b", "family_name": "Family family_b", "proxy_data_risk": "LOW", "duplicate_family_risk": "LOW"},
                {"family_id": "family_c", "family_name": "Family family_c", "proxy_data_risk": "HIGH", "duplicate_family_risk": "HIGH"},
            ],
        },
    )
    _write_latest(root, "portfolio_relevance_estimate", {"report_type": "PORTFOLIO_RELEVANCE_ESTIMATE", "required_conclusion": {}})
    if include_holdout:
        _write_latest(
            root,
            "holdout_replay_validation",
            {
                "report_type": "HOLDOUT_REPLAY_VALIDATION",
                "family_holdout_results": [
                    {"family_id": "family_a", "classification": "HOLDOUT_WEAKENED", "holdout_expectancy": 0.002, "holdout_profit_factor": 1.2},
                    {"family_id": "family_b", "classification": "HOLDOUT_SURVIVED", "holdout_expectancy": 0.005, "holdout_profit_factor": 2.0},
                    {"family_id": "family_c", "classification": "HOLDOUT_FAILED", "holdout_expectancy": -0.001, "holdout_profit_factor": 0.8},
                ],
            },
        )


def test_missing_holdout_blocks_all_families_from_observation_priority(tmp_path: Path) -> None:
    _seed_inputs(tmp_path, include_holdout=False)

    report = build_holdout_aware_family_ranking(root=tmp_path, created_at=NOW)

    assert report["summary"]["families_ranked"] == 3
    assert report["summary"]["families_blocked"] == 3
    assert report["summary"]["families_upgraded"] == 0
    assert report["required_conclusion"]["which_families_remain_worth_observing"] == []
    assert {row["holdout_status"] for row in report["ranked_families"]} == {"INSUFFICIENT_HOLDOUT_DATA"}
    assert all(row["new_rank"] == "BLOCKED" for row in report["ranked_families"])


def test_holdout_survived_can_improve_weakened_cannot_and_failed_downgrades(tmp_path: Path) -> None:
    _seed_inputs(tmp_path, include_holdout=True)

    report = build_holdout_aware_family_ranking(root=tmp_path, created_at=NOW)
    by_id = {row["family_id"]: row for row in report["ranked_families"]}

    assert by_id["family_b"]["new_rank"] == 1
    assert by_id["family_b"]["rank_direction"] == "UPGRADED"
    assert by_id["family_a"]["new_rank"] >= by_id["family_a"]["old_rank"]
    assert by_id["family_a"]["rank_direction"] != "UPGRADED"
    assert by_id["family_c"]["new_rank"] == "BLOCKED"
    assert by_id["family_c"]["rank_direction"] == "BLOCKED"
    assert report["summary"]["confidence_increase_from_search_selection_allowed"] is False


def test_holdout_summary_status_lists_mark_data_blocked_families(tmp_path: Path) -> None:
    _seed_inputs(tmp_path, include_holdout=False)
    _write_latest(
        tmp_path,
        "holdout_replay_validation",
        {
            "report_type": "HOLDOUT_REPLAY_VALIDATION",
            "summary": {
                "families_data_blocked": ["family_a"],
                "families_failed": [],
                "families_insufficient_holdout_data": [],
                "families_survived_holdout": [],
                "families_weakened": [],
            },
        },
    )

    report = build_holdout_aware_family_ranking(root=tmp_path, created_at=NOW)
    by_id = {row["family_id"]: row for row in report["ranked_families"]}

    assert by_id["family_a"]["holdout_status"] == "DATA_BLOCKED"
    assert by_id["family_a"]["new_rank"] == "BLOCKED"
    assert by_id["family_b"]["holdout_status"] == "INSUFFICIENT_HOLDOUT_DATA"


def test_holdout_aware_family_ranking_writes_latest_and_dated_outputs(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    report = build_holdout_aware_family_ranking(root=tmp_path, created_at=NOW)

    paths = write_holdout_aware_family_ranking(report, root=tmp_path)

    assert paths["json"].name == "holdout_aware_family_ranking_report.json"
    assert paths["summary"].name == "holdout_aware_family_ranking_summary.md"
    assert (tmp_path / "holdout_aware_family_ranking" / "latest.json").exists()
    assert "No confidence increase from search selection alone" in paths["summary"].read_text(encoding="utf-8")


def test_holdout_aware_family_ranking_cli_writes_report(tmp_path: Path, capsys) -> None:
    _seed_inputs(tmp_path)

    result = main(["--root", str(tmp_path), "--holdout-aware-family-ranking"])

    assert result == 0
    assert (tmp_path / "holdout_aware_family_ranking" / "latest.json").exists()
    output = json.loads(capsys.readouterr().out)
    assert output["summary"]["families_ranked"] == 3


def test_run_holdout_aware_family_ranking_returns_report(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)

    report = run_holdout_aware_family_ranking(root=tmp_path, created_at=NOW)

    assert report["report_type"] == "HOLDOUT_AWARE_FAMILY_RANKING"
    assert (tmp_path / "holdout_aware_family_ranking" / "latest_summary.md").exists()
