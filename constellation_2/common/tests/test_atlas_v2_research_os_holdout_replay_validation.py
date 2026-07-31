from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.holdout_replay_validation import (
    build_holdout_replay_validation_report,
    write_holdout_replay_validation_report,
)

NOW = "2026-06-06T00:00:00Z"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_required_reports(root: Path) -> None:
    _write_json(root / "search_overfit_guardrail" / "latest.json", {"report_type": "SEARCH_OVERFIT_MULTIPLE_TESTING_GUARDRAIL"})
    _write_json(
        root / "candidate_family_discovery" / "latest.json",
        {
            "report_type": "CANDIDATE_FAMILY_DISCOVERY",
            "candidate_families": [
                {
                    "family_id": "family-a",
                    "family_name": "Breakout chop 30m",
                    "best_rank": 1,
                    "classification": "HIGH_PRIORITY_FAMILY",
                    "dominant_mechanism": "BREAKOUT",
                    "dominant_regime": "CHOP",
                    "dominant_timeframe": "30M",
                    "best_candidate_id": "cand-a",
                    "candidate_ids": ["cand-a"],
                },
                {
                    "family_id": "family-b",
                    "family_name": "Mean reversion trend 30m",
                    "best_rank": 2,
                    "classification": "PROMISING_FAMILY",
                    "dominant_mechanism": "MEAN_REVERSION",
                    "dominant_regime": "TRENDING",
                    "dominant_timeframe": "30M",
                    "best_candidate_id": "cand-b",
                    "candidate_ids": ["cand-b"],
                },
            ],
        },
    )
    _write_json(
        root / "family_robustness_review" / "latest.json",
        {
            "report_type": "family_robustness_review",
            "family_reviews": [
                {
                    "family_id": "family-a",
                    "family_name": "Breakout chop 30m",
                    "best_rank": 1,
                    "classification": "ROBUST_ENOUGH_TO_OBSERVE",
                    "mechanism": "BREAKOUT",
                    "regime": "CHOP",
                    "timeframes": ["30M"],
                    "top_candidate_ids": ["cand-a"],
                    "proxy_data_dependence": {"requires_direct_data_validation": False},
                },
                {
                    "family_id": "family-b",
                    "family_name": "Mean reversion trend 30m",
                    "best_rank": 2,
                    "classification": "PROMISING_BUT_DATA_BLOCKED",
                    "mechanism": "MEAN_REVERSION",
                    "regime": "TRENDING",
                    "timeframes": ["30M"],
                    "top_candidate_ids": ["cand-b"],
                    "proxy_data_dependence": {"requires_direct_data_validation": True},
                },
            ],
        },
    )
    _write_json(
        root / "focused_observation_campaign" / "latest.json",
        {"report_type": "FOCUSED_OBSERVATION_CAMPAIGN", "campaign_candidates": [{"candidate_id": "cand-a"}, {"candidate_id": "cand-b"}]},
    )
    _write_json(root / "backtest_aware_final_qualification" / "latest.json", {"report_type": "BACKTEST_AWARE_FINAL_QUALIFICATION"})
    _write_json(root / "final_candidate_ranking" / "latest.json", {"report_type": "FINAL_CANDIDATE_RANKING"})


def test_holdout_replay_validation_survives_when_event_level_holdout_survives(tmp_path: Path) -> None:
    _seed_required_reports(tmp_path)
    returns = [0.02, -0.005, 0.015, 0.01, -0.004, 0.012, 0.005, 0.011, -0.003, 0.009]
    rows = [
        {
            "date": f"2025-01-{index + 1:02d}",
            "family_id": "family-a",
            "candidate_id": "cand-a",
            "mechanism": "BREAKOUT",
            "regime": "CHOP",
            "timeframe": "30M",
            "return_observed": value,
        }
        for index, value in enumerate(returns)
    ]
    _write_json(tmp_path / "holdout_replay_events" / "latest.json", {"holdout_events": rows})

    report = build_holdout_replay_validation_report(root=tmp_path, created_at=NOW)

    family_a = next(row for row in report["family_validations"] if row["family_id"] == "family-a")
    family_b = next(row for row in report["family_validations"] if row["family_id"] == "family-b")
    assert family_a["classification"] == "HOLDOUT_SURVIVED"
    assert family_a["holdout_survived"] is True
    assert family_a["discovery_sample_size"] == 7
    assert family_a["holdout_sample_size"] == 3
    assert family_a["confidence_impact"] == "INCREASE_FROM_HOLDOUT_ONLY"
    assert family_b["classification"] == "DATA_BLOCKED"
    assert report["summary"]["families_survived_holdout"] == ["family-a"]
    assert report["required_conclusion"]["did_any_family_survive_holdout"] is True
    assert report["authority_boundary"]["broker_execution_authorized"] is False


def test_holdout_replay_validation_writes_outputs_and_blocks_aggregate_only_data(tmp_path: Path) -> None:
    _seed_required_reports(tmp_path)

    report = build_holdout_replay_validation_report(root=tmp_path, created_at=NOW)
    paths = write_holdout_replay_validation_report(report, root=tmp_path)

    assert report["summary"]["classification_counts"]["DATA_BLOCKED"] == 2
    assert report["required_conclusion"]["did_any_family_survive_holdout"] is False
    assert report["data_availability"]["aggregate_backtest_rows_treated_as_holdout"] is False
    assert paths["json"].name == "holdout_replay_validation_report.json"
    assert paths["summary"].name == "holdout_replay_validation_summary.md"
    assert (tmp_path / "holdout_replay_validation" / "latest.json").exists()
    assert (tmp_path / "holdout_replay_validation" / "latest_summary.md").exists()
