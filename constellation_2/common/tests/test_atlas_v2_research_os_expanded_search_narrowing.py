from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.expanded_search_narrowing import (
    PROFILE_ID,
    build_expanded_search_narrowing_report,
    write_expanded_search_narrowing_report,
)

NOW = "2026-06-06T00:00:00Z"


def _write_latest(root: Path, name: str, payload: dict) -> None:
    path = root / name / "latest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _seed_inputs(root: Path) -> None:
    _write_latest(
        root,
        "regime_expansion",
        {
            "report_type": "REGIME_EXPANSION",
            "metrics": {
                "hypotheses_by_regime": {"TRENDING": 10, "CHOP": 8, "RANGE": 6},
                "positive_replay_rate_by_regime": {
                    "TRENDING": {"positive_replay_rate": 0.7, "positive_replays": 7, "total_replays": 10},
                    "CHOP": {"positive_replay_rate": 0.45, "positive_replays": 4, "total_replays": 8},
                    "RANGE": {"positive_replay_rate": 0.0, "positive_replays": 0, "total_replays": 6},
                },
                "eligible_candidates_by_regime": {"TRENDING": 4, "CHOP": 1, "RANGE": 0},
            },
        },
    )
    _write_latest(
        root,
        "market_structure_expansion",
        {
            "report_type": "MARKET_STRUCTURE_EXPANSION",
            "required_metrics": {
                "hypotheses_by_structure": {"COMPRESSION": 8, "GAP_UP": 3},
                "positive_replay_rate_by_structure": {"COMPRESSION": 0.75, "GAP_UP": 0.0},
                "eligible_candidates_by_structure": {"COMPRESSION": 3, "GAP_UP": 0},
                "top_structures_among_failures": [{"market_structure": "GAP_UP", "count": 5}],
            },
        },
    )
    _write_latest(
        root,
        "session_context_expansion",
        {
            "report_type": "SESSION_CONTEXT_EXPANSION",
            "session_contexts": ["OPEN", "MIDDAY"],
            "metrics": {
                "hypotheses_by_session": {"OPEN": 5, "MIDDAY": 4},
                "eligible_candidates_by_session": {"OPEN": 0, "MIDDAY": 0},
            },
        },
    )
    _write_latest(
        root,
        "expanded_search_trial",
        {
            "report_type": "EXPANDED_SEARCH_TRIAL",
            "comparison_against_prior_baseline": {
                "expansion_increased_diversity": True,
                "new_backtest_supported_candidates": 12,
                "quality_assessment": "MIXED_DATA_BLOCKED",
            },
            "final_qualification": {
                "backtest_supported_examples": [
                    {"candidate_id": "c1", "mechanism": "BREAKOUT", "regime": "TRENDING", "replay_score": 0.6, "backtest_classification": "BACKTEST_SUPPORTED"},
                    {"candidate_id": "c2", "mechanism": "BREAKOUT", "regime": "CHOP", "replay_score": 0.4, "backtest_classification": "BACKTEST_SUPPORTED"},
                ],
                "eligible_examples": [
                    {"candidate_id": "c3", "mechanism": "BREAKOUT", "regime": "TRENDING", "replay_score": 0.7, "source_types": ["journal_extract"], "timeframes": ["30m"], "symbols": ["SPY"]},
                ],
            },
        },
    )
    _write_latest(
        root,
        "winner_pattern_extraction",
        {
            "report_type": "WINNER_PATTERN_EXTRACTION",
            "top_20_candidates": [
                {
                    "candidate_id": "w1",
                    "mechanism": "BREAKOUT",
                    "regime": "TRENDING",
                    "replay_score": 0.7,
                    "profit_factor": 2.0,
                    "source_types": ["journal_extract"],
                    "timeframes": ["30m"],
                    "symbols": ["SPY", "QQQ"],
                }
            ],
            "top_8_campaign_candidates": [],
        },
    )
    _write_latest(
        root,
        "candidate_failure_patterns",
        {
            "report_type": "CANDIDATE_FAILURE_PATTERNS",
            "aggregate_main_disqualification_reasons": {"backtest classification INSUFFICIENT_DATA": 8},
            "preview_rejected_distributions": {
                "mechanisms": {"MEAN_REVERSION": 3},
                "regimes": {"RANGE": 5},
                "timeframes": {"5m": 4},
                "source_types": {"scanner_snapshot": 4},
            },
        },
    )
    _write_latest(
        root,
        "family_robustness_review",
        {
            "report_type": "FAMILY_ROBUSTNESS_REVIEW",
            "family_reviews": [
                {
                    "family_id": "f1",
                    "family_name": "BREAKOUT / TRENDING / 30M / JOURNAL_EXTRACT",
                    "classification": "ROBUST_ENOUGH_TO_OBSERVE",
                    "timeframe_concentration": {"timeframes": ["30m"]},
                    "source_type_dependence": {"source_types": ["journal_extract"]},
                    "symbol_universe_concentration": {"symbols": ["SPY", "QQQ"]},
                },
                {
                    "family_id": "f2",
                    "family_name": "MEAN_REVERSION / RANGE / 5M / SCANNER_SNAPSHOT",
                    "classification": "FRAGILE",
                    "timeframe_concentration": {"timeframes": ["5m"]},
                    "source_type_dependence": {"source_types": ["scanner_snapshot"]},
                    "symbol_universe_concentration": {"symbols": ["IWM"]},
                },
            ],
        },
    )


def test_expanded_search_narrowing_builds_dimension_recommendations(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)

    report = build_expanded_search_narrowing_report(root=tmp_path, created_at=NOW)
    rows = {(row["dimension"], row["value"]): row for row in report["dimension_reviews"]}

    assert report["report_type"] == "EXPANDED_SEARCH_NARROWING_RECOMMENDATION"
    assert rows[("regime", "TRENDING")]["recommendation"] == "KEEP"
    assert rows[("market_structure", "COMPRESSION")]["recommendation"] == "KEEP"
    assert rows[("regime", "RANGE")]["recommendation"] in {"NEEDS_DATA", "PAUSE", "REJECT"}
    assert rows[("mechanism", "BREAKOUT")]["robust_family_rate"] == 1.0
    assert report["recommended_next_search_profile"]["profile_id"] == PROFILE_ID
    assert "TRENDING" in report["recommended_next_search_profile"]["dimensions"]["regime"]
    assert "COMPRESSION" in report["recommended_next_search_profile"]["dimensions"]["market_structure"]
    assert report["summary"]["expanded_search_remains_useful"] is True
    assert report["authority_boundary"]["trade_recommendation_authorized"] is False
    assert report["authority_boundary"]["broker_execution_authorized"] is False


def test_expanded_search_narrowing_writes_requested_outputs(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    report = build_expanded_search_narrowing_report(root=tmp_path, created_at=NOW)

    paths = write_expanded_search_narrowing_report(report, root=tmp_path)

    for path in paths.values():
        assert path.exists()
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["recommended_next_search_profile"]["profile_id"] == PROFILE_ID
    assert paths["json"].name == "expanded_search_narrowing_report.json"
    assert paths["summary"].name == "expanded_search_narrowing_summary.md"
    assert "Expanded Search Narrowing Recommendation" in paths["latest_summary"].read_text(encoding="utf-8")
