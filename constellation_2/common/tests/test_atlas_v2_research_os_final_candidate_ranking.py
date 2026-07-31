from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.final_candidate_ranking import (
    build_final_candidate_ranking_report,
    build_focused_observation_campaign_report,
    write_final_candidate_ranking_report,
    write_focused_observation_campaign_report,
    _rankable_candidate,
)


NOW = "2026-06-05T00:00:00Z"


def trial_row(
    *,
    candidate_id: str = "ptc_test",
    mechanism: str = "BREAKOUT",
    sample_size: int = 90,
    max_drawdown: float = -0.08,
    classification: str = "BACKTEST_SUPPORTED",
    final_score: float = 0.715,
    proxy_penalty: float = 0.025,
    mismatch_penalty: float = 0.0,
    missing_penalty: float = 0.0,
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "mechanism": mechanism,
        "regime": "CHOP",
        "final_qualification": {
            "eligible": True,
            "final_score": final_score,
            "preliminary_edge_score": 0.685,
            "backtest_evidence_score": 0.7,
            "penalties": {
                "proxy_data_penalty": proxy_penalty,
                "intraday_daily_mismatch_penalty": mismatch_penalty,
                "missing_evidence_penalty": missing_penalty,
                "sample_size_penalty": 0.0 if sample_size >= 30 else 0.01,
            },
            "disqualification_reasons": [],
        },
        "candidate_backtest": {
            "classification": classification,
            "metrics": {
                "sample_size": sample_size,
                "expectancy": 0.012,
                "profit_factor": 1.9,
                "max_drawdown": max_drawdown,
                "replay_backtest_consistency": 0.8,
            },
            "warnings": [],
            "missing_data": [],
        },
    }


def test_rankable_candidate_ready_when_supported_and_filters_pass() -> None:
    row = _rankable_candidate(trial_row())

    assert row["classification"] == "READY_FOR_PAPER_FORWARD_OBSERVATION"
    assert row["recommended_human_action"] == "APPROVE_FOR_PAPER_FORWARD_OBSERVATION"
    assert row["authority_boundary"]["live_trading_authorized"] is False
    assert row["authority_boundary"]["position_sizing_authorized"] is False


def test_rankable_candidate_blocks_low_sample_missing_and_intraday_mismatch() -> None:
    low_sample = _rankable_candidate(trial_row(sample_size=12))
    missing = _rankable_candidate(trial_row(missing_penalty=0.015))
    intraday = _rankable_candidate(trial_row(mechanism="VWAP_OR_AVERAGE_RECLAIM", mismatch_penalty=0.015))

    assert low_sample["classification"] == "NEEDS_DATA_IMPROVEMENT"
    assert missing["classification"] == "NEEDS_DATA_IMPROVEMENT"
    assert intraday["classification"] == "NEEDS_DATA_IMPROVEMENT"


def test_rankable_candidate_rejects_unsupported_and_fragile_backtest() -> None:
    unsupported = _rankable_candidate(trial_row(classification="BACKTEST_WEAK"))
    fragile = _rankable_candidate(trial_row(max_drawdown=-0.28))

    assert unsupported["classification"] == "REJECT_FOR_NOW"
    assert fragile["classification"] == "TOO_FRAGILE"


def test_final_candidate_ranking_report_writes_latest_files(tmp_path: Path) -> None:
    report = build_final_candidate_ranking_report(tmp_path, observation_count=200, created_at=NOW)
    paths = write_final_candidate_ranking_report(report, root=tmp_path)

    assert paths["latest_json"].exists()
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["report_type"] == "FINAL_CANDIDATE_RANKING"
    assert payload["authority_boundary"]["broker_execution_authorized"] is False
    assert payload["authority_boundary"]["capital_authorized"] is False


def test_focused_campaign_report_is_observation_only(tmp_path: Path) -> None:
    ranking = build_final_candidate_ranking_report(tmp_path, observation_count=200, created_at=NOW)
    campaign = build_focused_observation_campaign_report(ranking, created_at=NOW)
    paths = write_focused_observation_campaign_report(campaign, root=tmp_path)

    assert paths["latest_json"].exists()
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["report_type"] == "FOCUSED_OBSERVATION_CAMPAIGN"
    assert payload["authority_boundary"]["paper_forward_observation_only"] is True
    assert payload["authority_boundary"]["automatic_paper_trade_placement_authorized"] is False
