from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.candidate_family_discovery import (
    build_candidate_family_discovery_report,
    run_candidate_family_discovery,
    write_candidate_family_discovery_report,
)
from constellation_2.common.atlas_v2_research_os.cli import main


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _candidate(candidate_id: str, rank: int, *, mechanism: str, timeframe: str, symbols: list[str], market_structure: str = "COMPRESSION", session_context: str = "OPEN", classification: str = "READY_FOR_PAPER_FORWARD_OBSERVATION") -> dict:
    return {
        "candidate_id": candidate_id,
        "rank": rank,
        "classification": classification,
        "mechanism": mechanism,
        "regime": "CHOP",
        "candidate_timeframes": [timeframe],
        "candidate_source_types": ["journal_extract"],
        "candidate_market_structures": [market_structure],
        "candidate_session_context": session_context,
        "candidate_universe_symbols": symbols,
        "final_score": 0.72,
        "expectancy": 0.0043,
        "profit_factor": 2.1,
        "sample_size": 102,
        "max_drawdown": -0.13,
        "replay_backtest_consistency": 0.9,
        "backtest_evidence_score": 0.68,
        "proxy_penalty": 0.025,
        "sample_size_penalty": 0.0,
        "missing_evidence_penalty": 0.0,
        "intraday_daily_mismatch_penalty": 0.0,
    }


def _write_sources(root: Path) -> None:
    top_20 = [
        _candidate("ptc-1", 1, mechanism="BREAKOUT", timeframe="5m", symbols=["SPY", "QQQ"]),
        _candidate("ptc-2", 2, mechanism="BREAKOUT", timeframe="5m", symbols=["QQQ", "SPY"]),
        _candidate("ptc-3", 3, mechanism="MEAN_REVERSION", timeframe="15m", symbols=["TLT"], market_structure="GAP_DOWN", session_context="MORNING"),
    ]
    excluded = [
        _candidate("ptc-4", 21, mechanism="BREAKOUT", timeframe="5m", symbols=["SPY", "QQQ"]),
        _candidate("ptc-5", 22, mechanism="BREAKOUT", timeframe="5m", symbols=["SPY", "QQQ"]),
        _candidate("ptc-6", 23, mechanism="REVERSAL", timeframe="30m", symbols=["AAPL"], market_structure="FAILED_BREAKDOWN", classification="REJECT_FOR_NOW"),
    ]
    _write_json(
        root / "final_candidate_ranking" / "latest.json",
        {
            "report_type": "FINAL_CANDIDATE_RANKING",
            "summary": {"ranked_eligible_candidates": 110},
            "top_20_robust_candidates": top_20,
            "excluded_candidates": excluded,
        },
    )
    _write_json(root / "focused_observation_campaign" / "latest.json", {"report_type": "FOCUSED_OBSERVATION_CAMPAIGN", "campaign_candidates": top_20[:2]})
    _write_json(
        root / "candidate_symbol_attribution" / "latest.json",
        {"report_type": "CANDIDATE_SYMBOL_ATTRIBUTION", "candidate_symbol_attributions": top_20[:2]},
    )
    _write_json(root / "backtest_aware_final_qualification" / "latest.json", {"report_type": "BACKTEST_AWARE_FINAL_QUALIFICATION", "summary": {}})
    _write_json(
        root / "winner_pattern_extraction" / "latest.json",
        {
            "report_type": "WINNER_PATTERN_EXTRACTION",
            "cohorts": {
                "all_110_eligible": {"count": 110, "mechanism_distribution": {"BREAKOUT": 44, "MEAN_REVERSION": 22}},
                "rejected_candidates": {"count": 490, "mechanism_distribution": {"OPENING_RANGE": 60}},
            },
            "top_20_candidates": top_20,
            "top_8_campaign_candidates": top_20[:2],
        },
    )


def test_candidate_family_discovery_clusters_and_answers_required_questions(tmp_path: Path) -> None:
    _write_sources(tmp_path)

    report = build_candidate_family_discovery_report(root=tmp_path, created_at="2026-06-05T00:00:00Z")

    assert report["summary"]["eligible_candidates_reported_by_source"] == 110
    assert report["summary"]["detailed_candidate_rows_reviewed"] == 6
    assert report["answers"]["how_many_unique_candidate_families_exist"] == 2
    assert report["summary"]["overrepresented_family_count"] == 1
    assert report["summary"]["eligible_candidates_by_regime"]["CHOP"] == 5
    assert report["summary"]["family_count_by_regime"]["CHOP"] == 2
    assert report["summary"]["rejected_candidates_reported_by_source"] == 490
    assert report["summary"]["eligible_candidates_by_session"]["OPEN"] == 4
    assert report["summary"]["eligible_candidates_by_session"]["MORNING"] == 1
    assert report["summary"]["family_count_by_session"]["OPEN"] == 1
    assert report["required_comparisons"]["all_110_eligible_candidates"]["source_reported_count"] == 110
    assert report["families"][0]["family_name"]
    assert report["families"][0]["dominant_mechanism"]
    assert report["families"][0]["dominant_market_structure"] == "COMPRESSION"
    assert report["required_comparisons"]["top_20_ranked_candidates"]["market_structure_distribution"]["COMPRESSION"] >= 2
    assert report["families"][0]["duplicate_or_distinct_assessment"]
    assert report["families"][0]["dominant_session_context"] in {"OPEN", "MORNING"}
    assert report["answers"]["which_families_contain_the_top_8"][0]["candidate_id"] == "ptc-1"
    assert report["answers"]["which_families_contain_the_top_8"][0]["session_context"] == "OPEN"
    duplicate_groups = report["answers"]["which_candidates_are_duplicates_or_near_duplicates"]
    assert any(set(group["candidate_ids"]) == {"ptc-1", "ptc-2", "ptc-4", "ptc-5"} for group in duplicate_groups)
    assert report["authority_boundary"]["trade_recommendation_authorized"] is False
    assert report["authority_boundary"]["paper_forward_observation_only"] is True
    assert report["authority_boundary"]["automatic_paper_trade_placement_authorized"] is False


def test_candidate_family_discovery_writes_latest_and_dated_outputs(tmp_path: Path) -> None:
    _write_sources(tmp_path)
    report = build_candidate_family_discovery_report(root=tmp_path, created_at="2026-06-05T00:00:00Z")

    paths = write_candidate_family_discovery_report(report, root=tmp_path)

    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()
    assert paths["dated_json"].name == "candidate_family_report.json"
    assert paths["dated_summary"].name == "candidate_family_summary.md"
    assert "Candidate Family Discovery" in paths["latest_summary"].read_text(encoding="utf-8")


def test_candidate_family_discovery_cli_writes_report(tmp_path: Path, capsys) -> None:
    _write_sources(tmp_path)

    result = main(["--root", str(tmp_path), "--candidate-family-discovery"])

    assert result == 0
    assert (tmp_path / "candidate_family_discovery" / "latest.json").exists()
    output = json.loads(capsys.readouterr().out)
    assert output["summary"]["unique_candidate_families"] == 2


def test_run_candidate_family_discovery_is_paper_forward_only(tmp_path: Path) -> None:
    _write_sources(tmp_path)

    report = run_candidate_family_discovery(root=tmp_path, created_at="2026-06-05T00:00:00Z")

    assert report["guardrails"][0] == "Paper-forward observation only."
    assert report["authority_boundary"]["capital_authorized"] is False
