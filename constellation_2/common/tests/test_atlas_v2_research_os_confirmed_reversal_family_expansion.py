from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.confirmed_reversal_family_expansion import (
    TARGET_CANDIDATE_ID,
    TARGET_FAMILY_ID,
    build_confirmed_reversal_family_expansion_report,
    write_confirmed_reversal_family_expansion_report,
)


def _write_latest(root: Path, name: str, payload: dict) -> None:
    path = root / name
    path.mkdir(parents=True, exist_ok=True)
    (path / "latest.json").write_text(json.dumps(payload), encoding="utf-8")


def test_confirmed_reversal_family_expansion_reports_target_and_related_variant(tmp_path: Path) -> None:
    _write_latest(
        tmp_path,
        "candidate_family_discovery",
        {
            "families": [
                {
                    "family_id": TARGET_FAMILY_ID,
                    "family_name": "REVERSAL / TRENDING / 1H / SCREEN_REPLAY",
                    "mechanism": "REVERSAL",
                    "regime": "TRENDING",
                    "timeframes": ["1H"],
                    "source_types": ["SCREEN_REPLAY"],
                    "symbols": ["BAC"],
                    "candidate_ids": [TARGET_CANDIDATE_ID],
                },
                {
                    "family_id": "family_related",
                    "family_name": "REVERSAL / TRENDING / 30M / UNKNOWN",
                    "mechanism": "REVERSAL",
                    "regime": "TRENDING",
                    "timeframes": ["30M"],
                    "source_types": ["UNKNOWN"],
                    "symbols": ["BAC"],
                    "candidate_ids": ["candidate_related"],
                    "best_rank": 2,
                },
            ]
        },
    )
    _write_latest(
        tmp_path,
        "direct_candidate_data_validation",
        {
            "candidate_validations": [
                {
                    "candidate_id": TARGET_CANDIDATE_ID,
                    "classification": "CONFIRMED",
                    "direct_result": {
                        "classification": "BACKTEST_SUPPORTED",
                        "sample_size": 62,
                        "expectancy": 0.004254,
                        "profit_factor": 1.547891,
                        "max_drawdown": -0.149031,
                    },
                }
            ]
        },
    )
    _write_latest(tmp_path, "family_robustness_review", {"family_reviews": [{"family_id": TARGET_FAMILY_ID, "classification": "PROMISING_BUT_DATA_BLOCKED"}]})
    _write_latest(tmp_path, "final_candidate_ranking", {"top_20_robust_candidates": [{"candidate_id": TARGET_CANDIDATE_ID, "mechanism": "REVERSAL", "regime": "TRENDING"}]})
    _write_latest(tmp_path, "holdout_replay_validation", {"data_availability": {"event_level_rows_loaded": 0, "event_level_rows_with_returns": 0}, "summary": {"classification_counts": {"DATA_BLOCKED": 1}}})

    report = build_confirmed_reversal_family_expansion_report(tmp_path, created_at="2026-06-06T00:00:00Z")

    assert report["target_family"]["family_id"] == TARGET_FAMILY_ID
    assert report["target_family"]["direct_validation_classification"] == "DIRECT_CONFIRMED"
    assert report["summary"]["related_variants_found"] == 1
    assert report["holdout_feasibility"]["can_run_now"] is False
    assert report["authority_boundary"]["live_trading_authorized"] is False
    assert report["authority_boundary"]["position_sizing_authorized"] is False


def test_confirmed_reversal_family_expansion_writes_latest(tmp_path: Path) -> None:
    _write_latest(tmp_path, "candidate_family_discovery", {"families": []})
    _write_latest(tmp_path, "direct_candidate_data_validation", {"candidate_validations": []})
    report = build_confirmed_reversal_family_expansion_report(tmp_path, created_at="2026-06-06T00:00:00Z")
    paths = write_confirmed_reversal_family_expansion_report(report, root=tmp_path)

    assert paths["latest_json"].exists()
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["report_type"] == "CONFIRMED_REVERSAL_FAMILY_EXPANSION"
