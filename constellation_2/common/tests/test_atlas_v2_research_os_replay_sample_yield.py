from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.cli import main
from constellation_2.common.atlas_v2_research_os.replay_sample_yield import (
    build_replay_sample_yield_report,
    run_replay_sample_yield,
    write_replay_sample_yield_report,
)


def _write_candidate_backtests(root: Path) -> None:
    report_dir = root / "candidate_backtests"
    report_dir.mkdir(parents=True)
    payload = {
        "report_type": "CANDIDATE_HISTORICAL_BACKTEST_REPLAY",
        "created_at": "2026-06-05T00:00:00Z",
        "candidates": [
            {
                "candidate_id": "ptc-supported",
                "mechanism": "MEAN_REVERSION",
                "classification": "BACKTEST_SUPPORTED",
                "metrics": {
                    "trigger_count": 100,
                    "regime_filtered_count": 40,
                    "invalidations": 10,
                    "sample_size": 50,
                },
                "warnings": ["40 triggered samples filtered by candidate regime constraints"],
            },
            {
                "candidate_id": "ptc-zero",
                "mechanism": "BREAKOUT",
                "classification": "INSUFFICIENT_DATA",
                "metrics": {
                    "trigger_count": 20,
                    "regime_filtered_count": 20,
                    "invalidations": 0,
                    "sample_size": 0,
                },
                "missing_data": ["minimum sample size 30 not met after trigger/regime filtering; observed 0"],
            },
            {
                "candidate_id": "ptc-low",
                "mechanism": "REVERSAL",
                "classification": "BACKTEST_WEAK",
                "metrics": {
                    "trigger_count": 25,
                    "regime_filtered_count": 0,
                    "invalidations": 0,
                    "sample_size": 25,
                },
            },
        ],
    }
    (report_dir / "latest.json").write_text(json.dumps(payload), encoding="utf-8")


def test_replay_sample_yield_builds_from_existing_backtest_artifact(tmp_path: Path) -> None:
    _write_candidate_backtests(tmp_path)

    report = build_replay_sample_yield_report(root=tmp_path, created_at="2026-06-05T00:00:00Z")

    assert report["read_only"] is True
    assert report["raw_candidate_count"] == 3
    assert report["trigger_sample_count"] == 145
    assert report["post_filter_sample_count"] == 75
    assert report["replay_sample_yield"] == 0.517241
    assert report["attrition_by_filter"]["regime_filter"]["count"] == 60
    assert report["attrition_by_filter"]["invalidations"]["count"] == 10
    assert report["attrition_by_filter"]["other_or_unclassified"]["count"] == 0
    assert [candidate["candidate_id"] for candidate in report["zero_sample_candidates"]] == ["ptc-zero"]
    assert [candidate["candidate_id"] for candidate in report["low_sample_candidates"]] == ["ptc-low"]
    assert report["authority_boundary"]["replay_behavior_changes_allowed"] is False
    assert report["authority_boundary"]["candidate_promotion_allowed"] is False
    assert report["authority_boundary"]["capital_authority_allowed"] is False


def test_replay_sample_yield_writes_latest_and_dated_outputs(tmp_path: Path) -> None:
    _write_candidate_backtests(tmp_path)
    report = build_replay_sample_yield_report(root=tmp_path, created_at="2026-06-05T00:00:00Z")

    paths = write_replay_sample_yield_report(report, root=tmp_path)

    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()
    assert paths["dated_json"].exists()
    assert paths["dated_summary"].exists()
    summary = paths["latest_summary"].read_text(encoding="utf-8")
    assert "Replay Sample Yield" in summary
    assert "No replay behavior changes." in summary


def test_replay_sample_yield_cli_writes_report(tmp_path: Path, capsys) -> None:
    _write_candidate_backtests(tmp_path)

    result = main(["--root", str(tmp_path), "--replay-sample-yield"])

    assert result == 0
    assert (tmp_path / "replay_sample_yield" / "latest.json").exists()
    output = json.loads(capsys.readouterr().out)
    assert output["summary"]["raw_candidate_count"] == 3
    assert output["report"].endswith("replay_sample_yield/latest.json")


def test_run_replay_sample_yield_is_report_only(tmp_path: Path) -> None:
    _write_candidate_backtests(tmp_path)

    report = run_replay_sample_yield(root=tmp_path, created_at="2026-06-05T00:00:00Z")

    assert report["source_reports"]["candidate_backtests"]["exists"] is True
    assert report["authority_boundary"]["replay_override_allowed"] is False
    assert (tmp_path / "candidate_backtests" / "latest.json").exists()
