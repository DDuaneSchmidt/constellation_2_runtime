from __future__ import annotations

import json

from constellation_2.common.atlas_v2_research_os.observation_scale_stress import (
    build_observation_scale_stress_report,
    render_observation_scale_stress_summary,
    write_observation_scale_stress_report,
)


def test_observation_scale_stress_runs_graduated_levels_without_authority(tmp_path):
    report = build_observation_scale_stress_report(levels=(50, 100), created_at="2026-06-05T00:00:00Z")

    assert report["levels_executed"] == [50, 100]
    assert report["confirmations"]["no_live_trading"] is True
    assert report["confirmations"]["no_broker_execution"] is True
    assert report["confirmations"]["no_capital_authority"] is True
    assert report["confirmations"]["no_automatic_paper_placement"] is True
    assert report["confirmations"]["no_position_sizing"] is True
    assert report["confirmations"]["paper_forward_ready_is_human_review_only"] is True
    for row in report["results"]:
        assert row["observations_generated"] == row["level"]
        assert row["observations_imported"] > 0
        assert row["clusters_created"] > 0
        assert row["claims_generated"] == row["clusters_created"]
        assert row["hypotheses_generated"] == row["historical_replays_executed"]
        assert "paper_forward_ready_candidates_per_1000" in row["quality_per_1000_observations"]
        assert row["governance_certification"]["status"] == "PASS"
        assert row["authority_boundary"]["live_trading_authorized"] is False
        assert row["authority_boundary"]["broker_execution_authorized"] is False
        assert row["authority_boundary"]["capital_authorized"] is False
        assert row["authority_boundary"]["automatic_paper_trade_placement_authorized"] is False
        assert row["authority_boundary"]["position_sizing_authorized"] is False

    summary = render_observation_scale_stress_summary(report)
    assert "No live trading." in summary
    assert "No broker execution." in summary
    assert "No capital authority." in summary


def test_observation_scale_stress_skips_100000_without_clean_50000():
    report = build_observation_scale_stress_report(levels=(5, 100_000), created_at="2026-06-05T00:00:00Z")

    skipped = [row for row in report["results"] if row["level"] == 100_000][0]
    assert skipped["status"] == "SKIPPED"
    assert skipped["skip_reason"] == "STOP_BEFORE_100000_UNLESS_50000_COMPLETES_CLEANLY"


def test_write_observation_scale_stress_report(tmp_path):
    paths = write_observation_scale_stress_report(
        report_root=tmp_path / "observation_scale_stress",
        levels=(25,),
        day="2026-06-05",
        created_at="2026-06-05T00:00:00Z",
    )

    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["day"] == "2026-06-05"
    assert paths["summary"].exists()
    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()
