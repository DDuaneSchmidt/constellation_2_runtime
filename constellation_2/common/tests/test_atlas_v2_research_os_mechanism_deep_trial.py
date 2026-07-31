from __future__ import annotations

from pathlib import Path

from constellation_2.common.atlas_v2_research_os.mechanism_deep_trial import run_mechanism_deep_trial, write_mechanism_deep_trial_report
from constellation_2.common.atlas_v2_research_os.mechanism_hypothesis_generator import generate_mechanism_hypotheses


def test_mechanism_deep_trial_report_ranks_mechanisms_and_writes_latest(tmp_path: Path) -> None:
    run = generate_mechanism_hypotheses(limit=30, root=tmp_path / "store", created_at="2026-06-05T00:00:00Z", replay_limit=10)
    report = run_mechanism_deep_trial(root=tmp_path / "store", created_at="2026-06-05T00:00:00Z", run=run)
    assert report["summary"]["hypotheses_generated"] == 30
    assert report["summary"]["historical_replays_executed"] == 10
    assert len(report["mechanism_metrics"]) == 10
    assert report["authority_boundary"]["live_trading_allowed"] is False
    assert report["authority_boundary"]["capital_allocation_allowed"] is False
    paths = write_mechanism_deep_trial_report(report, report_root=tmp_path / "mechanism_deep_trial", day="2026-06-05")
    assert paths["json"].exists()
    assert paths["summary"].exists()
    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()
