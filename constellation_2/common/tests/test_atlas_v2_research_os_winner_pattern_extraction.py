from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.winner_pattern_extraction import (
    build_winner_pattern_report,
    write_winner_pattern_report,
)


def test_winner_pattern_report_writes_latest_and_keeps_authority_boundary(tmp_path: Path) -> None:
    report = build_winner_pattern_report(tmp_path, observation_count=200, created_at="2026-06-05T00:00:00Z")
    paths = write_winner_pattern_report(report, tmp_path)

    assert report["cohorts"]["top_8_campaign"]["count"] <= 8
    assert report["input_context"]["split_diversity_hypotheses"] > 0
    assert report["authority_boundary"]["paper_forward_observation_only"] is True
    assert report["authority_boundary"]["live_trading_authorized"] is False
    assert report["authority_boundary"]["capital_authorized"] is False
    assert paths["latest_json"].exists()
    latest = json.loads((tmp_path / "winner_pattern_extraction" / "latest.json").read_text(encoding="utf-8"))
    assert latest["answers"]["what_winners_have_in_common"]
    assert "positive_replay_rate_by_regime" in latest["cohorts"]["top_20"]
