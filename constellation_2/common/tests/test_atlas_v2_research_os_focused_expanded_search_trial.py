from __future__ import annotations

import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.focused_expanded_search_trial import (
    FOCUSED_DIMENSIONS,
    PROFILE,
    build_focused_expanded_search_trial_report,
    write_focused_expanded_search_trial_report,
)
from constellation_2.common.atlas_v2_research_os.observation_models import OBSERVATION_WORKLOAD_PROFILES

NOW = "2026-06-05T00:00:00Z"


def test_focused_profile_is_registered_with_requested_dimensions() -> None:
    assert PROFILE == "EXPANDED_OBSERVATION_TRIAL_FOCUSED"
    assert OBSERVATION_WORKLOAD_PROFILES[PROFILE] == 6400
    assert FOCUSED_DIMENSIONS["session_context"] == []
    assert set(FOCUSED_DIMENSIONS["mechanism"]) == {"BREAKOUT", "MEAN_REVERSION", "EVENT_REACTION", "REVERSAL"}
    assert set(FOCUSED_DIMENSIONS["symbol_universe"]) == {"QQQ", "DBC", "DIA", "USO", "GOOGL"}


def test_focused_expanded_search_trial_builds_comparisons_and_guardrails(tmp_path: Path) -> None:
    report = build_focused_expanded_search_trial_report(root=tmp_path, created_at=NOW, dry_run_limit=3, trial_limit=8)
    assert report["schema_id"] == "atlas_v2_research_os_focused_expanded_search_trial_v1"
    assert report["profile"] == PROFILE
    assert report["observation_expansion"]["metrics"]["observations_generated"] == 6400
    assert report["required_metrics"]["hypotheses_generated"] == 8
    assert "quality_vs_broad_expanded_search" in report["comparison"]
    assert "profile_decision" in report["required_conclusion"]
    assert report["authority_boundary"]["live_trading_authorized"] is False
    assert report["authority_boundary"]["broker_execution_authorized"] is False
    assert report["authority_boundary"]["capital_authorized"] is False
    assert report["authority_boundary"]["position_sizing_authorized"] is False
    assert report["authority_boundary"]["trade_recommendation_authorized"] is False
    assert report["authority_boundary"]["automatic_paper_trade_placement_authorized"] is False


def test_focused_expanded_search_trial_writes_requested_outputs(tmp_path: Path) -> None:
    paths = write_focused_expanded_search_trial_report(root=tmp_path, day="2026-06-05", created_at=NOW, dry_run_limit=2, trial_limit=5)
    for path in paths.values():
        assert path.exists()
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["report_type"] == "FOCUSED_EXPANDED_SEARCH_TRIAL"
    assert paths["json"].name == "focused_expanded_search_trial_report.json"
    assert paths["summary"].name == "focused_expanded_search_trial_summary.md"
    assert "Focused Expanded Search Trial" in paths["latest_summary"].read_text(encoding="utf-8")
