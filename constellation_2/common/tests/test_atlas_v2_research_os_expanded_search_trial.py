from __future__ import annotations

import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.expanded_search_trial import (
    BASELINE,
    PROFILE,
    build_expanded_search_trial_report,
    write_expanded_search_trial_report,
)
from constellation_2.common.atlas_v2_research_os.observation_models import OBSERVATION_WORKLOAD_PROFILES

NOW = "2026-06-05T00:00:00Z"


def test_expanded_search_trial_profile_is_bounded() -> None:
    assert PROFILE == "EXPANDED_OBSERVATION_TRIAL_5000"
    assert OBSERVATION_WORKLOAD_PROFILES[PROFILE] == 5000


def test_expanded_search_trial_builds_pipeline_and_baseline_comparison(tmp_path: Path) -> None:
    report = build_expanded_search_trial_report(root=tmp_path, created_at=NOW, dry_run_limit=4, trial_limit=12)
    assert report["schema_id"] == "atlas_v2_research_os_expanded_search_trial_v1"
    assert report["profile"] == PROFILE
    assert report["bounded"] is True
    assert report["observation_expansion"]["metrics"]["observations_generated"] == 5000
    assert report["final_qualification"]["summary"]["hypotheses_generated"] == 12
    assert report["comparison_against_prior_baseline"]["baseline"] == BASELINE
    assert report["observation_dimensions"]["session_contexts_covered"] == 8
    assert report["observation_dimensions"]["market_structures_covered"] >= 5
    assert report["authority_boundary"]["live_trading_authorized"] is False
    assert report["authority_boundary"]["broker_execution_authorized"] is False
    assert report["authority_boundary"]["capital_authorized"] is False
    assert report["authority_boundary"]["trade_recommendation_authorized"] is False
    assert report["authority_boundary"]["automatic_paper_trade_placement_authorized"] is False
    assert report["authority_boundary"]["candidate_promotion_authorized"] is False


def test_expanded_search_trial_writes_requested_outputs(tmp_path: Path) -> None:
    paths = write_expanded_search_trial_report(root=tmp_path, day="2026-06-05", created_at=NOW, dry_run_limit=2, trial_limit=5)
    for path in paths.values():
        assert path.exists()
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["profile"] == PROFILE
    assert paths["json"].name == "expanded_search_trial_report.json"
    assert paths["summary"].name == "expanded_search_trial_summary.md"
    assert "Expanded Search Trial" in paths["latest_summary"].read_text(encoding="utf-8")
