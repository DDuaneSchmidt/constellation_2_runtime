from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.candidate_data_validation_plan import (
    build_candidate_data_validation_plan,
    build_paper_forward_approval_checklist,
    write_candidate_data_validation_plan,
    write_paper_forward_approval_checklist,
)


def _write_campaign(root: Path) -> None:
    path = root / "focused_observation_campaign"
    path.mkdir(parents=True, exist_ok=True)
    payload = {
        "campaign_candidates": [
            {
                "candidate_id": "ptc_a",
                "campaign_rank": 1,
                "mechanism": "BREAKOUT",
                "regime": "CHOP",
                "final_score": 0.72,
                "expectancy": 0.004,
                "profit_factor": 2.1,
                "sample_size": 102,
                "max_drawdown": -0.13,
            },
            {
                "candidate_id": "ptc_b",
                "campaign_rank": 2,
                "mechanism": "EVENT_REACTION",
                "regime": "TRENDING",
                "final_score": 0.714,
                "expectancy": 0.003,
                "profit_factor": 1.5,
                "sample_size": 96,
                "max_drawdown": -0.14,
            },
        ]
    }
    (path / "latest.json").write_text(json.dumps(payload), encoding="utf-8")


def test_candidate_data_validation_plan_covers_campaign_and_proxy_blocker(tmp_path: Path) -> None:
    _write_campaign(tmp_path)

    report = build_candidate_data_validation_plan(tmp_path, created_at="2026-06-05T00:00:00Z")

    assert report["summary"]["candidates_covered"] == 2
    assert report["summary"]["direct_symbol_data_required"] == 2
    assert report["candidate_data_validation_plans"][1]["intraday_required"] is True
    assert "SPY daily proxy" in report["summary"]["primary_blocker"]
    assert report["authority_boundary"]["live_trading_authorized"] is False
    assert report["authority_boundary"]["position_sizing_authorized"] is False


def test_data_plan_and_approval_checklist_write_latest(tmp_path: Path) -> None:
    _write_campaign(tmp_path)

    plan_paths = write_candidate_data_validation_plan(tmp_path, created_at="2026-06-05T00:00:00Z")
    checklist = build_paper_forward_approval_checklist(tmp_path, created_at="2026-06-05T00:00:00Z")
    checklist_paths = write_paper_forward_approval_checklist(tmp_path, created_at="2026-06-05T00:00:00Z")

    assert plan_paths["latest_json"].exists()
    assert checklist_paths["latest_json"].exists()
    assert checklist["summary"]["candidates_covered"] == 2
    assert all(row["human_review_required"] is True for row in checklist["candidate_checklists"])
    assert all(row["authority_boundary"]["broker_execution_authorized"] is False for row in checklist["candidate_checklists"])
