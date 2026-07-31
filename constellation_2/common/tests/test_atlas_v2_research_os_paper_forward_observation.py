from pathlib import Path
import json

import pytest

from constellation_2.common.atlas_v2_research_os.paper_forward_observation_governance import PaperForwardObservationGovernanceError, validate_paper_forward_observation_allowed
from constellation_2.common.atlas_v2_research_os.paper_forward_observation_plans import build_paper_forward_observation_plans
from constellation_2.common.atlas_v2_research_os.paper_forward_observation_reports import write_paper_forward_observation_report


def _write_trial(root: Path):
    (root / "methodology_trial").mkdir(parents=True)
    eligible = []
    for idx in range(2):
        eligible.append({
            "paper_trade_candidate_id": f"ptc-{idx}",
            "mechanism": "BREAKOUT" if idx == 0 else "MEAN_REVERSION",
            "hypothesis_id": f"hyp-{idx}",
            "replay_id": f"replay-{idx}",
            "edge_score": 0.72 - idx * 0.01,
            "replay_score": 0.75 - idx * 0.01,
            "regime": "TRENDING",
            "paper_trade_eligible": True,
            "trial_index": idx + 1,
        })
    (root / "methodology_trial" / "latest.json").write_text(json.dumps({"eligible_candidates": eligible}), encoding="utf-8")
    (root / "paper_trade_candidates").mkdir(parents=True)
    (root / "paper_trade_candidates" / "latest.json").write_text(json.dumps({"candidate": {"candidate_id": "ptc-latest"}}), encoding="utf-8")


def test_builds_observation_plans_for_eligible_candidates(tmp_path: Path):
    _write_trial(tmp_path)
    report = build_paper_forward_observation_plans(tmp_path)
    assert report["plans_created"] == 2
    assert report["human_review_queue"][0]["state"] == "READY_FOR_HUMAN_REVIEW"
    assert report["plans"][0]["human_review_required"] is True
    assert report["plans"][0]["minimum_sample_size"] == 30
    assert report["governance_result"]["status"] == "PASS"


def test_paper_forward_observation_governance_blocks_authority():
    with pytest.raises(PaperForwardObservationGovernanceError):
        validate_paper_forward_observation_allowed({"human_review_required": True, "live_trading_authorized": True})
    with pytest.raises(PaperForwardObservationGovernanceError):
        validate_paper_forward_observation_allowed({"human_review_required": True, "artifact_types": ["LiveTrade"]})


def test_writes_observation_report(tmp_path: Path):
    _write_trial(tmp_path)
    paths = write_paper_forward_observation_report(tmp_path, day="2026-06-05")
    assert paths["json"].exists()
    assert paths["summary"].exists()
    latest = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert latest["plans_created"] == 2
