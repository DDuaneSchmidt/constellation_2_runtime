from pathlib import Path
import json

from constellation_2.common.atlas_v2_research_os.candidate_observation_readiness import build_candidate_observation_readiness, write_candidate_observation_readiness_report


def _seed(root: Path):
    (root / "paper_forward_observation").mkdir(parents=True)
    (root / "methodology_trial").mkdir(parents=True)
    plan = {
        "candidate_id": "ptc-ready",
        "mechanism": "BREAKOUT",
        "hypothesis": "Observe whether BREAKOUT hypothesis hyp-ready continues to show replay-supported behavior in paper-forward data.",
        "source_hypothesis_id": "hyp-ready",
        "source_replay_id": "replay-ready",
        "edge_score": 0.75,
        "replay_score": 0.75,
        "entry_observation_condition": "Record a paper-forward observation only when the breakout setup appears with the same regime and confirmation context used in replay; do not place an order.",
        "exit_observation_condition": "Close the observation record when the replay-defined follow-through window ends, the setup invalidates, or the paper-forward observation reaches its fixed review horizon.",
        "invalidating_conditions": ["Required market context is unavailable or stale.", "Observed regime differs from the plan regime constraints.", "Setup trigger cannot be reconstructed from paper-forward data."],
        "observation_window": {"sessions": 30},
        "minimum_sample_size": 30,
        "success_metrics": ["paper_forward_sample_size"],
        "failure_metrics": ["paper_forward_failure_rate"],
        "regime_constraints": {"primary_regime": "TRENDING", "allowed_regimes": ["TRENDING"]},
        "human_review_required": True,
    }
    (root / "paper_forward_observation" / "latest.json").write_text(json.dumps({"plans": [plan]}), encoding="utf-8")
    (root / "methodology_trial" / "latest.json").write_text(json.dumps({"eligible_candidates": [{"paper_trade_candidate_id": "ptc-ready", "replay_sample_size": 12, "regime": "TRENDING"}]}), encoding="utf-8")


def test_candidate_observation_readiness_report(tmp_path: Path):
    _seed(tmp_path)
    report = build_candidate_observation_readiness(tmp_path, day="2026-06-05")
    assert report["total_candidates_reviewed"] == 1
    assert report["ready_for_observation_count"] == 1
    assert report["recommended_human_decisions"][0]["decision"] == "APPROVE_FOR_PAPER_FORWARD_OBSERVATION"
    paths = write_candidate_observation_readiness_report(tmp_path, day="2026-06-05")
    assert paths["json"].exists()
    assert paths["latest_summary"].exists()
