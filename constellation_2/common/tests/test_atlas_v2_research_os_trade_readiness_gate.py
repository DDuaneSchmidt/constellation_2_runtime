from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.trade_readiness_gate import (
    NEAR,
    RESEARCH,
    build_trade_readiness_gate,
    write_trade_readiness_gate,
)


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_builds_209_220_fails_closed_when_validation_layers_missing(tmp_path: Path) -> None:
    _write(
        tmp_path / "mechanism_expansion_program" / "latest.json",
        {
            "summary": {"exact_replays_run": 1},
            "cost_adjusted_mechanism_results": [
                {
                    "candidate_id": "c1",
                    "family_id": "f1",
                    "symbol": "AAPL",
                    "timeframe": "1h",
                    "mechanism": "BREAKOUT",
                    "regime": "CHOP",
                    "cost_bps": 10.0,
                    "sample_size": 75,
                    "gross_expectancy": 0.002,
                    "gross_profit_factor": 1.5,
                    "net_expectancy": 0.001,
                    "net_profit_factor": 1.4,
                    "classification": "MECHANISM_SURVIVOR_STRONG",
                }
            ],
        },
    )

    report = build_trade_readiness_gate(root=tmp_path, created_at="2026-06-06T00:00:00Z")

    assert report["summary"]["survivors_evaluated"] == 1
    assert report["survivor_gate_evaluation"][0]["classification"] == RESEARCH
    assert report["summary"]["paper_spec_ready_count"] == 0


def test_builds_209_220_shortlists_target_but_blocks_on_decay_and_execution(tmp_path: Path) -> None:
    _write(
        tmp_path / "mechanism_expansion_program" / "latest.json",
        {
            "summary": {"exact_replays_run": 187},
            "cost_adjusted_mechanism_results": [
                {
                    "candidate_id": "ptc_backtest_final_651cd169dd508c4e",
                    "family_id": "family_59cc928bca30cc44",
                    "symbol": "TSLA",
                    "timeframe": "30m",
                    "mechanism": "REVERSAL",
                    "regime": "TRENDING",
                    "cost_bps": 10.0,
                    "sample_size": 338,
                    "gross_expectancy": 0.001496,
                    "gross_profit_factor": 1.498734,
                    "net_expectancy": 0.000496,
                    "net_profit_factor": 1.398734,
                    "classification": "MECHANISM_SURVIVOR_STRONG",
                }
            ],
        },
    )
    _write(tmp_path / "walk_forward_validation" / "latest.json", {"summary": {"classification": "WALK_FORWARD_STABLE"}})
    _write(tmp_path / "null_model_randomized_control" / "latest.json", {"summary": {"overall_classification": "BEATS_NULL_STRONGLY"}})
    _write(tmp_path / "temporal_robustness_decay" / "latest.json", {"summary": {"classification": "DECAYING"}})
    _write(tmp_path / "final_research_verdict" / "latest.json", {"summary": {"edge_likely_exploitable": "NOT_YET_PROVEN"}})

    report = build_trade_readiness_gate(root=tmp_path, created_at="2026-06-06T00:00:00Z")
    paths = write_trade_readiness_gate(report, root=tmp_path)

    assert report["survivor_gate_evaluation"][0]["classification"] == NEAR
    assert report["summary"]["near_ready_count"] == 1
    assert report["summary"]["paper_spec_ready_count"] == 0
    assert paths["latest_json"].exists()
    assert (tmp_path / "trade_readiness_gate" / "final_trade_readiness_review.csv").exists()
