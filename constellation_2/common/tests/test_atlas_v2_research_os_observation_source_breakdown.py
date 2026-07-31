from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.observation_source_breakdown import (
    build_observation_source_breakdown,
    write_observation_source_breakdown_report,
)


def _write_latest(root: Path, name: str, payload: dict) -> None:
    path = root / name
    path.mkdir(parents=True, exist_ok=True)
    (path / "latest.json").write_text(json.dumps(payload), encoding="utf-8")


def test_breakdown_groups_observation_trials_by_source_dimensions(tmp_path: Path) -> None:
    _write_latest(
        tmp_path,
        "observation_import",
        {
            "batch": {
                "records": [
                    {
                        "observation_id": "obs_a",
                        "symbol": "SPY",
                        "timeframe": "5m",
                        "metadata": {"source_type": "manual_review"},
                    },
                    {
                        "observation_id": "obs_b",
                        "symbol": "QQQ",
                        "timeframe": "15m",
                        "metadata": {"source_type": "scanner_snapshot"},
                    },
                ]
            }
        },
    )
    _write_latest(
        tmp_path,
        "observation_trial",
        {
            "trials": [
                {
                    "candidate_id": "ptc_a",
                    "mechanism": "BREAKOUT",
                    "regime": "TRENDING",
                    "claim": {
                        "source_observation_ids": ["obs_a"],
                        "symbols": ["SPY"],
                        "timeframes": ["5m"],
                        "confidence": 0.55,
                    },
                    "historical_replay": {"status": "REPLAY_POSITIVE"},
                    "edge_qualification": {"eligible": True, "edge_score": 0.71},
                    "candidate_backtest": {"classification": "BACKTEST_SUPPORTED", "metrics": {"expectancy": 0.01, "profit_factor": 1.5}},
                    "paper_forward_ready": True,
                },
                {
                    "candidate_id": "ptc_b",
                    "mechanism": "REVERSAL",
                    "regime": "CHOP",
                    "claim": {
                        "source_observation_ids": ["obs_b"],
                        "symbols": ["QQQ"],
                        "timeframes": ["15m"],
                        "confidence": 0.82,
                    },
                    "historical_replay": {"status": "REPLAY_NEGATIVE"},
                    "edge_qualification": {"eligible": False, "edge_score": 0.4},
                    "candidate_backtest": {"classification": "BACKTEST_WEAK", "metrics": {"expectancy": -0.01, "profit_factor": 0.8}},
                    "paper_forward_ready": False,
                },
            ]
        },
    )

    report = build_observation_source_breakdown(tmp_path, created_at="2026-06-05T00:00:00Z")

    assert report["summary"]["trials_analyzed"] == 2
    assert report["summary"]["positive_replay_rate"] == 0.5
    source_groups = {row["group"]: row for row in report["dimension_breakdowns"]["source_type"]}
    assert source_groups["manual_review"]["paper_forward_readiness_rate"] == 1.0
    assert source_groups["scanner_snapshot"]["backtest_support_rate"] == 0.0
    confidence_groups = {row["group"]: row for row in report["dimension_breakdowns"]["confidence_bucket"]}
    assert "LOW_<0.60" in confidence_groups
    assert "HIGH_0.75_0.90" in confidence_groups
    assert report["authority_boundary"]["live_trading_authorized"] is False
    assert report["authority_boundary"]["capital_authorized"] is False
    assert report["authority_boundary"]["trade_recommendation_authorized"] is False


def test_breakdown_report_writes_latest_files(tmp_path: Path) -> None:
    _write_latest(tmp_path, "observation_import", {"batch": {"records": []}})
    _write_latest(tmp_path, "observation_trial", {"trials": []})

    paths = write_observation_source_breakdown_report(tmp_path, day="2026-06-05")

    assert paths["json"].exists()
    assert paths["summary"].exists()
    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()
