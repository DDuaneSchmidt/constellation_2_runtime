from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.paper_forward_campaign import (
    build_paper_forward_campaign,
    write_paper_forward_campaign_report,
)


def _write_latest(root: Path, name: str, payload: dict) -> None:
    path = root / name
    path.mkdir(parents=True, exist_ok=True)
    (path / "latest.json").write_text(json.dumps(payload), encoding="utf-8")


def test_campaign_selects_deduplicated_observation_only_pool(tmp_path: Path) -> None:
    _write_latest(
        tmp_path,
        "candidate_observation_readiness",
        {
            "candidate_reviews": [
                {
                    "candidate_id": "ptc_a",
                    "classification": "READY_FOR_OBSERVATION",
                    "recommended_human_decision": "APPROVE_FOR_PAPER_FORWARD_OBSERVATION",
                    "mechanism": "MEAN_REVERSION",
                    "edge_score": 0.74,
                    "minimum_sample_size": 30,
                    "regime_constraints": {"primary_regime": "TRENDING"},
                },
                {
                    "candidate_id": "ptc_b",
                    "classification": "READY_FOR_OBSERVATION",
                    "recommended_human_decision": "APPROVE_FOR_PAPER_FORWARD_OBSERVATION",
                    "mechanism": "BREAKOUT",
                    "edge_score": 0.73,
                    "minimum_sample_size": 30,
                    "regime_constraints": {"primary_regime": "TRENDING"},
                },
            ]
        },
    )
    _write_latest(tmp_path, "candidate_review", {"top_candidates": []})
    _write_latest(tmp_path, "paper_forward_observation", {"plans": []})
    _write_latest(
        tmp_path,
        "observation_trial",
        {
            "trials": [
                {
                    "candidate_id": "ptc_obs",
                    "paper_forward_ready": True,
                    "mechanism": "REVERSAL",
                    "regime": "HIGH_VOLATILITY",
                    "candidate_backtest": {"classification": "BACKTEST_SUPPORTED", "metrics": {"expectancy": 0.01, "profit_factor": 1.5}},
                    "hypothesis": {
                        "entry_observation_condition": "Observe reversal only.",
                        "invalidation_condition": ["lineage breaks"],
                    },
                }
            ]
        },
    )
    _write_latest(
        tmp_path,
        "candidate_backtests",
        {
            "candidates": [
                {
                    "candidate_id": "ptc_a",
                    "classification": "BACKTEST_SUPPORTED",
                    "mechanism": "MEAN_REVERSION",
                    "metrics": {"expectancy": 0.02, "profit_factor": 2.0},
                    "backtest_spec": {"entry_observation_condition": "Observe mean reversion only."},
                },
                {
                    "candidate_id": "ptc_c",
                    "classification": "BACKTEST_SUPPORTED",
                    "mechanism": "VWAP_OR_AVERAGE_RECLAIM",
                    "metrics": {"expectancy": 0.01, "profit_factor": 1.3},
                    "backtest_spec": {"entry_observation_condition": "Observe reclaim only."},
                },
            ]
        },
    )

    report = build_paper_forward_campaign(tmp_path, created_at="2026-06-05T00:00:00Z")

    ids = [row["candidate_id"] for row in report["campaign_candidates"]]
    assert ids.count("ptc_a") == 1
    assert set(ids) == {"ptc_a", "ptc_b", "ptc_obs", "ptc_c"}
    assert report["summary"]["deduplicated_overlap_count"] == 1
    for row in report["campaign_candidates"]:
        assert row["authority_boundary"]["paper_forward_observation_only"] is True
        assert row["authority_boundary"]["trade_recommendation_authorized"] is False
        assert row["authority_boundary"]["broker_execution_authorized"] is False
        assert row["authority_boundary"]["capital_authorized"] is False
        assert row["authority_boundary"]["position_sizing_authorized"] is False


def test_campaign_report_writes_latest_files(tmp_path: Path) -> None:
    for name in [
        "candidate_backtests",
        "candidate_review",
        "candidate_observation_readiness",
        "paper_forward_observation",
        "observation_trial",
    ]:
        _write_latest(tmp_path, name, {})

    paths = write_paper_forward_campaign_report(tmp_path, day="2026-06-05")

    assert paths["json"].exists()
    assert paths["summary"].exists()
    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()
