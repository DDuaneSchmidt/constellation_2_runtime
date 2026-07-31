from __future__ import annotations

import csv
import json
from datetime import date, timedelta
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.top_candidate_deep_backtest import (
    build_top_candidate_deep_backtest_report,
    write_top_candidate_deep_backtest_report,
)


def test_top_candidate_deep_backtest_consolidates_sources_and_preserves_authority(tmp_path: Path) -> None:
    _write_source_reports(tmp_path)
    data_path = _write_trending_data(tmp_path)

    report = build_top_candidate_deep_backtest_report(
        root=tmp_path,
        top_n=3,
        data_path=data_path,
        created_at="2026-06-05T00:00:00Z",
    )

    assert report["schema_id"] == "atlas_v2_research_os_top_candidate_deep_backtest_v1"
    assert report["selection_policy"]["selected_for_deep_backtest"] == 3
    assert len(report["candidates"]) == 3
    assert len(report["final_ranked_list_for_paper_forward_observation_only"]) == 3
    first = report["candidates"][0]
    assert {
        "candidate_id",
        "mechanism",
        "expectancy",
        "profit_factor",
        "sample_size",
        "max_drawdown",
        "regime_performance",
        "failure_modes",
        "status",
    }.issubset(first)
    assert first["status"] in {"supported", "weakened", "insufficient data"}
    assert first["paper_forward_observation_only"] is True
    assert report["authority_boundary"]["trade_recommendation_authorized"] is False
    assert report["authority_boundary"]["capital_authorized"] is False
    assert report["authority_boundary"]["broker_execution_authorized"] is False
    assert "does not authorize live trading" in report["authority_statement"]


def test_top_candidate_deep_backtest_writes_dated_and_latest_outputs(tmp_path: Path) -> None:
    _write_source_reports(tmp_path)
    data_path = _write_trending_data(tmp_path)
    report = build_top_candidate_deep_backtest_report(
        root=tmp_path,
        top_n=2,
        data_path=data_path,
        created_at="2026-06-05T00:00:00Z",
    )

    paths = write_top_candidate_deep_backtest_report(report, root=tmp_path)

    assert paths["json"].exists()
    assert paths["summary"].exists()
    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()
    saved = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert saved["summary"]["candidates_deep_tested"] == 2
    assert "Paper-Forward Observation Only" in paths["latest_summary"].read_text(encoding="utf-8")


def _write_source_reports(root: Path) -> None:
    campaign_dir = root / "paper_forward_campaign"
    campaign_dir.mkdir(parents=True)
    (campaign_dir / "latest.json").write_text(
        json.dumps(
            {
                "campaign_candidates": [
                    {
                        "rank": 1,
                        "candidate_id": "ptc_campaign_1",
                        "mechanism": "TREND_CONTINUATION",
                        "regime": "TRENDING",
                        "edge_score": 0.74,
                        "replay_score": 0.73,
                        "expectancy": 0.002,
                        "profit_factor": 1.4,
                        "status": "READY_FOR_OBSERVATION_REVIEW",
                    },
                    {
                        "rank": 2,
                        "candidate_id": "ptc_campaign_2",
                        "mechanism": "BREAKOUT",
                        "regime": "TRENDING",
                        "edge_score": 0.7,
                        "replay_score": 0.7,
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    trial_dir = root / "observation_trial"
    trial_dir.mkdir(parents=True)
    (trial_dir / "latest.json").write_text(
        json.dumps(
            {
                "trials": [
                    {
                        "candidate_id": "ptc_obs_1",
                        "mechanism": "TREND_CONTINUATION",
                        "regime": "TRENDING",
                        "paper_forward_ready": True,
                        "edge_qualification": {"edge_score": 0.72},
                        "historical_replay": {"score": 0.71},
                        "hypothesis": {
                            "hypothesis": "Trend continuation may persist in trending regimes.",
                            "mechanism": "TREND_CONTINUATION",
                            "proxy_regime": "TRENDING",
                            "entry_observation_condition": "Observe matching trend continuation setup only.",
                            "exit_observation_condition": "Close observation at replay horizon.",
                            "invalidation_condition": ["Regime mismatch."],
                        },
                        "candidate_backtest": {
                            "classification": "BACKTEST_SUPPORTED",
                            "metrics": {"expectancy": 0.003, "profit_factor": 1.5},
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    backtest_dir = root / "candidate_backtests"
    backtest_dir.mkdir(parents=True)
    (backtest_dir / "latest.json").write_text(
        json.dumps(
            {
                "candidates": [
                    {
                        "candidate_id": "ptc_backtest_1",
                        "mechanism": "TREND_CONTINUATION",
                        "classification": "BACKTEST_SUPPORTED",
                        "backtest_spec": {
                            "candidate_id": "ptc_backtest_1",
                            "mechanism": "TREND_CONTINUATION",
                            "primary_regime": "UNKNOWN",
                            "allowed_regimes": ["UNKNOWN"],
                            "horizon_days": 5,
                            "source_hypothesis_id": "hyp-backtest-1",
                            "replay_score": 0.7,
                            "research_only": True,
                        },
                        "metrics": {"expectancy": 0.004, "profit_factor": 1.6, "sample_size": 50},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )


def _write_trending_data(root: Path) -> Path:
    path = root / "SPY_fixture.csv"
    start = date(2025, 1, 1)
    price = 100.0
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["date", "open", "high", "low", "close", "volume"])
        writer.writeheader()
        for index in range(140):
            current = start + timedelta(days=index)
            open_price = price
            close = price * 1.003
            writer.writerow(
                {
                    "date": current.isoformat(),
                    "open": round(open_price, 4),
                    "high": round(close * 1.002, 4),
                    "low": round(open_price * 0.998, 4),
                    "close": round(close, 4),
                    "volume": 1000000 + index,
                }
            )
            price = close
    return path
