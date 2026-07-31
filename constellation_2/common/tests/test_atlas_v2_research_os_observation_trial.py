from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.observation_trial import (
    build_observation_trial_report,
    write_observation_trial_report,
)

NOW = "2026-06-05T00:00:00Z"


def _write_observation_import(root: Path) -> None:
    out = root / "observation_import"
    out.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_id": "atlas_v2_research_os_observation_import_report_v1",
        "schema_version": "v1",
        "day": "2026-06-05",
        "raw_observations": 4,
        "valid_observations": 4,
        "invalid_observations": 0,
        "duplicates_skipped": 0,
        "clusters_created": 2,
        "claims_created": 2,
        "backlog_items_created": 2,
        "clusters": [
            {
                "cluster_id": "obs_cluster_breakout",
                "cluster_summary": "2 BREAKOUT observations in TRENDING regime",
                "confidence": 0.70,
                "mechanism": "BREAKOUT",
                "observation_count": 2,
                "regime": "TRENDING",
                "source_observation_ids": ["obs-1", "obs-2"],
                "symbols": ["SPY"],
                "timeframes": ["1d"],
                "metadata": {"measurement_only": True},
            },
            {
                "cluster_id": "obs_cluster_reversal",
                "cluster_summary": "2 REVERSAL observations in HIGH_VOLATILITY regime",
                "confidence": 0.82,
                "mechanism": "REVERSAL",
                "observation_count": 2,
                "regime": "HIGH_VOLATILITY",
                "source_observation_ids": ["obs-3", "obs-4"],
                "symbols": ["SPY"],
                "timeframes": ["1d"],
                "metadata": {"measurement_only": True},
            },
        ],
        "claim_seeds": [],
        "authority_boundary": {
            "research_only": True,
            "broker_execution_authorized": False,
            "capital_authorized": False,
            "live_trading_authorized": False,
        },
    }
    (out / "latest.json").write_text(json.dumps(payload), encoding="utf-8")


def _write_mechanism_search(root: Path) -> None:
    out = root / "mechanism_search"
    out.mkdir(parents=True, exist_ok=True)
    payload = {
        "pipeline_results": [
            {
                "historical_replay": {"status": "REPLAY_POSITIVE"},
                "edge_qualification": {"eligible": False},
                "paper_trade_candidate_gate": {"paper_trade_eligible": False},
            },
            {
                "historical_replay": {"status": "REPLAY_NEUTRAL"},
                "edge_qualification": {"eligible": False},
                "paper_trade_candidate_gate": {"paper_trade_eligible": False},
            },
        ]
    }
    (out / "latest.json").write_text(json.dumps(payload), encoding="utf-8")


def _write_spy_csv(path: Path, rows: int = 120) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["date", "open", "high", "low", "close", "volume", "adjOpen", "adjHigh", "adjLow", "adjClose", "adjVolume"],
        )
        writer.writeheader()
        price = 100.0
        for index in range(rows):
            direction = -0.006 if index % 7 in {0, 1, 2} else 0.008
            price *= 1.0 + direction
            open_price = price * (0.99 if index % 7 == 3 else 1.002)
            high = max(open_price, price) * 1.015
            low = min(open_price, price) * 0.985
            writer.writerow(
                {
                    "date": f"2024-01-{(index % 28) + 1:02d}T00:00:00.000Z",
                    "open": open_price,
                    "high": high,
                    "low": low,
                    "close": price,
                    "volume": 1000000 + index,
                    "adjOpen": open_price,
                    "adjHigh": high,
                    "adjLow": low,
                    "adjClose": price,
                    "adjVolume": 1000000 + index,
                }
            )


def test_observation_trial_builds_required_metrics_and_guardrails(tmp_path: Path) -> None:
    _write_observation_import(tmp_path)
    _write_mechanism_search(tmp_path)
    data_path = tmp_path / "spy.csv"
    _write_spy_csv(data_path)

    report = build_observation_trial_report(root=tmp_path, created_at=NOW, data_path=data_path)

    metrics = report["metrics"]
    assert metrics["observations_imported"] == 4
    assert metrics["clusters_created"] == 2
    assert metrics["claims_generated"] == 2
    assert metrics["hypotheses_generated"] == 2
    assert "positive_replay_rate" in metrics
    assert "eligible_candidates" in metrics
    assert "backtest_supported_candidates" in metrics
    assert "paper_forward_ready_candidates" in metrics
    assert report["mechanism_comparison"]["mechanism_search_available"] is True
    assert report["authority_boundary"]["live_trading_authorized"] is False
    assert report["authority_boundary"]["capital_authorized"] is False
    assert report["authority_boundary"]["broker_execution_authorized"] is False


def test_observation_trial_writes_report_and_latest_pointers(tmp_path: Path) -> None:
    _write_observation_import(tmp_path)
    _write_mechanism_search(tmp_path)
    data_path = tmp_path / "spy.csv"
    _write_spy_csv(data_path)

    paths = write_observation_trial_report(root=tmp_path, day="2026-06-05", created_at=NOW, data_path=data_path)

    assert paths["json"].exists()
    assert paths["summary"].exists()
    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["schema_id"] == "atlas_v2_research_os_observation_trial_report_v1"
    assert "ObservationCluster" in payload["pipeline"]
    assert "No live" in paths["latest_summary"].read_text(encoding="utf-8") or "live trading" in paths["latest_summary"].read_text(encoding="utf-8")

