from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.candidate_backtests import (
    INSUFFICIENT_DATA_CLASSIFICATION,
    build_candidate_backtest_report,
    create_backtest_spec,
    run_candidate_backtest_spec,
    write_candidate_backtest_report,
)


def _write_plan(root: Path) -> None:
    report_dir = root / "paper_forward_observation"
    report_dir.mkdir(parents=True)
    payload = {
        "plans": [
            {
                "candidate_id": "ptc-test-breakout",
                "mechanism": "BREAKOUT",
                "hypothesis": "Breakout continuation remains observable.",
                "entry_observation_condition": "close breaks above prior range",
                "exit_observation_condition": "observe forward return",
                "invalidating_conditions": ["breakout fails"],
                "regime_constraints": {"primary_regime": "UNKNOWN", "allowed_regimes": ["UNKNOWN"], "reject_if_regime_unknown": False},
                "replay_score": 0.7,
                "edge_score": 0.72,
                "source_hypothesis_id": "hyp-test",
                "source_replay_id": "replay-test",
            }
        ]
    }
    (report_dir / "latest.json").write_text(json.dumps(payload), encoding="utf-8")


def _write_spy_csv(path: Path, rows: int = 90) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["date", "open", "high", "low", "close", "volume", "adjOpen", "adjHigh", "adjLow", "adjClose", "adjVolume"])
        writer.writeheader()
        price = 100.0
        for index in range(rows):
            price *= 1.002
            open_price = price * 0.995
            high = price * 1.01
            low = price * 0.99
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


def test_candidate_backtest_builds_and_writes_report(tmp_path: Path) -> None:
    _write_plan(tmp_path)
    data_path = tmp_path / "spy.csv"
    _write_spy_csv(data_path)

    report = build_candidate_backtest_report(root=tmp_path, data_path=data_path, created_at="2026-06-05T00:00:00Z")
    assert report["authority"]["live_trading_allowed"] is False
    assert report["authority"]["capital_authority_allowed"] is False
    assert report["summary"]["candidates_tested"] == 1
    assert report["candidates"][0]["metrics"]["sample_size"] >= 0

    paths = write_candidate_backtest_report(report, root=tmp_path)
    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()


def test_candidate_backtest_fails_closed_when_data_missing() -> None:
    spec = create_backtest_spec({"candidate_id": "ptc-missing", "mechanism": "BREAKOUT"})
    result = run_candidate_backtest_spec(spec, [], created_at="2026-06-05T00:00:00Z")
    assert result["classification"] == INSUFFICIENT_DATA_CLASSIFICATION
    assert "local SPY adjusted daily OHLCV data not found" in result["missing_data"]
    assert result["research_only"] is True
