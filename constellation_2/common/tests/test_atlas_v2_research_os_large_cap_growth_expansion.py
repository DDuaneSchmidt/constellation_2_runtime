from __future__ import annotations

import csv
from pathlib import Path

from constellation_2.common.atlas_v2_research_os import large_cap_growth_expansion as mod
from constellation_2.common.atlas_v2_research_os.large_cap_growth_expansion import run_large_cap_growth_expansion


NOW = "2026-06-06T00:00:00Z"


def _write_30m(path: Path, *, rows: int = 80) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
        writer.writeheader()
        for index in range(rows):
            price = 100.0 + index * 0.01
            writer.writerow(
                {
                    "timestamp": f"2023-01-{(index % 28) + 1:02d}T09:30:00Z",
                    "open": price,
                    "high": price + 1.0,
                    "low": price - 1.0,
                    "close": price + 0.5,
                    "volume": 1000 + index,
                }
            )


def test_large_cap_growth_expansion_ranks_symbols_and_blocks_missing_exact_data(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    data_dir = tmp_path / "data" / "manual_intraday_import"
    for symbol in ["AAPL", "MSFT", "META", "AMZN", "TSLA"]:
        _write_30m(data_dir / f"{symbol}_30m.csv")

    metrics_by_symbol = {
        "AAPL": {"sample_size": 60, "expectancy": 0.002, "profit_factor": 1.6, "max_drawdown": -0.03},
        "MSFT": {"sample_size": 60, "expectancy": 0.0004, "profit_factor": 1.1, "max_drawdown": -0.04},
        "META": {"sample_size": 60, "expectancy": 0.0003, "profit_factor": 1.14, "max_drawdown": -0.05},
        "AMZN": {"sample_size": 60, "expectancy": -0.0002, "profit_factor": 0.9, "max_drawdown": -0.06},
        "TSLA": {"sample_size": 60, "expectancy": 0.0015, "profit_factor": 1.5, "max_drawdown": -0.03},
    }

    def fake_run_candidate_backtest_spec(spec, rows, created_at=None):
        symbol = spec["candidate_symbol"]
        return {"metrics": metrics_by_symbol[symbol], "warnings": [], "missing_data": []}

    monkeypatch.setattr(mod, "run_candidate_backtest_spec", fake_run_candidate_backtest_spec)

    report = run_large_cap_growth_expansion(root=root, repo_root=tmp_path, created_at=NOW)

    assert report["build"] == "175-178"
    assert report["summary"]["symbols_requested"] == 8
    assert report["summary"]["symbols_with_exact_data"] == 5
    assert report["summary"]["symbols_confirmed"] == 2
    assert report["summary"]["symbols_weak"] == 2
    assert report["summary"]["symbols_failed"] == 4
    assert report["cross_symbol_ranking"][0]["symbol"] == "AAPL"
    by_symbol = {row["symbol"]: row for row in report["cross_symbol_ranking"]}
    assert by_symbol["GOOGL"]["blocked"] is True
    assert by_symbol["NVDA"]["classification"] == "EXPANSION_FAILED"
    assert by_symbol["MSFT"]["classification"] == "EXPANSION_WEAK"
    assert by_symbol["AAPL"]["classification"] == "EXPANSION_CONFIRMED"
    survivor_by_symbol = {row["symbol"]: row for row in report["survivorship_analysis"]}
    assert survivor_by_symbol["NFLX"]["survivorship_classification"] == "SURVIVORSHIP_DATA_BLOCKED"
    assert report["authority_boundary"]["trade_recommendations"] is False
    assert (root / "large_cap_growth_expansion" / "latest.json").exists()
    assert (root / "large_cap_growth_expansion" / "exact_replay.csv").exists()
    assert (root / "large_cap_growth_expansion" / "cost_analysis.csv").exists()
    assert (root / "large_cap_growth_expansion" / "cross_symbol_ranking.csv").exists()
    assert (root / "large_cap_growth_expansion" / "survivorship_analysis.csv").exists()
