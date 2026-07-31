from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os import volatility_profile_expansion as build179


NOW = "2026-06-06T00:00:00Z"


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    (root / "exact_coverage_import_validator").mkdir(parents=True)
    return root


def _write_validator(root: Path, symbols: list[str]) -> None:
    payload = {
        "import_validation_matrix": [
            {
                "symbol": symbol,
                "timeframe": "30m",
                "validation_status": "VALID_WITH_WARNINGS",
                "actual_file": f"data/manual_intraday_import/{symbol}_30m.csv",
            }
            for symbol in symbols
        ]
    }
    (root / "exact_coverage_import_validator" / "latest.json").write_text(json.dumps(payload), encoding="utf-8")


def _write_bars(repo: Path, symbols: list[str]) -> None:
    data_dir = repo / "data" / "manual_intraday_import"
    data_dir.mkdir(parents=True)
    rows = ["timestamp,open,high,low,close,volume"]
    for i in range(80):
        close = 100.0 + (i * 0.1)
        rows.append(f"2024-01-{(i % 28) + 1:02d}T09:30:00Z,{close - 0.1},{close + 0.2},{close - 0.2},{close},1000")
    for symbol in symbols:
        (data_dir / f"{symbol}_30m.csv").write_text("\n".join(rows) + "\n", encoding="utf-8")


def _patch_replay(monkeypatch, metrics_by_symbol: dict[str, dict[str, object]]) -> None:
    def fake_run_requirement(requirement, valid_file, *, created_at):
        symbol = requirement["symbol"]
        metrics = metrics_by_symbol.get(symbol, {"sample_size": 80, "expectancy": -0.001, "profit_factor": 0.9})
        return {
            "candidate_id": requirement["candidate_id"],
            "family_id": requirement["family_id"],
            "symbol": symbol,
            "timeframe": requirement["timeframe"],
            "data_file": valid_file["data_file"],
            "sample_size": metrics["sample_size"],
            "expectancy": metrics["expectancy"],
            "profit_factor": metrics["profit_factor"],
            "max_drawdown": -0.1,
            "classification": metrics.get("classification", "EXACT_CONFIRMED_STRONG"),
            "fallback_used": False,
            "regime_bridged": "TRENDING",
            "notes": "",
        }

    monkeypatch.setattr(build179, "_run_exact_requirement", fake_run_requirement)


def test_volatility_profile_is_weak_when_high_bucket_has_only_one_survivor(monkeypatch, tmp_path: Path) -> None:
    root = _root(tmp_path)
    symbols = ["TSLA", "META", "AMZN", "AAPL", "MSFT", "JPM"]
    _write_validator(root, symbols)
    _write_bars(tmp_path, symbols)
    _patch_replay(
        monkeypatch,
        {
            "TSLA": {"sample_size": 80, "expectancy": 0.003, "profit_factor": 1.5},
            "META": {"sample_size": 80, "expectancy": 0.0005, "profit_factor": 1.1, "classification": "EXACT_BACKTEST_WEAK"},
            "AMZN": {"sample_size": 80, "expectancy": -0.001, "profit_factor": 0.9, "classification": "EXACT_FAILED"},
            "AAPL": {"sample_size": 80, "expectancy": -0.001, "profit_factor": 0.9, "classification": "EXACT_FAILED"},
            "MSFT": {"sample_size": 80, "expectancy": -0.001, "profit_factor": 0.9, "classification": "EXACT_FAILED"},
            "JPM": {"sample_size": 80, "expectancy": -0.001, "profit_factor": 0.9, "classification": "EXACT_FAILED"},
        },
    )

    report = build179.build_volatility_profile_expansion(root=root, created_at=NOW, repo_root=tmp_path)

    assert report["overall_classification"] == "VOLATILITY_WEAK"
    buckets = {row["volatility_bucket"]: row for row in report["bucket_results"]}
    assert buckets["HIGH_VOLATILITY"]["bucket_classification"] == "VOLATILITY_WEAK"
    assert buckets["HIGH_VOLATILITY"]["symbols_blocked"] == 2
    assert any(row["symbol"] == "NFLX" and row["exact_classification"] == "EXACT_BLOCKED" for row in report["volatility_failures"])


def test_volatility_profile_writes_requested_outputs(monkeypatch, tmp_path: Path) -> None:
    root = _root(tmp_path)
    symbols = ["TSLA"]
    _write_validator(root, symbols)
    _write_bars(tmp_path, symbols)
    _patch_replay(monkeypatch, {"TSLA": {"sample_size": 80, "expectancy": 0.003, "profit_factor": 1.5}})

    report = build179.run_volatility_profile_expansion(root=root, created_at=NOW, repo_root=tmp_path)
    out = root / build179.REPORT_DIRNAME

    assert report["authority_boundary"]["fallback_allowed"] is False
    for name in ["bucket_results.csv", "volatility_survivors.csv", "volatility_failures.csv", "bucket_cost_analysis.csv"]:
        assert (out / name).exists()
    with (out / "bucket_results.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert {row["volatility_bucket"] for row in rows} == set(build179.VOLATILITY_BUCKETS)
