from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os import mechanism_expansion_program as mep


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    (root / "candidate_family_discovery").mkdir(parents=True)
    (root / "exact_data_repair_loop").mkdir(parents=True)
    return root


def _write_inputs(root: Path, *, symbols=("AAPL", "TSLA"), include_tsla=True) -> None:
    families = [
        {
            "family_id": "family_breakout_30m",
            "mechanism": "BREAKOUT",
            "regime": "CHOP",
            "dominant_timeframe": "30M",
            "candidate_ids": ["cand_breakout"],
        },
        {
            "family_id": "family_reversal_30m",
            "mechanism": "REVERSAL",
            "regime": "TRENDING",
            "dominant_timeframe": "30M",
            "candidate_ids": ["cand_reversal"],
        },
        {
            "family_id": "family_mean_1h",
            "mechanism": "MEAN_REVERSION",
            "regime": "TRENDING",
            "dominant_timeframe": "1H",
            "candidate_ids": ["cand_mean"],
        },
        {
            "family_id": "family_event_30m",
            "mechanism": "EVENT_REACTION",
            "regime": "CHOP",
            "dominant_timeframe": "30M",
            "candidate_ids": ["cand_event"],
        },
    ]
    (root / "candidate_family_discovery" / "latest.json").write_text(json.dumps(families), encoding="utf-8")
    validations = []
    for symbol in symbols:
        if symbol == "TSLA" and not include_tsla:
            continue
        for timeframe in ["30m", "1h"]:
            validations.append(
                {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "validation_status": "VALID_READY",
                    "revalidated_file": f"/tmp/{symbol}_{timeframe}.csv",
                }
            )
    (root / "exact_data_repair_loop" / "latest.json").write_text(json.dumps({"revalidation_matrix": validations}), encoding="utf-8")


def _patch_replay(monkeypatch):
    rows = [
        {"timestamp": f"2023-01-{(idx % 28) + 1:02d}T09:30:00Z", "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "volume": 1000}
        for idx in range(120)
    ]
    monkeypatch.setattr(mep, "normalize_market_data_csv", lambda path, symbol, timeframe: rows)

    def fake_run(spec, data_rows, *, created_at=None):
        mechanism = spec["mechanism"]
        symbol = spec["candidate_symbol"]
        if mechanism == "BREAKOUT" and symbol == "AAPL":
            metrics = {"sample_size": 80, "expectancy": 0.0012, "profit_factor": 1.5, "max_drawdown": -0.02}
        elif mechanism == "REVERSAL" and symbol == "TSLA":
            metrics = {"sample_size": 80, "expectancy": 0.0010, "profit_factor": 1.45, "max_drawdown": -0.03}
        elif mechanism == "MEAN_REVERSION":
            metrics = {"sample_size": 80, "expectancy": 0.00018, "profit_factor": 1.08, "max_drawdown": -0.02}
        elif mechanism == "EVENT_REACTION":
            metrics = {"sample_size": 30, "expectancy": 0.001, "profit_factor": 1.4, "max_drawdown": -0.01}
        else:
            metrics = {"sample_size": 80, "expectancy": -0.0002, "profit_factor": 0.9, "max_drawdown": -0.05}
        return {"metrics": metrics, "warnings": [], "missing_data": []}

    monkeypatch.setattr(mep, "run_candidate_backtest_spec", fake_run)


def test_exact_replay_uses_no_fallback_and_reports_blocked_missing_exact(monkeypatch, tmp_path):
    root = _root(tmp_path)
    _write_inputs(root, include_tsla=False)
    monkeypatch.setattr(mep, "TARGET_SYMBOLS", ["AAPL", "TSLA"])
    _patch_replay(monkeypatch)

    report = mep.run_mechanism_expansion_program(root=root, created_at="2026-06-06T00:00:00Z", repo_root=tmp_path)

    assert report["summary"]["fallback_used_count"] == 0
    assert all(row["fallback_used"] is False for row in report["exact_replay_results"])
    assert report["blocked_mechanism_tests"]
    assert {row["blocker"] for row in report["blocked_mechanism_tests"]} == {"MISSING_EXACT_FILE"}


def test_cost_adjustment_is_applied_and_cost_erodes_weak_edges(monkeypatch, tmp_path):
    root = _root(tmp_path)
    _write_inputs(root)
    monkeypatch.setattr(mep, "TARGET_SYMBOLS", ["AAPL", "TSLA"])
    _patch_replay(monkeypatch)

    report = mep.run_mechanism_expansion_program(root=root, created_at="2026-06-06T00:00:00Z", repo_root=tmp_path)
    mean_rows = [row for row in report["cost_adjusted_mechanism_results"] if row["mechanism"] == "MEAN_REVERSION" and row["cost_bps"] == 25.0]

    assert mean_rows
    assert all(row["net_expectancy"] < row["gross_expectancy"] for row in mean_rows)
    assert {row["classification"] for row in mean_rows} == {"MECHANISM_COST_ERODED"}


def test_results_are_deterministic(monkeypatch, tmp_path):
    root = _root(tmp_path)
    _write_inputs(root)
    monkeypatch.setattr(mep, "TARGET_SYMBOLS", ["AAPL", "TSLA"])
    _patch_replay(monkeypatch)

    first = mep.run_mechanism_expansion_program(root=root, created_at="2026-06-06T00:00:00Z", repo_root=tmp_path)
    second = mep.run_mechanism_expansion_program(root=root, created_at="2026-06-06T00:00:00Z", repo_root=tmp_path)

    assert first == second


def test_required_reports_are_written(monkeypatch, tmp_path):
    root = _root(tmp_path)
    _write_inputs(root)
    monkeypatch.setattr(mep, "TARGET_SYMBOLS", ["AAPL", "TSLA"])
    _patch_replay(monkeypatch)

    mep.run_mechanism_expansion_program(root=root, created_at="2026-06-06T00:00:00Z", repo_root=tmp_path)
    out = root / mep.REPORT_DIRNAME
    for filename in [
        "latest.json",
        "latest_summary.md",
        "mechanism_survivor_matrix.csv",
        "family_mechanism_results.csv",
        "symbol_mechanism_results.csv",
        "cost_adjusted_mechanism_results.csv",
        "blocked_mechanism_tests.csv",
        "survivor_density_report.csv",
    ]:
        assert (out / filename).exists()
    with (out / "cost_adjusted_mechanism_results.csv").open(newline="", encoding="utf-8") as handle:
        assert list(csv.DictReader(handle))


def test_no_trading_or_promotion_authority_emitted(monkeypatch, tmp_path):
    root = _root(tmp_path)
    _write_inputs(root)
    monkeypatch.setattr(mep, "TARGET_SYMBOLS", ["AAPL", "TSLA"])
    _patch_replay(monkeypatch)

    report = mep.run_mechanism_expansion_program(root=root, created_at="2026-06-06T00:00:00Z", repo_root=tmp_path)
    authority = report["authority_boundary"]

    assert authority["research_only"] is True
    assert "live trading" in authority["forbidden_actions"]
    assert "candidate promotion" in authority["forbidden_actions"]
    assert "production promotion" in authority["forbidden_actions"]


def test_mechanism_density_answers_outside_tsla_reversal(monkeypatch, tmp_path):
    root = _root(tmp_path)
    _write_inputs(root)
    monkeypatch.setattr(mep, "TARGET_SYMBOLS", ["AAPL", "TSLA"])
    _patch_replay(monkeypatch)

    report = mep.run_mechanism_expansion_program(root=root, created_at="2026-06-06T00:00:00Z", repo_root=tmp_path)

    assert report["answers"]["survivors_outside_tsla_reversal"] == "YES"
    assert report["summary"]["best_mechanism"]
    assert any(row["mechanism_classification"] == "MECHANISM_PROMISING" for row in report["survivor_density_report"])
