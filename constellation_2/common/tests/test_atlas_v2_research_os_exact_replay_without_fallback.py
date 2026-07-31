from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os import exact_replay_without_fallback as build099


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    (root / "reversal_trending_exact_coverage_plan").mkdir(parents=True)
    (root / "exact_coverage_import_validation").mkdir(parents=True)
    return root


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=build099.RESULT_COLUMNS if False else [
            "candidate_id", "family_id", "mechanism", "regime", "timeframe", "source", "symbol", "universe",
            "required_start", "required_end", "available_exact_file", "available_exact_rows", "available_exact_start",
            "available_exact_end", "available_fallback_file", "fallback_used", "coverage_status", "coverage_gap_reason", "priority",
        ])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _coverage_row(candidate_id="cand_1", family_id="family_55443d63b32328bd", symbol="SPY", timeframe="30m", *, fallback=False, exact=True):
    return {
        "candidate_id": candidate_id,
        "family_id": family_id,
        "mechanism": "REVERSAL",
        "regime": "TRENDING",
        "timeframe": timeframe,
        "source": "SCREEN_REPLAY",
        "symbol": symbol,
        "universe": symbol,
        "required_start": "2023-01-01",
        "required_end": "2023-03-31",
        "available_exact_file": f"/tmp/{symbol}_{timeframe}.csv" if exact else "",
        "available_exact_rows": "100" if exact else "",
        "available_exact_start": "2023-01-01" if exact else "",
        "available_exact_end": "2023-03-31" if exact else "",
        "available_fallback_file": f"/tmp/{symbol}_daily.csv" if fallback else "",
        "fallback_used": str(fallback).lower(),
        "coverage_status": "EXACT_COVERAGE_AVAILABLE" if exact else ("FALLBACK_ONLY" if fallback else "MISSING_TIMEFRAME"),
        "coverage_gap_reason": "exact symbol/timeframe file exists locally" if exact else "exact file missing",
        "priority": "P1",
    }


def _write_inputs(root: Path, coverage_rows: list[dict[str, object]], validations: list[dict[str, object]]) -> None:
    _write_csv(root / "reversal_trending_exact_coverage_plan" / "coverage_matrix.csv", coverage_rows)
    (root / "exact_coverage_import_validation" / "latest.json").write_text(json.dumps({"file_validations": validations}), encoding="utf-8")


def _patch_replay(monkeypatch, metrics_by_candidate: dict[str, dict[str, object]] | None = None):
    rows = [{"timestamp": f"2023-01-{(i % 28) + 1:02d}T09:30:00Z", "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "volume": 1000} for i in range(90)]
    monkeypatch.setattr(build099, "normalize_market_data_csv", lambda path, symbol, timeframe: rows)

    def fake_run(spec, data_rows, *, created_at=None):
        candidate_id = spec["candidate_id"]
        metrics = (metrics_by_candidate or {}).get(candidate_id, {"sample_size": 60, "expectancy": 0.01, "profit_factor": 1.3, "max_drawdown": -0.02})
        return {"metrics": metrics, "warnings": [], "missing_data": [], "classification": "BACKTEST_SUPPORTED", "backtest_spec": spec}

    monkeypatch.setattr(build099, "run_candidate_backtest_spec", fake_run)


def test_only_build_098_valid_ready_and_warning_files_are_used(monkeypatch, tmp_path):
    root = _root(tmp_path)
    coverage = [_coverage_row("cand_ready", symbol="SPY"), _coverage_row("cand_invalid", symbol="BAC")]
    validations = [
        {"candidate_id": "cand_ready", "symbol": "SPY", "timeframe": "30m", "status": "VALID_READY", "data_file": "/tmp/SPY_30m.csv"},
        {"candidate_id": "cand_invalid", "symbol": "BAC", "timeframe": "30m", "status": "INVALID", "data_file": "/tmp/BAC_30m.csv"},
    ]
    _write_inputs(root, coverage, validations)
    _patch_replay(monkeypatch)
    report = build099.run_exact_replay_without_fallback(root=root, created_at="2026-06-06T00:00:00Z", repo_root=tmp_path)
    assert [row["candidate_id"] for row in report["candidate_results"]] == ["cand_ready"]
    assert report["blocked_exact_replays"][0]["candidate_id"] == "cand_invalid"
    assert report["blocked_exact_replays"][0]["blocker"] == "INVALID_EXACT_FILE"


def test_missing_and_all_fallback_paths_block_without_replay(monkeypatch, tmp_path):
    root = _root(tmp_path)
    coverage = [
        _coverage_row("missing", symbol="MSFT", exact=False),
        _coverage_row("daily_fallback", symbol="BAC", fallback=True, exact=False),
        _coverage_row("alternate_timeframe", symbol="QQQ", timeframe="30m", exact=False),
        _coverage_row("alternate_symbol", symbol="TLT", timeframe="30m", exact=False),
    ]
    validations = [
        {"candidate_id": "daily_fallback", "symbol": "BAC", "timeframe": "daily", "status": "VALID_READY", "data_file": "/tmp/BAC_daily.csv"},
        {"candidate_id": "alternate_timeframe", "symbol": "QQQ", "timeframe": "5m", "status": "VALID_READY", "data_file": "/tmp/QQQ_5m.csv"},
        {"candidate_id": "alternate_symbol", "symbol": "SPY", "timeframe": "30m", "status": "VALID_READY", "data_file": "/tmp/SPY_30m.csv"},
    ]
    _write_inputs(root, coverage, validations)
    _patch_replay(monkeypatch)
    report = build099.run_exact_replay_without_fallback(root=root, created_at="2026-06-06T00:00:00Z", repo_root=tmp_path)
    blockers = {row["candidate_id"]: row["blocker"] for row in report["blocked_exact_replays"]}
    assert report["candidate_results"] == []
    assert blockers["missing"] == "MISSING_EXACT_FILE"
    assert blockers["daily_fallback"] == "FALLBACK_REQUIRED"
    assert blockers["alternate_timeframe"] == "MISSING_EXACT_FILE"
    assert blockers["alternate_symbol"] == "MISSING_EXACT_FILE"


def test_fallback_used_is_always_false_and_outputs_are_written(monkeypatch, tmp_path):
    root = _root(tmp_path)
    _write_inputs(root, [_coverage_row()], [{"candidate_id": "cand_1", "symbol": "SPY", "timeframe": "30m", "status": "VALID_WITH_WARNINGS", "data_file": "/tmp/SPY_30m.csv"}])
    _patch_replay(monkeypatch)
    report = build099.run_exact_replay_without_fallback(root=root, created_at="2026-06-06T00:00:00Z", repo_root=tmp_path)
    out = root / build099.REPORT_DIRNAME
    assert report["candidate_results"][0]["fallback_used"] is False
    assert (out / "latest.json").exists()
    assert (out / "latest_summary.md").exists()
    assert (out / "exact_replay_results.csv").exists()
    assert (out / "family_exact_repeatability.csv").exists()
    assert (out / "blocked_exact_replay.csv").exists()
    with (out / "exact_replay_results.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["fallback_used"] == "false"


def test_candidate_and_family_classifications_are_deterministic(monkeypatch, tmp_path):
    assert build099.classify_candidate_exact_replay(60, 0.01, 1.3) == "EXACT_CONFIRMED_STRONG"
    assert build099.classify_candidate_exact_replay(60, 0.01, 1.22) == "EXACT_CONFIRMED_WEAK"
    assert build099.classify_candidate_exact_replay(60, 0.01, 1.1) == "EXACT_BACKTEST_WEAK"
    assert build099.classify_candidate_exact_replay(10, 0.01, 2.0) == "EXACT_INSUFFICIENT_SAMPLE"
    assert build099.classify_candidate_exact_replay(60, -0.01, 2.0) == "EXACT_FAILED"
    rows = [{"classification": "EXACT_CONFIRMED_STRONG"}, {"classification": "EXACT_CONFIRMED_WEAK"}]
    assert build099.classify_family_repeatability(rows, 0) == "EXACT_REPEATABLE_STRONG"
    assert build099.classify_family_repeatability([{"classification": "EXACT_CONFIRMED_WEAK"}, {"classification": "EXACT_CONFIRMED_WEAK"}], 0) == "EXACT_REPEATABLE_WEAK"
    assert build099.classify_family_repeatability([{"classification": "EXACT_CONFIRMED_WEAK"}], 1) == "EXACT_PARTIALLY_REPEATABLE"
    assert build099.classify_family_repeatability([{"classification": "EXACT_FAILED"}, {"classification": "EXACT_FAILED"}], 0) == "EXACT_NOT_REPEATABLE"


def test_confidence_impact_defaults_none_and_small_increase_requires_repeatability(monkeypatch, tmp_path):
    root = _root(tmp_path)
    _write_inputs(root, [_coverage_row("single")], [{"candidate_id": "single", "symbol": "SPY", "timeframe": "30m", "status": "VALID_READY", "data_file": "/tmp/SPY_30m.csv"}])
    _patch_replay(monkeypatch)
    report = build099.run_exact_replay_without_fallback(root=root, created_at="2026-06-06T00:00:00Z", repo_root=tmp_path)
    assert report["summary"]["confidence_impact"] == "NONE"

    root2 = _root(tmp_path / "repeatable")
    coverage = [_coverage_row("a", symbol="SPY"), _coverage_row("b", symbol="QQQ")]
    validations = [
        {"candidate_id": "a", "symbol": "SPY", "timeframe": "30m", "status": "VALID_READY", "data_file": "/tmp/SPY_30m.csv"},
        {"candidate_id": "b", "symbol": "QQQ", "timeframe": "30m", "status": "VALID_READY", "data_file": "/tmp/QQQ_30m.csv"},
    ]
    _write_inputs(root2, coverage, validations)
    _patch_replay(monkeypatch, {"a": {"sample_size": 60, "expectancy": 0.01, "profit_factor": 1.3, "max_drawdown": -0.02}, "b": {"sample_size": 60, "expectancy": 0.01, "profit_factor": 1.22, "max_drawdown": -0.02}})
    repeatable = build099.run_exact_replay_without_fallback(root=root2, created_at="2026-06-06T00:00:00Z", repo_root=tmp_path)
    assert repeatable["summary"]["confidence_impact"] == "SMALL_INCREASE"


def test_no_forbidden_authority_fields_are_emitted(monkeypatch, tmp_path):
    root = _root(tmp_path)
    _write_inputs(root, [_coverage_row()], [{"candidate_id": "cand_1", "symbol": "SPY", "timeframe": "30m", "status": "VALID_READY", "data_file": "/tmp/SPY_30m.csv"}])
    _patch_replay(monkeypatch)
    report = build099.run_exact_replay_without_fallback(root=root, created_at="2026-06-06T00:00:00Z", repo_root=tmp_path)
    forbidden_keys = {"live_trading", "broker", "capital", "position_sizing", "trade_recommendation", "paper_placement", "candidate_promotion", "production_promotion"}

    def walk(value):
        if isinstance(value, dict):
            for key, child in value.items():
                assert key not in forbidden_keys
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(report["candidate_results"])
    walk(report["family_repeatability"])
    walk(report["blocked_exact_replays"])
