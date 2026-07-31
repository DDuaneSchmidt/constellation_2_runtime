from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.similar_symbol_expansion_program import (
    COMPARISON_COLUMNS,
    COST_COLUMNS,
    FAILURE_COLUMNS,
    SURVIVOR_COLUMNS,
    build_similar_symbol_expansion_program,
    write_similar_symbol_expansion_program,
)


def _seed_source(root: Path) -> None:
    source_dir = root / "controlled_similar_symbol_expansion"
    source_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "similar_symbol_comparison": [
            {
                "symbol": "TSLA",
                "family_id": "family_59cc928bca30cc44",
                "mechanism": "REVERSAL",
                "regime": "TRENDING",
                "timeframe": "30m",
                "sample_size": 1014,
                "expectancy": 0.001496,
                "profit_factor": 1.498734,
                "max_drawdown": -0.127069,
                "net_expectancy_10bps": 0.000496,
            },
            {
                "symbol": "META",
                "family_id": "family_59cc928bca30cc44",
                "mechanism": "REVERSAL",
                "regime": "TRENDING",
                "timeframe": "30m",
                "sample_size": 1134,
                "expectancy": 0.000336,
                "profit_factor": 1.140193,
                "max_drawdown": -0.196989,
                "net_expectancy_10bps": -0.000664,
            },
            {
                "symbol": "AMZN",
                "family_id": "family_59cc928bca30cc44",
                "mechanism": "REVERSAL",
                "regime": "TRENDING",
                "timeframe": "30m",
                "sample_size": 999,
                "expectancy": -0.000309,
                "profit_factor": 0.876927,
                "max_drawdown": -0.160899,
                "net_expectancy_10bps": -0.001309,
            },
        ],
        "blocked_symbols": [
            {
                "symbol": "NVDA",
                "family_id": "family_59cc928bca30cc44",
                "required_file": "/repo/data/manual_intraday_import/NVDA_30m.csv",
            },
            {
                "symbol": "AMD",
                "family_id": "family_59cc928bca30cc44",
                "required_file": "/repo/data/manual_intraday_import/AMD_30m.csv",
            },
            {
                "symbol": "NFLX",
                "family_id": "family_59cc928bca30cc44",
                "required_file": "/repo/data/manual_intraday_import/NFLX_30m.csv",
            },
        ],
    }
    (source_dir / "latest.json").write_text(json.dumps(payload), encoding="utf-8")


def test_similar_symbol_expansion_program_maps_requested_classifications(tmp_path: Path) -> None:
    _seed_source(tmp_path)

    report = build_similar_symbol_expansion_program(tmp_path, created_at="2026-06-06T00:00:00Z")
    by_symbol = {row["symbol"]: row for row in report["symbol_comparison"]}

    assert by_symbol["TSLA"]["classification"] == "SYMBOL_SURVIVOR"
    assert by_symbol["META"]["classification"] == "SYMBOL_WEAK"
    assert by_symbol["AMZN"]["classification"] == "SYMBOL_FAILED"
    assert by_symbol["NVDA"]["classification"] == "SYMBOL_FAILED"
    assert report["summary"]["non_tsla_survivors"] == []
    assert "TSLA remains unique" in report["summary"]["conclusion"]


def test_similar_symbol_expansion_program_writes_requested_files(tmp_path: Path) -> None:
    _seed_source(tmp_path)
    report = build_similar_symbol_expansion_program(tmp_path, created_at="2026-06-06T00:00:00Z")
    paths = write_similar_symbol_expansion_program(report, tmp_path)

    for key in ["symbol_comparison", "symbol_survivors", "symbol_failures", "symbol_cost_results", "expansion_summary"]:
        assert paths[key].exists()

    with paths["symbol_comparison"].open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == COMPARISON_COLUMNS
    with paths["symbol_survivors"].open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == SURVIVOR_COLUMNS
    with paths["symbol_failures"].open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == FAILURE_COLUMNS
    with paths["symbol_cost_results"].open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == COST_COLUMNS

    summary = paths["expansion_summary"].read_text(encoding="utf-8")
    assert "# Builds 171-174 - Similar Symbol Expansion Program" in summary
    assert "No live trading" in summary
