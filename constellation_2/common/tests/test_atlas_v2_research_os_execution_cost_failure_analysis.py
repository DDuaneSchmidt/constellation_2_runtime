from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.execution_cost_failure_analysis import run_execution_cost_failure_analysis


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")


def _write_market_csv(path: Path, volume: int = 1_000_000) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
        writer.writeheader()
        for idx in range(60):
            writer.writerow({"timestamp": f"2025-01-{(idx % 28) + 1:02d}T10:00:00Z", "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": volume})


def test_execution_cost_failure_analysis_separates_cost_erosion_from_failed_and_tsla(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    tsla = tmp_path / "data" / "TSLA_30m.csv"
    spy = tmp_path / "data" / "SPY_30m.csv"
    _write_market_csv(tsla)
    _write_market_csv(spy)
    _write_json(root / "exact_replay_without_fallback" / "latest.json", {
        "candidate_results": [
            {"candidate_id": "small", "family_id": "fam", "symbol": "SPY", "timeframe": "30m", "sample_size": 100, "expectancy": 0.00005, "profit_factor": 1.05, "data_file": str(spy)},
            {"candidate_id": "failed", "family_id": "fam", "symbol": "SPY", "timeframe": "30m", "sample_size": 100, "expectancy": -0.0002, "profit_factor": 0.8, "data_file": str(spy)},
            {"candidate_id": "tsla_good", "family_id": "fam_tsla", "symbol": "TSLA", "timeframe": "30m", "sample_size": 200, "expectancy": 0.0015, "profit_factor": 1.5, "data_file": str(tsla)},
            {"candidate_id": "tsla_bad", "family_id": "fam_tsla", "symbol": "TSLA", "timeframe": "1h", "sample_size": 200, "expectancy": -0.0001, "profit_factor": 0.9, "data_file": str(tsla)},
        ]
    })
    _write_json(root / "net_of_cost_evidence" / "latest.json", {
        "sensitivity_matrix": [{"cost_scenario": "10bps", "cost_eroded": 1, "net_failed": 1}],
    })

    report = run_execution_cost_failure_analysis(root=root, created_at="2026-06-06T00:00:00Z")

    summary = report["summary"]
    assert summary["rows_cost_eroded_10bps"] == 1
    assert summary["rows_net_failed_10bps"] == 2
    assert summary["tsla_viability"]["classification"] == "POSSIBLY_VIABLE"
    modes = {row["failure_mode"]: row for row in report["execution_failure_modes"]}
    assert modes["10BPS_COST_EROSION"]["rows_affected"] == 1
    assert modes["PRE_EXISTING_NEGATIVE_EDGE"]["rows_affected"] == 2
    assert (root / "execution_cost_failure_analysis" / "latest.json").exists()
    assert (root / "execution_cost_failure_analysis" / "tsla_execution_viability.csv").exists()
    assert report["authority_boundary"]["trade_recommendations"] is False
