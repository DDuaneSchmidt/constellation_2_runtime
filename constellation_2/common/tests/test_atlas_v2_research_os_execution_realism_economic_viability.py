from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os import execution_realism_economic_viability as build126


def _write_ohlcv(path: Path, volumes: list[int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
        writer.writeheader()
        for i, volume in enumerate(volumes):
            writer.writerow({"timestamp": f"2023-01-01T{i:02d}:00:00Z", "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": volume})


def _seed_inputs(root: Path, data_root: Path, *, expectancy: float = 0.002, profit_factor: float = 1.5) -> None:
    (root / "exact_replay_without_fallback").mkdir(parents=True)
    (root / "net_of_cost_evidence").mkdir(parents=True)
    (root / "edge_magnitude_estimation").mkdir(parents=True)
    (root / "forward_observation_scoreboard").mkdir(parents=True)
    (root / "databento_import").mkdir(parents=True)
    aapl = data_root / "AAPL_30m.csv"
    spy = data_root / "SPY_30m.csv"
    _write_ohlcv(aapl, [600000, 650000, 700000, 620000] * 20)
    _write_ohlcv(spy, [900000, 850000, 950000, 875000] * 20)
    exact = {
        "summary": {"confidence_impact": "SMALL_INCREASE"},
        "candidate_results": [
            {"candidate_id": "c1", "family_id": build126.PRIMARY_FAMILY_ID, "symbol": "AAPL", "timeframe": "30m", "data_file": str(aapl), "sample_size": 80, "expectancy": expectancy, "profit_factor": profit_factor, "classification": "EXACT_CONFIRMED_STRONG"},
            {"candidate_id": "c2", "family_id": build126.PRIMARY_FAMILY_ID, "symbol": "SPY", "timeframe": "30m", "data_file": str(spy), "sample_size": 90, "expectancy": expectancy, "profit_factor": profit_factor, "classification": "EXACT_CONFIRMED_STRONG"},
        ],
        "family_repeatability": [{"family_id": build126.PRIMARY_FAMILY_ID, "family_classification": "EXACT_REPEATABLE_STRONG"}],
    }
    net = {
        "family_results": [
            {"family_id": build126.PRIMARY_FAMILY_ID, "cost_model": "fixed_bps", "cost_bps": 0, "family_classification": "NET_SURVIVES_STRONG"},
            {"family_id": build126.PRIMARY_FAMILY_ID, "cost_model": "fixed_bps", "cost_bps": 10, "family_classification": "NET_SURVIVES_STRONG"},
        ],
        "summary": {"confidence_impact": "NONE"},
    }
    (root / "exact_replay_without_fallback" / "latest.json").write_text(json.dumps(exact), encoding="utf-8")
    (root / "net_of_cost_evidence" / "latest.json").write_text(json.dumps(net), encoding="utf-8")
    (root / "edge_magnitude_estimation" / "latest.json").write_text(json.dumps({"summary": {"could_any_family_plausibly_matter": True}}), encoding="utf-8")
    (root / "forward_observation_scoreboard" / "latest.json").write_text(json.dumps({"summary": {"forward_sample_size": 0}}), encoding="utf-8")
    (root / "databento_import" / "databento_import_manifest.csv").write_text("symbol,timeframe,rows_written,data_start,data_end,path\n", encoding="utf-8")


def test_outputs_are_deterministic_and_required_files_are_written(tmp_path):
    root = tmp_path / "reports" / "atlas_v2_research_os"
    _seed_inputs(root, tmp_path / "data")
    first = build126.run_execution_realism_economic_viability(root=root, created_at="2026-06-06T00:00:00Z")
    second = build126.run_execution_realism_economic_viability(root=root, created_at="2026-06-06T00:00:00Z")
    assert first == second
    out = root / build126.REPORT_DIRNAME
    for name in ["latest.json", "latest_summary.md", "liquidity_assessment.csv", "spread_sensitivity.csv", "slippage_sensitivity.csv", "capacity_assessment.csv", "execution_robustness.csv"]:
        assert (out / name).exists()


def test_no_trading_authority_or_sizing_recommendations_are_emitted(tmp_path):
    root = tmp_path / "reports" / "atlas_v2_research_os"
    _seed_inputs(root, tmp_path / "data")
    report = build126.run_execution_realism_economic_viability(root=root, created_at="2026-06-06T00:00:00Z")
    serialized = json.dumps(report, sort_keys=True).lower()
    assert "research-only" in serialized
    assert "recommended_size" not in serialized
    assert "allocation_amount" not in serialized
    assert "order_quantity" not in serialized
    assert "broker_order" not in serialized


def test_confidence_rules_require_exact_net_and_favorable_execution(tmp_path):
    root = tmp_path / "reports" / "atlas_v2_research_os"
    _seed_inputs(root, tmp_path / "data")
    favorable = build126.run_execution_realism_economic_viability(root=root, created_at="2026-06-06T00:00:00Z")
    assert favorable["summary"]["confidence_impact"] == "SMALL_INCREASE"

    weak_root = tmp_path / "weak" / "reports" / "atlas_v2_research_os"
    _seed_inputs(weak_root, tmp_path / "weak_data", expectancy=0.00001, profit_factor=1.01)
    net_path = weak_root / "net_of_cost_evidence" / "latest.json"
    net = json.loads(net_path.read_text(encoding="utf-8"))
    net["family_results"][-1]["family_classification"] = "COST_ERODED"
    net_path.write_text(json.dumps(net), encoding="utf-8")
    weak = build126.run_execution_realism_economic_viability(root=weak_root, created_at="2026-06-06T00:00:00Z")
    assert weak["summary"]["confidence_impact"] in {"NONE", "DECREASE"}


def test_classification_helpers_are_stable():
    assert build126.classify_liquidity(600000, 200000, 0.2, 0.5, 100) == "HIGH_LIQUIDITY"
    assert build126.classify_liquidity(150000, 50000, 0.4, 0.3, 100) == "MODERATE_LIQUIDITY"
    assert build126.classify_liquidity(1, 1, 1, 0, 10) == "INSUFFICIENT_DATA"
    assert build126.classify_slippage(5, 5, 10) == "SLIPPAGE_ROBUST"
    assert build126.classify_slippage(2, 1, 10) == "SLIPPAGE_SENSITIVE"
    assert build126.classify_slippage(10, 0, 10) == "SLIPPAGE_ERODED"
    assert build126.classify_capacity("HIGH_LIQUIDITY", "LOW", "30m", 0.1) == "HIGH_CAPACITY"
    assert build126.classify_execution(85, "HIGH_LIQUIDITY") == "EXECUTION_ROBUST"
