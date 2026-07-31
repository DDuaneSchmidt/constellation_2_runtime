from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.market_data_demand_v1 import build_market_data_demand_v1
from ops.aegis.market_data_universe_consistency_v1 import build_market_data_universe_consistency_v1

DAY = "2026-06-01"


def _seed(root: Path, *, demand_symbols=None, returned_symbols=None, certified_symbols=None) -> None:
    demand_symbols = demand_symbols or ["SPY"]
    returned_symbols = returned_symbols or demand_symbols
    certified_symbols = certified_symbols or returned_symbols
    write_json_v1(root / f"reports/aegis_market_data_demand_v1/{DAY}/market_data_demand.v1.json", {"requested_symbols": demand_symbols, "required_symbols": demand_symbols, "demand_rows": [{"symbol": s, "required": True, "consumer_sleeves": ["paper_position_marks"], "demand_sources": ["test"]} for s in demand_symbols]})
    write_json_v1(root / f"reports/aegis_market_data_v1/{DAY}/market_data.v1.json", {"market_session_date": DAY, "symbols": {s: {"freshness_status": "CURRENT", "market_session_date": DAY} for s in returned_symbols}})
    write_json_v1(root / f"reports/aegis_market_data_coverage_v1/{DAY}/market_data_coverage.v1.json", {"certified_symbols": certified_symbols, "open_position_count": 1, "marked_position_count": 1, "mark_coverage_by_position_pct": 100.0})
    write_json_v1(root / f"reports/aegis_paper_position_ledger_v1/{DAY}/paper_position_ledger.v1.json", {"day_utc": DAY, "open_positions": [{"symbol": "SPY"}]})
    write_json_v1(root / f"reports/aegis_oil_shock_candidate_producer_v1/{DAY}/oil_shock_candidate_producer.v1.json", {"required_market_symbols": ["DBC", "SPY", "USO", "XLE"], "producer_status": "SYSTEM_DATA_PIPELINE_REQUIRED"})


def test_open_paper_position_symbols_enter_consolidated_universe(tmp_path: Path) -> None:
    _seed(tmp_path, demand_symbols=["SPY", "DBC", "USO", "XLE"])
    payload = build_market_data_universe_consistency_v1(truth_root=tmp_path, day_utc=DAY)
    assert "SPY" in payload["consolidated_required_symbols"]
    assert payload["paper_position_symbol_coverage"]["coverage_status"] == "COMPLETE"


def test_oil_shock_required_symbols_enter_consolidated_universe_and_missing_uso_detected(tmp_path: Path) -> None:
    _seed(tmp_path, demand_symbols=["DBC", "SPY", "XLE"], returned_symbols=["DBC", "SPY", "XLE"])
    payload = build_market_data_universe_consistency_v1(truth_root=tmp_path, day_utc=DAY)
    oil = payload["oil_shock_symbol_coverage"]
    assert set(["DBC", "SPY", "USO", "XLE"]) <= set(payload["consolidated_required_symbols"])
    assert oil["blocker_code"] == "SYMBOL_NOT_REQUESTED"
    assert oil["owner"] == "AEGIS_SYSTEM"
    assert "USO" in oil["missing_from_request"]


def test_uso_no_longer_blocks_oil_shock_if_requested_returned_certified(tmp_path: Path) -> None:
    _seed(tmp_path, demand_symbols=["DBC", "SPY", "USO", "XLE"], returned_symbols=["DBC", "SPY", "USO", "XLE"], certified_symbols=["DBC", "SPY", "USO", "XLE"])
    payload = build_market_data_universe_consistency_v1(truth_root=tmp_path, day_utc=DAY)
    oil = payload["oil_shock_symbol_coverage"]
    assert oil["missing_from_request"] == []
    assert oil["requested_but_not_returned"] == []
    assert oil["returned_but_not_certified"] == []
    assert oil["coverage_status"] == "COMPLETE"


def test_broad_context_only_market_data_cannot_pass(tmp_path: Path) -> None:
    _seed(tmp_path, demand_symbols=["SPY", "QQQ"], returned_symbols=["SPY", "QQQ"], certified_symbols=["SPY", "QQQ"])
    payload = build_market_data_universe_consistency_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["status"] == "INCOMPLETE"
    assert "USO" in payload["missing_required_symbols"]


def test_market_data_demand_includes_oil_shock_required_symbols(tmp_path: Path) -> None:
    write_json_v1(tmp_path / f"reports/aegis_symbol_map_v1/{DAY}/symbol_map.v1.json", {"required_symbols": ["SPY"], "raw_signal_symbols": []})
    write_json_v1(tmp_path / f"reports/aegis_sleeve_input_contracts_v1/{DAY}/sleeve_input_contracts.v1.json", {"contracts": []})
    write_json_v1(tmp_path / f"reports/aegis_candidate_contracts_v1/{DAY}/candidate_contracts.v1.json", {"candidate_contracts": []})
    write_json_v1(tmp_path / f"reports/aegis_paper_position_ledger_v1/{DAY}/paper_position_ledger.v1.json", {"day_utc": DAY, "open_positions": [{"symbol": "AAPL"}]})
    write_json_v1(tmp_path / f"reports/aegis_oil_shock_candidate_producer_v1/{DAY}/oil_shock_candidate_producer.v1.json", {"required_market_symbols": ["DBC", "SPY", "USO", "XLE"]})
    payload = build_market_data_demand_v1(repo_root=ROOT, truth_root=tmp_path, day_utc=DAY)
    assert set(["AAPL", "DBC", "SPY", "USO", "XLE"]) <= set(payload["required_symbols"])
    row = next(row for row in payload["demand_rows"] if row["symbol"] == "USO")
    assert row["required"] is True
    assert "oil_shock_candidate_flow" in row["consumer_sleeves"]


def test_safety_gates_remain_disabled(tmp_path: Path) -> None:
    _seed(tmp_path, demand_symbols=["DBC", "SPY", "USO", "XLE"])
    payload = build_market_data_universe_consistency_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["broker_execution_allowed"] is False
    assert payload["trade_advice_allowed"] is False
    assert payload["safety"]["safety_gates_changed"] is False
