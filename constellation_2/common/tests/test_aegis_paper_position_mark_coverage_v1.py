from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.evidence_lineage_integrity_v1 import build_evidence_lineage_integrity_v1, integrity_failures_v1
from ops.aegis.market_data_coverage_v1 import build_market_data_coverage_v1
from ops.aegis.market_data_demand_v1 import build_market_data_demand_v1

DAY = "2026-06-02"


def _write(root: Path, family: str, filename: str, payload: dict, *, day: str = DAY) -> None:
    path = root / "reports" / family / day / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _symbol_map(root: Path, symbols: list[str] | None = None) -> None:
    symbols = symbols or ["SPY", "QQQ", "VIX"]
    _write(root, "aegis_symbol_map_v1", "symbol_map.v1.json", {"day_utc": DAY, "required_symbols": symbols, "raw_signal_symbols": [], "symbols": {symbol: {"symbol": symbol} for symbol in symbols}})


def _contracts(root: Path) -> None:
    _write(root, "aegis_sleeve_input_contracts_v1", "sleeve_input_contracts.v1.json", {"day_utc": DAY, "contracts": [{"sleeve_id": "CTX", "required_inputs": [{"data_item_id": "market.price.SPY"}], "optional_inputs": [{"data_item_id": "market.volatility.VIX"}]}]})


def _ledger(root: Path, positions: list[dict]) -> None:
    normalized = []
    for idx, row in enumerate(positions):
        normalized.append({"position_id": row.get("position_id", f"pos-{idx}"), "symbol": row.get("symbol", ""), "entry_price": row.get("entry_price", "100"), "quantity": row.get("quantity", "1"), "current_status": "OPEN", "sleeve_id": row.get("sleeve_id", "SLEEVE")})
    _write(root, "aegis_paper_position_ledger_v1", "paper_position_ledger.v1.json", {"day_utc": DAY, "positions": normalized, "open_positions": normalized, "closed_positions": []})


def _market(root: Path, symbols: dict[str, dict]) -> None:
    rows = {}
    for symbol, extra in symbols.items():
        row = {"symbol": symbol, "last_price": 101, "freshness_status": "CURRENT", "market_session_date": DAY, "data_timestamp_utc": f"{DAY}T15:00:00Z", "source_url_or_path": f"/raw/{symbol}.json"}
        row.update(extra)
        rows[symbol] = row
    _write(root, "aegis_market_data_v1", "market_data.v1.json", {"day_utc": DAY, "symbols": rows, "provider_attempts": []})


def _inputs_and_registry(root: Path, symbols: list[str]) -> None:
    _write(root, "market_data_inputs_v1", "market_data_inputs.v1.json", {"day_utc": DAY, "input_records": [{"data_item_id": f"market.price.{s}", "symbol": s, "validation_status": "VALID", "market_session_date": DAY} for s in symbols]})
    _write(root, "aegis_data_registry_v1", "data_registry.v1.json", {"day_utc": DAY, "data_items": [{"data_item_id": f"market.price.{s}", "status": "CURRENT", "market_session_date": DAY} for s in symbols]})


def test_open_paper_position_symbols_are_required_market_data_demand(tmp_path: Path) -> None:
    _symbol_map(tmp_path, ["SPY"])
    _contracts(tmp_path)
    _ledger(tmp_path, [{"symbol": "AAPL"}, {"symbol": "MSFT"}])

    payload = build_market_data_demand_v1(repo_root=Path.cwd(), truth_root=tmp_path, day_utc=DAY)

    assert "AAPL" in payload["required_symbols"]
    assert "MSFT" in payload["required_symbols"]
    assert "SPY" in payload["requested_symbols"]
    assert "open_paper_position_symbols" in payload["demand_sources"]
    assert payload["broker_execution_allowed"] is False
    assert payload["autonomous_execution_allowed"] is False
    assert payload["trade_advice_allowed"] is False


def test_broad_context_only_market_data_cannot_pass_position_coverage(tmp_path: Path) -> None:
    _symbol_map(tmp_path, ["SPY"])
    _contracts(tmp_path)
    _ledger(tmp_path, [{"symbol": "AAPL"}])
    _write(tmp_path, "aegis_market_data_demand_v1", "market_data_demand.v1.json", {"day_utc": DAY, "requested_symbols": ["SPY"], "required_symbols": []})
    _market(tmp_path, {"SPY": {}})
    _inputs_and_registry(tmp_path, ["SPY"])

    payload = build_market_data_coverage_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["status"] == "BLOCKED"
    assert payload["open_position_count"] == 1
    assert payload["marked_position_count"] == 0
    assert payload["mark_coverage_by_position_pct"] == 0.0
    assert payload["position_mark_rows"][0]["mark_status"] == "SYMBOL_NOT_REQUESTED"


def test_market_data_coverage_does_not_report_zero_open_positions_when_ledger_has_positions(tmp_path: Path) -> None:
    _symbol_map(tmp_path, ["SPY", "AAPL"])
    _contracts(tmp_path)
    _ledger(tmp_path, [{"symbol": "AAPL"}])
    _write(tmp_path, "aegis_market_data_demand_v1", "market_data_demand.v1.json", {"day_utc": DAY, "requested_symbols": ["SPY", "AAPL"], "required_symbols": ["AAPL"]})
    _market(tmp_path, {"SPY": {}})
    _inputs_and_registry(tmp_path, ["SPY"])

    payload = build_market_data_coverage_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["open_position_count"] == 1
    assert payload["missing_mark_position_count"] == 1
    assert payload["position_mark_rows"][0]["mark_status"] == "SYMBOL_NOT_IN_MARKET_DATA"


def test_missing_ledger_input_invalidates_coverage_instead_of_reporting_green(tmp_path: Path) -> None:
    _symbol_map(tmp_path, ["SPY"])
    _contracts(tmp_path)
    _write(tmp_path, "aegis_market_data_demand_v1", "market_data_demand.v1.json", {"day_utc": DAY, "requested_symbols": ["SPY"], "required_symbols": []})
    _market(tmp_path, {"SPY": {}})
    _inputs_and_registry(tmp_path, ["SPY"])

    payload = build_market_data_coverage_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["status"] == "INVALID"
    assert payload["coverage_status"] == "INVALID"
    assert payload["invalid_reason_codes"] == ["STALE_OR_MISSING_LEDGER_INPUT"]
    assert payload["mark_coverage_by_position_pct"] == 0.0


def test_missing_mark_timestamp_and_position_exclusions_have_explicit_reason_codes(tmp_path: Path) -> None:
    _symbol_map(tmp_path, ["AAPL"])
    _contracts(tmp_path)
    _ledger(tmp_path, [{"symbol": "AAPL"}, {"symbol": ""}])
    _write(tmp_path, "aegis_market_data_demand_v1", "market_data_demand.v1.json", {"day_utc": DAY, "requested_symbols": ["AAPL"], "required_symbols": ["AAPL"]})
    _market(tmp_path, {"AAPL": {"data_timestamp_utc": "", "source_timestamp_utc": "", "retrieved_at_utc": ""}})
    _inputs_and_registry(tmp_path, ["AAPL"])

    payload = build_market_data_coverage_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["position_mark_rows"][0]["mark_status"] == "MISSING_MARK_TIMESTAMP"
    assert payload["position_exclusion_rows"][0]["mark_status"] == "POSITION_EXCLUDED_WITH_REASON"
    assert payload["position_exclusion_rows"][0]["exclusion_reason"] == "MISSING_POSITION_SYMBOL"


def test_evidence_lineage_fails_closed_for_missing_marks(tmp_path: Path) -> None:
    _write(tmp_path, "aegis_candidate_contracts_v1", "candidate_contracts.v1.json", {"day_utc": DAY, "candidate_contracts": [{"candidate_id": "c1", "raw_signal_id": "s1", "sleeve_id": "SLEEVE", "created_at_utc": f"{DAY}T12:00:00Z"}]})
    position = {"position_id": "p1", "symbol": "AAPL", "candidate_id": "c1", "sleeve_id": "SLEEVE", "candidate_lineage": {"candidate_id": "c1", "sleeve_id": "SLEEVE", "raw_signal_id": "s1"}, "current_certified_mark": "", "mark_timestamp_utc": "", "mark_source_path": "", "mark_certification_status": "MISSING_MARK"}
    _write(tmp_path, "aegis_paper_position_ledger_v1", "paper_position_ledger.v1.json", {"day_utc": DAY, "positions": [position], "open_positions": [position], "closed_positions": []})
    _write(tmp_path, "aegis_research_validation_samples_v1", "research_validation_samples.v1.json", {"samples": []})
    _write(tmp_path, "aegis_research_validation_result_v1", "research_validation_result.v1.json", {"results": []})
    _write(tmp_path, "aegis_sleeve_analytics_v1", "sleeve_analytics.v1.json", {"summary": {}})
    _write(tmp_path, "aegis_sleeve_performance_truth_v1", "sleeve_performance_truth.v1.json", {"sleeves": []})

    payload = build_evidence_lineage_integrity_v1(truth_root=tmp_path, day_utc=DAY)
    failures = integrity_failures_v1(payload)

    assert payload["evidence_coverage_panel"]["mark_coverage_pct"] == 0.0
    assert any("mark_coverage_integrity" in failure for failure in failures)
    assert payload["trade_advice_allowed"] is False
    assert payload["broker_execution_allowed"] is False
