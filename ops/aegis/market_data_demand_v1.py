from __future__ import annotations

import csv
import hashlib
import json
from io import StringIO
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, write_json_v1
from ops.aegis.market_data.symbol_alias_registry_v1 import canonicalize_symbol_list_v1, normalize_market_symbol_v1
from ops.aegis.market_data.symbol_map_v1 import build_symbol_map_v1, symbol_map_entry_for_symbol_v1, write_symbol_map_v1

REPORT_FAMILY = "aegis_market_data_demand_v1"


def build_market_data_demand_v1(*, repo_root: Path, truth_root: Path, day_utc: str, symbol_map_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    repo = Path(repo_root).resolve()
    sources: dict[str, dict[str, Any]] = {}
    symbol_map_path, symbol_map = latest_json_v1(root, "aegis_symbol_map_v1", day_utc, "symbol_map.v1.json")
    if symbol_map_payload is not None:
        symbol_map = symbol_map_payload
    if not symbol_map:
        symbol_map = build_symbol_map_v1(repo_root=repo, truth_root=root, day_utc=day_utc)
        paths = write_symbol_map_v1(truth_root=root, day_utc=day_utc, payload=symbol_map)
        symbol_map_path = Path(paths["json"])
    contracts_path, contracts = latest_json_v1(root, "aegis_sleeve_input_contracts_v1", day_utc, "sleeve_input_contracts.v1.json")
    candidate_contracts_path, candidate_contracts = latest_json_v1(root, "aegis_candidate_contracts_v1", day_utc, "candidate_contracts.v1.json")
    paper_position_ledger_path, paper_position_ledger = latest_json_v1(root, "aegis_paper_position_ledger_v1", day_utc, "paper_position_ledger.v1.json")
    oil_producer_path, oil_producer = latest_json_v1(root, "aegis_oil_shock_candidate_producer_v1", day_utc, "oil_shock_candidate_producer.v1.json")
    evidence_packets_path, evidence_packets = latest_json_v1(root, "aegis_hypothesis_evidence_packet_v1", day_utc, "evidence_packets.v1.json")
    sources["symbol_map"] = _source_ref(symbol_map_path)
    sources["sleeve_input_contracts"] = _source_ref(contracts_path)
    sources["candidate_contracts"] = _source_ref(candidate_contracts_path)
    sources["paper_position_ledger"] = _source_ref(paper_position_ledger_path)
    sources["oil_shock_candidate_producer"] = _source_ref(oil_producer_path)
    sources["generated_hypothesis_evidence_packets"] = _source_ref(evidence_packets_path)

    demand_by_symbol: dict[str, dict[str, Any]] = {}
    _add_symbols(
        demand_by_symbol,
        symbols=symbol_map.get("raw_signal_symbols") if isinstance(symbol_map.get("raw_signal_symbols"), list) else [],
        source_type="raw_signal_symbols",
        required=False,
        consumer="raw_signal_registry",
        source_path=str(symbol_map_path or ""),
    )
    _add_symbols(
        demand_by_symbol,
        symbols=_candidate_contract_symbols(candidate_contracts),
        source_type="candidate_contract_symbols",
        required=True,
        consumer="candidate_contracts",
        source_path=str(candidate_contracts_path or ""),
    )
    for contract in contracts.get("contracts", []) if isinstance(contracts.get("contracts"), list) else []:
        if not isinstance(contract, dict):
            continue
        sleeve_id = str(contract.get("sleeve_id") or "")
        symbol_resolution = contract.get("symbol_resolution") if isinstance(contract.get("symbol_resolution"), dict) else {}
        source = str(symbol_resolution.get("source") or "sleeve_input_contracts")
        source_path = str(symbol_resolution.get("source_path") or contracts_path or "")
        for item in contract.get("required_inputs", []) if isinstance(contract.get("required_inputs"), list) else []:
            item_id = str(item.get("data_item_id") or "") if isinstance(item, dict) else ""
            symbol = _symbol_from_item_id(item_id)
            if not symbol:
                continue
            source_type = "required_context_symbols" if symbol == "VIX" else "sleeve_canonical_dynamic_universe_symbols"
            _add_symbol(
                demand_by_symbol,
                symbol=symbol,
                source_type=source_type,
                required=True,
                consumer=sleeve_id,
                data_item_id=item_id,
                source_universe=source,
                source_path=source_path,
                symbol_universe_hash=str(contract.get("symbol_universe_hash") or ""),
            )
        for item in contract.get("optional_inputs", []) if isinstance(contract.get("optional_inputs"), list) else []:
            item_id = str(item.get("data_item_id") or "") if isinstance(item, dict) else ""
            symbol = _symbol_from_item_id(item_id)
            if symbol:
                _add_symbol(demand_by_symbol, symbol=symbol, source_type="required_context_symbols", required=False, consumer=sleeve_id, data_item_id=item_id, source_universe=source, source_path=source_path, symbol_universe_hash=str(contract.get("symbol_universe_hash") or ""))
    raw_required = symbol_map.get("required_symbols") if isinstance(symbol_map.get("required_symbols"), list) else []
    _add_symbols(demand_by_symbol, symbols=raw_required, source_type="symbol_map_required_symbols", required=False, consumer="symbol_map", source_path=str(symbol_map_path or ""))
    _add_symbols(
        demand_by_symbol,
        symbols=_open_position_symbols(paper_position_ledger),
        source_type="open_paper_position_symbols",
        required=True,
        consumer="paper_position_marks",
        source_path=str(paper_position_ledger_path or ""),
    )
    _add_symbols(
        demand_by_symbol,
        symbols=_oil_shock_required_symbols(oil_producer),
        source_type="generated_hypothesis_required_symbols",
        required=True,
        consumer="oil_shock_candidate_flow",
        source_path=str(oil_producer_path or ""),
    )
    for hypothesis_name, symbols in _generated_hypothesis_required_symbols(evidence_packets):
        _add_symbols(
            demand_by_symbol,
            symbols=symbols,
            source_type="generated_hypothesis_required_symbols",
            required=True,
            consumer=hypothesis_name,
            source_path=str(evidence_packets_path or ""),
        )

    rows = sorted(demand_by_symbol.values(), key=lambda row: row["symbol"])
    required_symbols = canonicalize_symbol_list_v1([row["symbol"] for row in rows if row.get("required")])
    requested_symbols = canonicalize_symbol_list_v1([row["symbol"] for row in rows])
    return {
        "schema_id": "aegis_market_data_demand",
        "schema_version": "v1",
        "artifact_id": "aegis_market_data_demand_v1",
        "day_utc": day_utc,
        "generated_at_utc": now_utc_v1(),
        "status": "READY" if requested_symbols else "NO_DEMAND",
        "requested_symbol_count": len(requested_symbols),
        "required_symbol_count": len(required_symbols),
        "requested_symbols": requested_symbols,
        "required_symbols": required_symbols,
        "demand_rows": rows,
        "demand_sources": sorted({source for row in rows for source in row.get("demand_sources", [])}),
        "sources": sources,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }


def write_market_data_demand_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "market_data_demand.v1.json", payload)
    txt_path = out_dir / "market_data_demand.v1.txt"
    csv_path = out_dir / "market_data_demand.matrix.csv"
    txt_path.write_text(render_market_data_demand_v1(payload), encoding="utf-8")
    csv_path.write_text(render_market_data_demand_csv_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "txt": str(txt_path), "matrix": str(csv_path)}


def demand_symbol_map_v1(demand: dict[str, Any]) -> dict[str, Any]:
    symbols = canonicalize_symbol_list_v1(demand.get("requested_symbols") if isinstance(demand.get("requested_symbols"), list) else [])
    entries: dict[str, Any] = {}
    missing: list[str] = []
    for symbol in symbols:
        try:
            entries[symbol] = symbol_map_entry_for_symbol_v1(symbol)
        except Exception:
            missing.append(symbol)
    return {
        "schema_id": "aegis_symbol_map",
        "schema_version": "v1",
        "artifact_id": "aegis_symbol_map_v1",
        "day_utc": str(demand.get("day_utc") or ""),
        "generated_at_utc": now_utc_v1(),
        "required_symbols": symbols,
        "requested_symbols": symbols,
        "runtime_symbols": symbols,
        "symbols": entries,
        "mapping_missing_symbols": missing,
        "runtime_universe_mode": "market_data_demand_v1",
        "requested_symbols_source": "aegis_market_data_demand_v1",
        "total_requested_symbol_count": len(symbols),
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def render_market_data_demand_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS MARKET DATA DEMAND v1",
        f"day_utc: {payload.get('day_utc')}",
        f"status: {payload.get('status')}",
        f"requested_symbol_count: {payload.get('requested_symbol_count')}",
        f"required_symbol_count: {payload.get('required_symbol_count')}",
        f"demand_sources: {', '.join(payload.get('demand_sources') or [])}",
        "broker_execution_allowed: false",
        "autonomous_execution_allowed: false",
        "trade_advice_allowed: false",
        "",
        "symbols:",
    ]
    for row in payload.get("demand_rows") or []:
        lines.append(f"- {row.get('symbol')}: required={str(row.get('required')).lower()} consumers={','.join(row.get('consumer_sleeves') or [])} sources={','.join(row.get('demand_sources') or [])}")
    return "\n".join(lines) + "\n"


def render_market_data_demand_csv_v1(payload: dict[str, Any]) -> str:
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=["symbol", "required", "required_data_item", "demand_sources", "consumer_sleeves", "source_universe", "symbol_universe_hash"])
    writer.writeheader()
    for row in payload.get("demand_rows") or []:
        writer.writerow({
            "symbol": row.get("symbol", ""),
            "required": bool(row.get("required")),
            "required_data_item": row.get("required_data_item", ""),
            "demand_sources": "|".join(row.get("demand_sources") or []),
            "consumer_sleeves": "|".join(row.get("consumer_sleeves") or []),
            "source_universe": row.get("source_universe", ""),
            "symbol_universe_hash": row.get("symbol_universe_hash", ""),
        })
    return out.getvalue()


def _source_ref(path: Path | None) -> dict[str, Any]:
    return {"path": str(path or ""), "found": bool(path), "hash": _sha256(path) if path else ""}


def _sha256(path: Path | None) -> str:
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest() if path else ""
    except Exception:
        return ""


def _symbol_from_item_id(item_id: str) -> str:
    text = str(item_id or "")
    if text.startswith("market.price."):
        return normalize_market_symbol_v1(text.rsplit(".", 1)[-1])
    if text == "market.volatility.VIX":
        return "VIX"
    return ""


def _candidate_contract_symbols(payload: dict[str, Any]) -> list[str]:
    rows = []
    for key in ("candidate_contracts", "contracts", "rows"):
        value = payload.get(key) if isinstance(payload, dict) else None
        if isinstance(value, list):
            rows.extend(row for row in value if isinstance(row, dict))
    return canonicalize_symbol_list_v1(row.get("symbol") or row.get("underlying_symbol") for row in rows)


def _open_position_symbols(payload: dict[str, Any]) -> list[str]:
    rows = payload.get("open_positions") if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return []
    return canonicalize_symbol_list_v1(row.get("symbol") for row in rows if isinstance(row, dict))


def _oil_shock_required_symbols(payload: dict[str, Any]) -> list[str]:
    if not isinstance(payload, dict) or not payload:
        return []
    symbols = payload.get("required_market_symbols")
    out = canonicalize_symbol_list_v1(symbols if isinstance(symbols, list) else [])
    if out:
        return out
    marker = " ".join(str(payload.get(key) or "") for key in ("hypothesis_id", "hypothesis_name", "artifact_id", "schema_id"))
    return ["DBC", "SPY", "USO", "XLE"] if "oil" in marker.lower() else []


def _generated_hypothesis_required_symbols(payload: dict[str, Any]) -> list[tuple[str, list[str]]]:
    rows = payload.get("evidence_packets") if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return []
    out: list[tuple[str, list[str]]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("hypothesis_name") or row.get("hypothesis_id") or "generated_hypothesis").strip()
        symbols = canonicalize_symbol_list_v1(row.get("instrument_universe") if isinstance(row.get("instrument_universe"), list) else row.get("required_market_symbols") if isinstance(row.get("required_market_symbols"), list) else [])
        if symbols:
            out.append((name, symbols))
    return out


def _add_symbols(target: dict[str, dict[str, Any]], *, symbols: Any, source_type: str, required: bool, consumer: str, source_path: str) -> None:
    if not isinstance(symbols, list):
        return
    for symbol in canonicalize_symbol_list_v1(symbols):
        _add_symbol(target, symbol=symbol, source_type=source_type, required=required, consumer=consumer, data_item_id=("market.volatility.VIX" if symbol == "VIX" else f"market.price.{symbol}"), source_universe=source_type, source_path=source_path, symbol_universe_hash="")


def _add_symbol(target: dict[str, dict[str, Any]], *, symbol: str, source_type: str, required: bool, consumer: str, data_item_id: str, source_universe: str, source_path: str, symbol_universe_hash: str) -> None:
    canonical = normalize_market_symbol_v1(symbol)
    if not canonical:
        return
    row = target.setdefault(canonical, {
        "symbol": canonical,
        "required": False,
        "required_data_item": data_item_id,
        "demand_sources": [],
        "consumer_sleeves": [],
        "source_universe": source_universe,
        "source_universe_path": source_path,
        "symbol_universe_hash": symbol_universe_hash,
    })
    row["required"] = bool(row.get("required") or required)
    if required and data_item_id:
        row["required_data_item"] = data_item_id
    if source_type not in row["demand_sources"]:
        row["demand_sources"].append(source_type)
    if consumer and consumer not in row["consumer_sleeves"]:
        row["consumer_sleeves"].append(consumer)
    if symbol_universe_hash and not row.get("symbol_universe_hash"):
        row["symbol_universe_hash"] = symbol_universe_hash
    if source_universe and not row.get("source_universe"):
        row["source_universe"] = source_universe
    if source_path and not row.get("source_universe_path"):
        row["source_universe_path"] = source_path
