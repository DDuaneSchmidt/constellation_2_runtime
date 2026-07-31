from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, write_json_v1
from ops.aegis.evidence_event_store_v1 import read_evidence_events_v1
from ops.aegis.market_data.symbol_alias_registry_v1 import (
    canonical_aliases_v1,
    canonicalize_symbol_list_v1,
    normalize_market_symbol_v1,
    symbol_matches_canonical_v1,
)


REPORT_FAMILY = "aegis_data_registry_v1"
BASELINE_ITEMS = ("market.calendar.session", "provider.health", "runtime.freshness")
INPUT_CLASSIFICATIONS = {"SLEEVE_CRITICAL", "RUNTIME_CRITICAL", "ADVISORY_ONLY", "LEGACY_OPTIONAL", "FUTURE_REQUIRED"}
BLOCKING_CLASSIFICATIONS = {"SLEEVE_CRITICAL", "RUNTIME_CRITICAL"}
FUTURE_REQUIRED_ITEMS: set[str] = set()
CONTEXT_ITEMS = (
    "market.price.SPY",
    "market.price.QQQ",
    "market.volatility.VIX",
    "market.breadth.down_pct",
    "market.breadth.advance_decline_delta",
)


def build_data_registry_v1(*, truth_root: Path, day_utc: str, symbols: list[str] | None = None, universe_metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated_at = now_utc_v1()
    market_path, market_payload = latest_json_v1(root, "aegis_market_data_v1", day_utc, "market_data.v1.json")
    market_inputs_path, market_inputs_payload = latest_json_v1(root, "market_data_inputs_v1", day_utc, "market_data_inputs.v1.json")
    contracts_path, contracts_payload = latest_json_v1(root, "aegis_sleeve_input_contracts_v1", day_utc, "sleeve_input_contracts.v1.json")
    required_contract_item_ids, optional_contract_item_ids = _contract_item_sets(contracts_payload)
    requested_from_report = market_payload.get("requested_symbols") if isinstance(market_payload.get("requested_symbols"), list) else []
    requested_from_inputs = [
        str(row.get("symbol") or "")
        for row in market_inputs_payload.get("input_records", [])
        if isinstance(row, dict) and str(row.get("symbol") or "")
    ]
    universe_metadata = universe_metadata if isinstance(universe_metadata, dict) else {}
    requested_symbols = canonicalize_symbol_list_v1([*requested_from_inputs, *requested_from_report, *(symbols or []), "SPY", "QQQ", "IWM", "DIA"])
    data_items: list[dict[str, Any]] = []
    provider_status = market_payload.get("provider_results") if isinstance(market_payload.get("provider_results"), list) else []
    market_session_date = str(market_payload.get("market_session_date") or day_utc) if market_payload else day_utc
    market_input_events = _market_input_event_refs(root=root, day_utc=day_utc, market_inputs_path=market_inputs_path)

    data_items.append(_item("market.calendar.session", "CALENDAR", status="CURRENT", provider="runtime_calendar", day_utc=day_utc, market_session_date=market_session_date, notes=[]))
    data_items.append(
        _item(
            "provider.health",
            "CUSTOM",
            status=_provider_health_status(market_payload),
            provider=str(market_payload.get("source") or "") if market_payload else "",
            day_utc=day_utc,
            market_session_date=market_session_date,
            source_artifact_path=str(market_path or ""),
            notes=[str(market_payload.get("failure_reason") or "")] if market_payload.get("failure_reason") else [],
        )
    )
    data_items.append(_item("runtime.freshness", "CUSTOM", status="CURRENT", provider="aegis_data_registry_v1", day_utc=day_utc, market_session_date=market_session_date, notes=[]))

    for symbol in requested_symbols:
        if symbol == "VIX":
            continue
        data_items.append(_price_item(root=root, day_utc=day_utc, symbol=symbol, market_payload=market_payload, market_path=market_path, market_inputs_payload=market_inputs_payload, market_inputs_path=market_inputs_path, market_input_events=market_input_events))
    data_items.append(_vix_item(root=root, day_utc=day_utc, market_payload=market_payload, market_path=market_path, market_inputs_payload=market_inputs_payload, market_inputs_path=market_inputs_path, market_input_events=market_input_events))
    data_items.extend(_breadth_items(day_utc=day_utc, market_payload=market_payload, market_path=market_path))

    for item in data_items:
        item_id = str(item.get("data_item_id") or "")
        classification = _input_classification(item_id, required_contract_item_ids=required_contract_item_ids, optional_contract_item_ids=optional_contract_item_ids)
        item["input_classification"] = classification
        item["readiness_effect"] = _readiness_effect(item.get("status"), classification)

    by_id: dict[str, dict[str, Any]] = {}
    for item in data_items:
        by_id[str(item.get("data_item_id"))] = item
    ordered = [by_id[item] for item in BASELINE_ITEMS if item in by_id]
    ordered.extend(by_id[item] for item in sorted(by_id) if item not in BASELINE_ITEMS)
    missing = [item["data_item_id"] for item in ordered if item.get("status") == "MISSING"]
    stale = [item["data_item_id"] for item in ordered if item.get("status") == "STALE"]
    gap_items = [item for item in ordered if item.get("status") in {"MISSING", "STALE", "UNKNOWN"}]
    blocked_items = [item["data_item_id"] for item in gap_items if item.get("input_classification") in BLOCKING_CLASSIFICATIONS]
    warning_items = [item["data_item_id"] for item in gap_items if item.get("input_classification") not in BLOCKING_CLASSIFICATIONS]
    sleeve_critical_blocked = [item["data_item_id"] for item in gap_items if item.get("input_classification") == "SLEEVE_CRITICAL"]
    runtime_critical_blocked = [item["data_item_id"] for item in gap_items if item.get("input_classification") == "RUNTIME_CRITICAL"]
    optional_blocked = [item["data_item_id"] for item in gap_items if item.get("input_classification") == "ADVISORY_ONLY"]
    legacy_blocked = [item["data_item_id"] for item in gap_items if item.get("input_classification") == "LEGACY_OPTIONAL"]
    future_required = [item["data_item_id"] for item in gap_items if item.get("input_classification") == "FUTURE_REQUIRED"]
    classification = {item["data_item_id"]: item.get("input_classification") for item in gap_items}
    return {
        "schema_id": "aegis_data_registry",
        "schema_version": "v1",
        "artifact_id": "aegis_data_registry_v1",
        "day_utc": day_utc,
        "generated_at_utc": generated_at,
        "market_session_date": market_session_date,
        "data_items": ordered,
        "missing_items": missing,
        "stale_items": stale,
        "provider_status": provider_status,
        "source_market_data_artifact_path": str(market_path or ""),
        "source_market_data_inputs_artifact_path": str(market_inputs_path or ""),
        "source_sleeve_input_contracts_artifact_path": str(contracts_path or ""),
        "requested_symbols": requested_symbols,
        "current_sleeve_required_item_ids": required_contract_item_ids,
        "current_sleeve_optional_item_ids": optional_contract_item_ids,
        "blocking_items": blocked_items,
        "warning_items": warning_items,
        "sleeve_critical_blocked_items": sleeve_critical_blocked,
        "runtime_critical_blocked_items": runtime_critical_blocked,
        "optional_blocked_items": optional_blocked,
        "advisory_only_warning_items": optional_blocked,
        "legacy_non_sleeve_blocked_items": legacy_blocked,
        "legacy_optional_warning_items": legacy_blocked,
        "future_required_warning_items": future_required,
        "market_input_gap_classification": classification,
        "runtime_universe_mode": universe_metadata.get("runtime_universe_mode") or market_payload.get("runtime_universe_mode") or "",
        "production_scan_dataset_id": universe_metadata.get("production_scan_dataset_id") or market_payload.get("production_scan_dataset_id") or "",
        "dataset_snapshot_id": universe_metadata.get("dataset_snapshot_id") or market_payload.get("dataset_snapshot_id") or "",
        "production_scan_universe_count": int(universe_metadata.get("production_scan_universe_count") or market_payload.get("production_scan_universe_count") or 0),
        "sleeve_required_symbol_count": int(universe_metadata.get("sleeve_required_symbol_count") or market_payload.get("sleeve_required_symbol_count") or 0),
        "total_requested_symbol_count": len(requested_symbols),
        "requested_symbols_source": universe_metadata.get("requested_symbols_source") or market_payload.get("requested_symbols_source") or "",
        "minimum_viable_runtime_reference_removed": bool(universe_metadata.get("minimum_viable_runtime_reference_removed") or market_payload.get("minimum_viable_runtime_reference_removed")),
        "fetched_symbols": market_payload.get("fetched_symbols") if isinstance(market_payload.get("fetched_symbols"), list) else [],
        "missing_symbols": market_payload.get("missing_symbols") if isinstance(market_payload.get("missing_symbols"), list) else [],
        "stale_symbols": market_payload.get("stale_symbols") if isinstance(market_payload.get("stale_symbols"), list) else [],
        "mapping_missing_symbols": market_payload.get("mapping_missing_symbols") if isinstance(market_payload.get("mapping_missing_symbols"), list) else [],
        "provider_failed_symbols": market_payload.get("provider_failed_symbols") if isinstance(market_payload.get("provider_failed_symbols"), list) else [],
        "provider_config": market_payload.get("provider_config") if isinstance(market_payload.get("provider_config"), dict) else {},
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def write_data_registry_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "data_registry.v1.json", payload)
    summary_path = out_dir / "data_registry.summary.txt"
    matrix_path = out_dir / "data_registry.matrix.csv"
    summary_path.write_text(render_data_registry_summary_v1(payload), encoding="utf-8")
    matrix_path.write_text(render_data_registry_matrix_csv_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "matrix": str(matrix_path)}


def render_data_registry_summary_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS DATA REGISTRY v1",
        f"day_utc: {payload.get('day_utc')}",
        f"market_session_date: {payload.get('market_session_date')}",
        f"data_items: {len(payload.get('data_items') or [])}",
        f"missing_items: {len(payload.get('missing_items') or [])}",
        f"stale_items: {len(payload.get('stale_items') or [])}",
        f"universe_mode: {payload.get('runtime_universe_mode') or ''}",
        f"dataset_snapshot_id: {payload.get('production_scan_dataset_id') or payload.get('dataset_snapshot_id') or ''}",
        f"production_scan_universe_count: {payload.get('production_scan_universe_count') or 0}",
        f"sleeve_required_symbol_count: {payload.get('sleeve_required_symbol_count') or 0}",
        f"total_requested_symbol_count: {payload.get('total_requested_symbol_count') or len(payload.get('requested_symbols') or [])}",
        f"requested_symbols_source: {payload.get('requested_symbols_source') or ''}",
        f"blocking_items: {','.join(payload.get('blocking_items') or [])}",
        f"warning_items: {','.join(payload.get('warning_items') or [])}",
        f"sleeve_critical_blocked_items: {','.join(payload.get('sleeve_critical_blocked_items') or [])}",
        f"runtime_critical_blocked_items: {','.join(payload.get('runtime_critical_blocked_items') or [])}",
        f"optional_blocked_items: {','.join(payload.get('optional_blocked_items') or [])}",
        f"legacy_non_sleeve_blocked_items: {','.join(payload.get('legacy_non_sleeve_blocked_items') or [])}",
        "broker_execution_allowed: false",
        "autonomous_execution_allowed: false",
        "",
        "items:",
    ]
    for item in payload.get("data_items") or []:
        lines.append(f"- {item.get('data_item_id')}: {item.get('status')} class={item.get('input_classification') or ''} effect={item.get('readiness_effect') or ''} provider={item.get('provider') or 'UNKNOWN'} timestamp={item.get('data_timestamp_utc') or ''}")
    return "\n".join(lines) + "\n"


def render_data_registry_matrix_csv_v1(payload: dict[str, Any]) -> str:
    out = StringIO()
    fieldnames = ["data_item_id", "kind", "symbol", "field", "status", "input_classification", "readiness_effect", "provider", "source_artifact_path", "source_hash", "data_timestamp_utc", "market_session_date", "freshness_rule", "quality", "notes"]
    writer = csv.DictWriter(out, fieldnames=fieldnames)
    writer.writeheader()
    for item in payload.get("data_items") or []:
        writer.writerow({key: ("|".join(item.get(key) or []) if key == "notes" else item.get(key, "")) for key in fieldnames})
    return out.getvalue()




def _input_classification(data_item_id: str, *, required_contract_item_ids: list[str], optional_contract_item_ids: list[str]) -> str:
    item_id = str(data_item_id or "").strip()
    if item_id in required_contract_item_ids:
        return "SLEEVE_CRITICAL"
    if item_id in {"market.calendar.session", "runtime.freshness"}:
        return "RUNTIME_CRITICAL"
    if item_id in FUTURE_REQUIRED_ITEMS:
        return "FUTURE_REQUIRED"
    if item_id == "provider.health":
        return "ADVISORY_ONLY"
    if item_id in optional_contract_item_ids:
        return "ADVISORY_ONLY"
    if item_id.startswith("market.breadth."):
        return "ADVISORY_ONLY"
    if item_id == "market.price.DIA":
        return "LEGACY_OPTIONAL"
    if item_id.startswith(("market.price.", "market.volatility.")):
        return "LEGACY_OPTIONAL"
    return "LEGACY_OPTIONAL"


def _readiness_effect(status: Any, classification: str) -> str:
    if str(status or "").upper() not in {"MISSING", "STALE", "UNKNOWN"}:
        return "NONE"
    return "BLOCKS" if classification in BLOCKING_CLASSIFICATIONS else "WARNS"

def _contract_item_sets(contracts_payload: dict[str, Any]) -> tuple[list[str], list[str]]:
    required: set[str] = set()
    optional: set[str] = set()
    for contract in contracts_payload.get("contracts", []) if isinstance(contracts_payload.get("contracts"), list) else []:
        if not isinstance(contract, dict):
            continue
        for row in contract.get("required_inputs", []) if isinstance(contract.get("required_inputs"), list) else []:
            if isinstance(row, dict) and str(row.get("data_item_id") or "").strip():
                required.add(str(row.get("data_item_id")).strip())
        for row in contract.get("optional_inputs", []) if isinstance(contract.get("optional_inputs"), list) else []:
            if isinstance(row, dict) and str(row.get("data_item_id") or "").strip():
                optional.add(str(row.get("data_item_id")).strip())
    return sorted(required), sorted(optional - required)

def _item(
    data_item_id: str,
    kind: str,
    *,
    status: str,
    provider: str,
    day_utc: str,
    market_session_date: str,
    symbol: str = "",
    field: str = "",
    source_artifact_path: str = "",
    source_hash: str = "",
    data_timestamp_utc: str = "",
    freshness_rule: str = "same market session required during trading day",
    quality: str = "UNKNOWN",
    notes: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "data_item_id": data_item_id,
        "kind": kind,
        "symbol": symbol,
        "field": field,
        "status": status,
        "provider": provider,
        "source_artifact_path": source_artifact_path,
        "source_hash": source_hash,
        "data_timestamp_utc": data_timestamp_utc,
        "market_session_date": market_session_date,
        "freshness_rule": freshness_rule,
        "quality": quality,
        "notes": [note for note in (notes or []) if note],
    }



def _market_input_record(payload: dict[str, Any], data_item_id: str) -> dict[str, Any]:
    for row in payload.get("input_records", []) if isinstance(payload.get("input_records"), list) else []:
        if isinstance(row, dict) and str(row.get("data_item_id") or "") == data_item_id:
            return row
    return {}


def _item_from_market_input(market_input: dict[str, Any], *, market_inputs_path: Path | None, market_input_events: dict[str, str] | None) -> dict[str, Any]:
    validation = str(market_input.get("validation_status") or "MISSING").upper()
    status = {
        "VALID": "CURRENT",
        "STALE": "STALE",
        "MISSING": "MISSING",
        "UNAVAILABLE_EXTERNAL_SOURCE": "MISSING",
        "MALFORMED": "MISSING",
        "TAMPERED": "MISSING",
    }.get(validation, "UNKNOWN")
    item = _item(
        str(market_input.get("data_item_id") or ""),
        "VOLATILITY" if str(market_input.get("field_type") or "") == "volatility" else "PRICE",
        symbol=str(market_input.get("symbol") or ""),
        field="level" if str(market_input.get("field_type") or "") == "volatility" else "last_price",
        status=status,
        provider=str(market_input.get("source_vendor") or ""),
        day_utc=str(market_input.get("day_utc") or ""),
        market_session_date=str(market_input.get("source_timestamp_utc") or "")[:10] or str(market_input.get("day_utc") or ""),
        source_artifact_path=str(market_inputs_path or market_input.get("source_path") or ""),
        source_hash=_file_hash(market_inputs_path) or str(market_input.get("transformed_value_hash") or ""),
        data_timestamp_utc=str(market_input.get("source_timestamp_utc") or ""),
        quality="HIGH" if status == "CURRENT" else "LOW",
        notes=[str(market_input.get("reason") or ""), f"market_data_input_validation={validation}"],
    )
    event_refs = market_input_events or {}
    item["evidence_event_id"] = event_refs.get("event_id", "")
    item["evidence_event_hash"] = event_refs.get("event_hash", "")
    item["raw_source_hash"] = str(market_input.get("raw_source_hash") or "")
    item["transformed_value_hash"] = str(market_input.get("transformed_value_hash") or "")
    item["market_data_validation_status"] = validation
    item["value"] = market_input.get("value")
    item["unit"] = market_input.get("unit") or ""
    return item


def _market_input_event_refs(*, root: Path, day_utc: str, market_inputs_path: Path | None) -> dict[str, str]:
    if not market_inputs_path:
        return {}
    target = str(Path(market_inputs_path).expanduser().resolve())
    try:
        events = read_evidence_events_v1(truth_root=root, day_utc=day_utc)
    except Exception:
        return {}
    for event in reversed(events):
        paths = [str(Path(path).expanduser().resolve()) for path in event.get("artifact_paths", []) if str(path)]
        if target in paths and str(event.get("event_type") or "") == "EvidenceValidated":
            return {"event_id": str(event.get("event_id") or ""), "event_hash": str(event.get("event_hash") or "")}
    return {}

def _provider_health_status(market_payload: dict[str, Any]) -> str:
    if not market_payload:
        return "MISSING"
    status = str(market_payload.get("status") or "").upper()
    if status in {"CURRENT", "CURRENT_WITH_WARNINGS"}:
        return "CURRENT"
    if status == "PARTIAL":
        return "DELAYED_BUT_USABLE"
    if status == "STALE":
        return "STALE"
    if status in {"MISSING", "FAILED"}:
        return "MISSING"
    return "UNKNOWN"


def _price_item(*, root: Path, day_utc: str, symbol: str, market_payload: dict[str, Any], market_path: Path | None, market_inputs_payload: dict[str, Any] | None = None, market_inputs_path: Path | None = None, market_input_events: dict[str, str] | None = None) -> dict[str, Any]:
    symbol = normalize_market_symbol_v1(symbol)
    row = _symbol_from_market_payload(market_payload, symbol)
    if row:
        session_date = str(row.get("market_session_date") or market_payload.get("market_session_date") or day_utc)
        status = str(row.get("freshness_status") or ("CURRENT" if session_date == day_utc else "STALE")).upper()
        if status == "CURRENT":
            return _item(
                f"market.price.{symbol}",
                "PRICE",
                symbol=symbol,
                field="last_price",
                status=status,
                provider=str(row.get("source") or row.get("provider") or market_payload.get("source") or "aegis_market_data_v1"),
                day_utc=day_utc,
                market_session_date=session_date,
                source_artifact_path=str(market_path or ""),
                source_hash=_file_hash(market_path),
                data_timestamp_utc=str(row.get("data_timestamp_utc") or row.get("timestamp_utc") or row.get("source_timestamp_utc") or ""),
                quality="HIGH",
                notes=[],
            )
    market_input = _market_input_record(market_inputs_payload or {}, f"market.price.{symbol}")
    if market_input:
        return _item_from_market_input(market_input, market_inputs_path=market_inputs_path, market_input_events=market_input_events)
    if row:
        session_date = str(row.get("market_session_date") or market_payload.get("market_session_date") or day_utc)
        status = str(row.get("freshness_status") or ("CURRENT" if session_date == day_utc else "STALE")).upper()
        return _item(
            f"market.price.{symbol}",
            "PRICE",
            symbol=symbol,
            field="last_price",
            status=status,
            provider=str(row.get("source") or row.get("provider") or market_payload.get("source") or "aegis_market_data_v1"),
            day_utc=day_utc,
            market_session_date=session_date,
            source_artifact_path=str(market_path or ""),
            source_hash=_file_hash(market_path),
            data_timestamp_utc=str(row.get("data_timestamp_utc") or row.get("timestamp_utc") or row.get("source_timestamp_utc") or ""),
            quality="HIGH" if status == "CURRENT" else "LOW",
            notes=[],
        )
    if market_payload:
        mapping_missing = symbol in set(str(item) for item in market_payload.get("mapping_missing_symbols") or [])
        provider_failed = symbol in set(str(item) for item in market_payload.get("provider_failed_symbols") or [])
        notes = []
        if mapping_missing:
            notes.append("SYMBOL_MAPPING_MISSING")
        if provider_failed:
            notes.append("PROVIDER_FAILED_SYMBOL")
        failure = str(market_payload.get("failure_reason") or "")
        if failure:
            notes.append(failure)
        return _item(f"market.price.{symbol}", "PRICE", symbol=symbol, field="last_price", status="MISSING", provider=str(market_payload.get("source") or ""), day_utc=day_utc, market_session_date=day_utc, source_artifact_path=str(market_path or ""), source_hash=_file_hash(market_path), notes=notes or ["No provider record returned for this symbol."])
    path, latest = _latest_canonical_symbol(root=root, day_utc=day_utc, symbol=symbol)
    if latest:
        row_day = str(latest.get("timestamp_utc") or "")[:10]
        status = "CURRENT" if row_day == day_utc else "STALE"
        return _item(
            f"market.price.{symbol}",
            "PRICE",
            symbol=symbol,
            field="last_price",
            status=status,
            provider="canonical_market_data_snapshot_v1",
            day_utc=day_utc,
            market_session_date=row_day or day_utc,
            source_artifact_path=str(path),
            source_hash=_file_hash(path),
            data_timestamp_utc=str(latest.get("timestamp_utc") or ""),
            quality="HIGH" if status == "CURRENT" else "LOW",
            notes=[] if status == "CURRENT" else [f"latest available row is {row_day or 'UNKNOWN'}"],
        )
    return _item(f"market.price.{symbol}", "PRICE", symbol=symbol, field="last_price", status="MISSING", provider="", day_utc=day_utc, market_session_date=day_utc, notes=["No real source row found."])


def _vix_item(*, root: Path, day_utc: str, market_payload: dict[str, Any], market_path: Path | None, market_inputs_payload: dict[str, Any] | None = None, market_inputs_path: Path | None = None, market_input_events: dict[str, str] | None = None) -> dict[str, Any]:
    row = _symbol_from_market_payload(market_payload, "VIX")
    if row:
        session_date = str(row.get("market_session_date") or market_payload.get("market_session_date") or day_utc)
        status = str(row.get("freshness_status") or ("CURRENT" if session_date == day_utc else "STALE")).upper()
        if status == "CURRENT":
            return _item("market.volatility.VIX", "VOLATILITY", symbol="VIX", field="level", status=status, provider=str(row.get("source") or row.get("provider") or market_payload.get("source") or "aegis_market_data_v1"), day_utc=day_utc, market_session_date=session_date, source_artifact_path=str(market_path or ""), source_hash=_file_hash(market_path), data_timestamp_utc=str(row.get("data_timestamp_utc") or row.get("timestamp_utc") or row.get("source_timestamp_utc") or ""), quality="HIGH")
    market_input = _market_input_record(market_inputs_payload or {}, "market.volatility.VIX")
    if market_input:
        return _item_from_market_input(market_input, market_inputs_path=market_inputs_path, market_input_events=market_input_events)
    if row:
        session_date = str(row.get("market_session_date") or market_payload.get("market_session_date") or day_utc)
        status = str(row.get("freshness_status") or ("CURRENT" if session_date == day_utc else "STALE")).upper()
        return _item("market.volatility.VIX", "VOLATILITY", symbol="VIX", field="level", status=status, provider=str(row.get("source") or row.get("provider") or market_payload.get("source") or "aegis_market_data_v1"), day_utc=day_utc, market_session_date=session_date, source_artifact_path=str(market_path or ""), source_hash=_file_hash(market_path), data_timestamp_utc=str(row.get("data_timestamp_utc") or row.get("timestamp_utc") or row.get("source_timestamp_utc") or ""), quality="HIGH" if status == "CURRENT" else "LOW")
    if market_payload:
        failure = str(market_payload.get("failure_reason") or "")
        explanations = market_payload.get("missing_symbol_explanations") if isinstance(market_payload.get("missing_symbol_explanations"), dict) else {}
        vix_explanation = explanations.get("VIX") if isinstance(explanations.get("VIX"), dict) else {}
        notes = [failure] if failure else []
        if vix_explanation.get("operator_message"):
            notes.append(str(vix_explanation.get("operator_message")))
        return _item("market.volatility.VIX", "VOLATILITY", symbol="VIX", field="level", status="MISSING", provider=str(market_payload.get("source") or ""), day_utc=day_utc, market_session_date=day_utc, source_artifact_path=str(market_path or ""), source_hash=_file_hash(market_path), notes=notes or ["No provider record returned for canonical VIX."])
    price = _price_item(root=root, day_utc=day_utc, symbol="VIX", market_payload={}, market_path=None)
    return {**price, "data_item_id": "market.volatility.VIX", "kind": "VOLATILITY", "field": "level"}


def _breadth_items(*, day_utc: str, market_payload: dict[str, Any], market_path: Path | None) -> list[dict[str, Any]]:
    breadth = market_payload.get("breadth") if isinstance(market_payload.get("breadth"), dict) else {}
    out = []
    for field, item_id in (("breadth_down_pct", "market.breadth.down_pct"), ("advance_decline_delta", "market.breadth.advance_decline_delta")):
        value_present = breadth.get(field) not in {None, ""}
        status = str(breadth.get("freshness_status") or ("CURRENT" if value_present else "MISSING")).upper()
        out.append(
            _item(
                item_id,
                "BREADTH",
                field=field,
                status=status if value_present else "MISSING",
                provider=str(breadth.get("source") or ""),
                day_utc=day_utc,
                market_session_date=str(breadth.get("market_session_date") or market_payload.get("market_session_date") or day_utc),
                source_artifact_path=str(market_path or ""),
                source_hash=_file_hash(market_path),
                data_timestamp_utc=str(breadth.get("data_timestamp_utc") or ""),
                quality="MEDIUM" if str(breadth.get("source") or "").upper() == "BREADTH_PROXY" else ("HIGH" if value_present else "UNKNOWN"),
                notes=[] if value_present else ["No real breadth source found; missing breadth is not fabricated."],
            )
        )
    return out


def _symbol_from_market_payload(payload: dict[str, Any], symbol: str) -> dict[str, Any]:
    symbols = payload.get("symbols") if isinstance(payload.get("symbols"), dict) else {}
    canonical = normalize_market_symbol_v1(symbol)
    row = symbols.get(canonical) if isinstance(symbols.get(canonical), dict) else {}
    if row:
        return row
    for key, value in symbols.items():
        if symbol_matches_canonical_v1(key, canonical) and isinstance(value, dict):
            return value
    return row


def _latest_canonical_symbol(*, root: Path, day_utc: str, symbol: str) -> tuple[Path, dict[str, Any]]:
    canonical = normalize_market_symbol_v1(symbol)
    path = root / "market_data_snapshot_v1" / canonical / f"{day_utc[:4]}.jsonl"
    latest: dict[str, Any] = {}
    for alias in canonical_aliases_v1(canonical):
        candidate_path = root / "market_data_snapshot_v1" / alias / f"{day_utc[:4]}.jsonl"
        try:
            for line in candidate_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                row = json.loads(line)
                if isinstance(row, dict) and symbol_matches_canonical_v1(row.get("symbol"), canonical):
                    latest = row
                    path = candidate_path
        except Exception:
            continue
    return path, latest


def _file_hash(path: Path | None) -> str:
    if not path or not path.exists() or not path.is_file():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()
