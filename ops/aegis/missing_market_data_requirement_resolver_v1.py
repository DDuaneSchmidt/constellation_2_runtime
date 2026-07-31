from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_mapping_rules_v1 import file_hash_v1, report_path_v1, stable_hash_v1

FAMILY = "aegis_missing_market_data_requirement_resolver_v1"
FILENAME = "missing_market_data_requirement_resolver.v1.json"
POLICY_VERSION = "AEGIS_MISSING_MARKET_DATA_REQUIREMENT_RESOLVER_READ_ONLY_V1"

RESOLUTION_TYPES = [
    "SOURCE_AVAILABLE_ROUTING_MISSING",
    "SOURCE_AVAILABLE_INGESTION_MISSING",
    "SOURCE_AVAILABLE_BUT_STALE",
    "SOURCE_AVAILABLE_BUT_INCOMPLETE",
    "SOURCE_NOT_CONFIGURED",
    "SOURCE_NOT_AVAILABLE",
    "DATA_REQUIREMENT_UNDECLARED",
    "UNSUPPORTED_DATA_DEPENDENCY",
    "NO_ACTION_REQUIRED",
    "UNKNOWN_DETERMINISTIC_BLOCKER",
]

SAFETY = {
    "read_only": True,
    "diagnostics_only": True,
    "no_sleeve_mutation": True,
    "no_strategy_logic_mutation": True,
    "no_threshold_mutation": True,
    "no_candidate_mutation": True,
    "no_quality_mutation": True,
    "no_allocation_mutation": True,
    "no_market_data_mutation": True,
    "no_signal_fabrication": True,
    "no_forced_candidates": True,
    "no_repair_performed": True,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
}


def missing_market_data_requirement_resolver_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, day_utc, FILENAME)


def build_missing_market_data_requirement_resolver_v1(
    *, truth_root: Path | str, day_utc: str, computed_at_utc: str | None = None
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    paths = _input_paths(root, day)
    payloads = {name: read_json_v1(path) for name, path in paths.items()}
    t02_rows = [
        row
        for row in _rows(payloads.get("dormant_sleeve_signal_generation_diagnostics"), "dormant_sleeves")
        if _upper(row.get("dormant_reason_code")) == "MISSING_MARKET_DATA"
    ]
    rows = [
        _build_sleeve_row(row, payloads, paths, root, day, computed_at_utc or f"{day}T00:00:00Z")
        for row in sorted(t02_rows, key=lambda item: _text(item.get("sleeve_id")))
    ]
    summary = _summary(rows)
    payload: dict[str, Any] = {
        "schema_id": "aegis_missing_market_data_requirement_resolver",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "policy_version": POLICY_VERSION,
        "day_utc": day,
        "target_day": day,
        "computed_at_utc": computed_at_utc or f"{day}T00:00:00Z",
        "sleeves": rows,
        "portfolio_summary": summary,
        "summary": summary,
        "answer": _answer(rows),
        "source_artifact_paths": {name: str(path) for name, path in sorted(paths.items()) if path.exists()},
        "source_artifact_hashes": {name: file_hash_v1(path) for name, path in sorted(paths.items()) if path.exists()},
        "safety_statement": "Missing market data requirement resolver diagnostics are read-only. This artifact explains market-data requirements, sources, ingestion, routing, freshness, and completeness only; it does not mutate sleeves, producers, strategy logic, thresholds, market data, candidates, quality, allocation, or signals.",
        **SAFETY,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = stable_hash_v1({**payload, "content_hash": ""})
    return payload


def write_missing_market_data_requirement_resolver_v1(
    *, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None
) -> Path:
    body = payload or build_missing_market_data_requirement_resolver_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(missing_market_data_requirement_resolver_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _input_paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "dormant_sleeve_signal_generation_diagnostics": report_path_v1(root, "aegis_dormant_sleeve_signal_generation_diagnostics_v1", day, "dormant_sleeve_signal_generation_diagnostics.v1.json"),
        "sleeve_throughput_diagnostics": report_path_v1(root, "aegis_sleeve_throughput_diagnostics_v1", day, "sleeve_throughput_diagnostics.v1.json"),
        "sleeve_input_contracts": report_path_v1(root, "aegis_sleeve_input_contracts_v1", day, "sleeve_input_contracts.v1.json"),
        "candidate_generation_diagnostics": report_path_v1(root, "aegis_candidate_generation_diagnostics_v1", day, "candidate_generation_diagnostics.v1.json"),
        "sleeve_evaluation_rollup": report_path_v1(root, "sleeve_evaluation_kernel_v1", day, "sleeve_evaluation_rollup.v1.json"),
        "market_data": report_path_v1(root, "aegis_market_data_v1", day, "market_data.v1.json"),
        "market_data_inputs": report_path_v1(root, "market_data_inputs_v1", day, "market_data_inputs.v1.json"),
        "market_data_readiness": report_path_v1(root, "market_data_readiness_v1", day, "market_data_readiness.v1.json"),
        "market_data_coverage": report_path_v1(root, "aegis_market_data_coverage_v1", day, "market_data_coverage.v1.json"),
        "market_data_demand": report_path_v1(root, "aegis_market_data_demand_v1", day, "market_data_demand.v1.json"),
        "provider_attempts": report_path_v1(root, "aegis_market_data_v1", day, "market_data_provider_attempts.v1.json"),
        "provider_capabilities": report_path_v1(root, "aegis_market_data_v1", day, "market_data_provider_capabilities.v1.json"),
        "dataset_manifest": root / "market_data_snapshot_v1" / "dataset_manifest.json",
    }


def _build_sleeve_row(
    t02: Mapping[str, Any],
    payloads: Mapping[str, Any],
    paths: Mapping[str, Path],
    root: Path,
    day: str,
    computed_at: str,
) -> dict[str, Any]:
    sleeve_id = _text(t02.get("sleeve_id"))
    hypothesis_id = _text(t02.get("hypothesis_id"))
    contract = _first_match(_rows(payloads.get("sleeve_input_contracts"), "contracts"), sleeve_id, hypothesis_id)
    eval_row = _first_match(_rows(payloads.get("sleeve_evaluation_rollup"), "outcomes"), sleeve_id, hypothesis_id)
    diag = _first_match(_rows(payloads.get("candidate_generation_diagnostics"), "sleeves", "per_sleeve_readiness"), sleeve_id, hypothesis_id)

    requirements = _requirements(t02, contract, eval_row, diag, root, day)
    item_rows = [_resolve_requirement(item, sleeve_id, payloads, root, day) for item in requirements]
    resolution_type = _rollup_resolution(item_rows, requirements)
    owner, david = _owner(resolution_type, item_rows)
    row = {
        "schema_id": "aegis_missing_market_data_requirement_resolver_row",
        "schema_version": "v1",
        "day_utc": day,
        "computed_at_utc": computed_at,
        "sleeve_id": sleeve_id,
        "sleeve_name": _text(t02.get("sleeve_name")) or sleeve_id,
        "hypothesis_id": hypothesis_id,
        "blocker_from_t02": _text(t02.get("dormant_reason_code")),
        "required_market_data_items": item_rows,
        "required_fields": sorted({field for item in item_rows for field in _string_list(item.get("required_fields"))}),
        "expected_source": _join_unique(item.get("expected_source") for item in item_rows),
        "expected_source_type": _join_unique(item.get("expected_source_type") for item in item_rows),
        "source_exists": all(bool(item.get("source_exists")) for item in item_rows) if item_rows else False,
        "source_ingested": all(bool(item.get("source_ingested")) for item in item_rows) if item_rows else False,
        "source_fresh": all(bool(item.get("source_fresh")) for item in item_rows) if item_rows else False,
        "data_routed_to_sleeve": all(bool(item.get("data_routed_to_sleeve")) for item in item_rows) if item_rows else False,
        "data_quality_status": _quality_status(item_rows),
        "missing_data_items": sorted({value for item in item_rows for value in _string_list(item.get("missing_data_items"))}),
        "missing_fields": sorted({value for item in item_rows for value in _string_list(item.get("missing_fields"))}),
        "stale_data_items": sorted({value for item in item_rows for value in _string_list(item.get("stale_data_items"))}),
        "unsupported_data_items": sorted({value for item in item_rows for value in _string_list(item.get("unsupported_data_items"))}),
        "resolution_status": "ACTION_REQUIRED" if resolution_type not in {"NO_ACTION_REQUIRED"} else "RESOLVED_NO_ACTION",
        "resolution_type": resolution_type,
        "owner": owner,
        "david_action_required": david,
        "next_action": _next_action(resolution_type, item_rows),
        "requirement_sources": _requirement_sources(requirements),
        "t02_link": {
            "artifact_id": "aegis_dormant_sleeve_signal_generation_diagnostics_v1",
            "path": str(paths["dormant_sleeve_signal_generation_diagnostics"]),
            "sleeve_id": sleeve_id,
            "dormant_reason_code": _text(t02.get("dormant_reason_code")),
        },
        "source_artifact_paths": {name: str(path) for name, path in sorted(paths.items()) if path.exists()},
        "source_artifact_hashes": {name: file_hash_v1(path) for name, path in sorted(paths.items()) if path.exists()},
        **SAFETY,
    }
    row["row_hash"] = stable_hash_v1({**row, "row_hash": ""})
    return row


def _requirements(
    t02: Mapping[str, Any],
    contract: Mapping[str, Any],
    eval_row: Mapping[str, Any],
    diag: Mapping[str, Any],
    root: Path,
    day: str,
) -> list[dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in _list(contract.get("required_inputs")):
        if isinstance(row, Mapping) and _text(row.get("data_item_id")).startswith("market."):
            req = _add_requirement(out, _text(row.get("data_item_id")), "price", "sleeve_input_contracts.required_inputs", row.get("reason"))
            if _text(row.get("expected_source_type")):
                req["expected_source_type"] = _text(row.get("expected_source_type"))
            if _text(row.get("expected_source")):
                req["expected_source"] = _text(row.get("expected_source"))
    for row in [*_list(diag.get("required_inputs_status")), *_list(t02.get("source_diagnostics", {}).get("candidate_generation", {}).get("required_inputs_status"))]:
        if isinstance(row, Mapping) and _text(row.get("data_item_id")).startswith("market."):
            _add_requirement(out, _text(row.get("data_item_id")), "price", "candidate_generation_diagnostics.required_inputs_status", row.get("reason"))
    for symbol in _string_list(eval_row.get("active_symbol_universe") or eval_row.get("allowed_symbols")):
        _add_requirement(out, f"market.price.{symbol}", "price", "sleeve_evaluation.active_symbol_universe", f"{_text(eval_row.get('engine_id'))} evaluated symbol {symbol}.")
    missing_text = " ".join(_string_list(t02.get("missing_inputs")) + [_text(eval_row.get("stderr_summary")), _text(eval_row.get("stdout_summary"))])
    for symbol in sorted(set(re.findall(r"market_data_snapshot_v1[:/](?:snapshots/\d{4}-\d{2}-\d{2}/)?([A-Z0-9.]+)", missing_text))):
        symbol = _clean_symbol(symbol.replace(".market_data_snapshot.v1.json", ""))
        if symbol:
            _add_requirement(out, f"market.price.{symbol}", "snapshot_json", "producer_missing_input_path", f"Producer reported missing snapshot path for {symbol}.")
    for symbol in sorted(set(re.findall(r"MISSING_BAR_FOR_DAY:\s*symbol=([A-Z0-9.]+)", missing_text))):
        req = _add_requirement(out, f"market.price.{symbol}", "historical_bar", "producer_reason_code", f"Producer reported missing historical bar for {symbol} on {day}.")
        req["requires_historical_day_bar"] = True
    for path in _missing_paths(missing_text):
        if "market_data_snapshot_v1" in path:
            symbol = _clean_symbol(Path(path).name.split(".")[0])
            req = _add_requirement(out, f"market.price.{symbol}", "snapshot_json", "producer_missing_input_path", f"Producer reported missing snapshot path {path}.")
            req.setdefault("expected_runtime_paths", []).append(path)
        elif "nav" in path.lower() or "positions" in path.lower():
            item_id = "runtime." + ("nav_snapshot" if "nav" in path.lower() else "positions_snapshot")
            req = _add_requirement(out, item_id, Path(path).name, "producer_missing_runtime_input_path", f"Producer reported missing runtime dependency {path}.")
            req["expected_source_type"] = "RUNTIME_TRUTH_FILE"
            req.setdefault("expected_runtime_paths", []).append(path)
    if not out and _text(t02.get("dormant_reason_code")) == "MISSING_MARKET_DATA":
        _add_requirement(out, "UNKNOWN_MARKET_DATA_REQUIREMENT", "UNKNOWN", "t02_missing_market_data_without_declared_item", "T02 reported missing market data but no deterministic item declaration was found.")
    for req in out.values():
        req.setdefault("expected_source", str(root / "reports" / "aegis_market_data_v1" / day / "market_data.v1.json"))
        req.setdefault("expected_source_type", "MARKET_DATA_ARTIFACT")
    return sorted(out.values(), key=lambda item: item["data_item_id"])


def _add_requirement(out: dict[str, dict[str, Any]], item_id: str, field: str, source: str, reason: Any) -> dict[str, Any]:
    row = out.setdefault(item_id, {"data_item_id": item_id, "required_fields": [], "requirement_sources": [], "requirement_reasons": []})
    if field and field not in row["required_fields"]:
        row["required_fields"].append(field)
    if source and source not in row["requirement_sources"]:
        row["requirement_sources"].append(source)
    text = _text(reason)
    if text and text not in row["requirement_reasons"]:
        row["requirement_reasons"].append(text)
    return row


def _resolve_requirement(item: Mapping[str, Any], sleeve_id: str, payloads: Mapping[str, Any], root: Path, day: str) -> dict[str, Any]:
    item_id = _text(item.get("data_item_id"))
    symbol = _symbol(item_id)
    runtime_paths = _string_list(item.get("expected_runtime_paths"))
    if item_id == "UNKNOWN_MARKET_DATA_REQUIREMENT":
        return _item_row(item, "DATA_REQUIREMENT_UNDECLARED", False, False, False, False, "UNDECLARED", [item_id], [item_id], [], [], item.get("expected_source"), item.get("expected_source_type"))
    if item_id.startswith("runtime."):
        exists = any(Path(path).exists() for path in runtime_paths)
        return _item_row(item, "NO_ACTION_REQUIRED" if exists else "SOURCE_AVAILABLE_ROUTING_MISSING", True, exists, exists, exists, "CURRENT" if exists else "MISSING_ROUTE", [] if exists else runtime_paths, [] if exists else runtime_paths, [], [], item.get("expected_source"), item.get("expected_source_type"))
    if not symbol:
        return _item_row(item, "UNSUPPORTED_DATA_DEPENDENCY", False, False, False, False, "UNSUPPORTED", [item_id], [], [], [item_id], item.get("expected_source"), item.get("expected_source_type"))

    input_status = _input_status(payloads, item_id)
    coverage = _coverage_row(payloads, symbol)
    attempts = _attempts(payloads, symbol)
    dataset_file = root / "market_data_snapshot_v1" / symbol / f"{day[:4]}.jsonl"
    dataset_has_day = _jsonl_has_day(dataset_file, day)
    source_exists = bool(input_status or coverage or attempts or dataset_file.exists() or symbol in _dataset_symbols(payloads))
    source_ingested = bool(input_status or dataset_has_day or dataset_file.exists())
    source_fresh = bool(_status_current(input_status) or dataset_has_day)
    routed = _routed_to_sleeve(payloads, sleeve_id, item_id, symbol, runtime_paths, bool(item.get("requires_historical_day_bar")))
    missing_items: list[str] = []
    missing_fields: list[str] = []
    stale_items: list[str] = []
    unsupported: list[str] = []
    quality = "CURRENT"
    resolution = "NO_ACTION_REQUIRED"

    if item.get("requires_historical_day_bar") and not dataset_has_day:
        quality = "INCOMPLETE"
        resolution = "SOURCE_AVAILABLE_BUT_INCOMPLETE" if source_exists else "SOURCE_NOT_CONFIGURED"
        missing_items.append(f"historical_bar.{symbol}.{day}")
        missing_fields.extend(["open", "high", "low", "close", "volume"])
    elif runtime_paths and not all(Path(path).exists() for path in runtime_paths):
        quality = "MISSING_ROUTE"
        resolution = "SOURCE_AVAILABLE_ROUTING_MISSING" if source_exists and source_ingested else "SOURCE_AVAILABLE_INGESTION_MISSING"
        missing_items.extend([path for path in runtime_paths if not Path(path).exists()])
        missing_fields.extend([Path(path).name for path in runtime_paths if not Path(path).exists()])
        routed = False
    elif not source_exists and _text(item.get("expected_source_type")) == "EXTERNAL_SOURCE_REQUIRED":
        quality = "SOURCE_NOT_AVAILABLE"
        resolution = "SOURCE_NOT_AVAILABLE"
        missing_items.append(item_id)
    elif not source_exists:
        quality = "SOURCE_NOT_CONFIGURED"
        resolution = "SOURCE_NOT_CONFIGURED"
        missing_items.append(item_id)
    elif source_exists and not source_ingested:
        quality = "NOT_INGESTED"
        resolution = "SOURCE_AVAILABLE_INGESTION_MISSING"
        missing_items.append(item_id)
    elif source_ingested and not source_fresh:
        quality = "STALE"
        resolution = "SOURCE_AVAILABLE_BUT_STALE"
        stale_items.append(item_id)
    elif not routed:
        quality = "MISSING_ROUTE"
        resolution = "SOURCE_AVAILABLE_ROUTING_MISSING"
        missing_items.append(item_id)
    return _item_row(item, resolution, source_exists, source_ingested, source_fresh, routed, quality, missing_items, missing_fields, stale_items, unsupported, _source_name(input_status, coverage, attempts, dataset_file), _source_type(input_status, coverage, dataset_file))


def _item_row(item: Mapping[str, Any], resolution: str, source_exists: bool, ingested: bool, fresh: bool, routed: bool, quality: str, missing: list[str], fields: list[str], stale: list[str], unsupported: list[str], expected_source: Any, expected_source_type: Any) -> dict[str, Any]:
    row = {
        "data_item_id": _text(item.get("data_item_id")),
        "required_fields": _string_list(item.get("required_fields")),
        "requirement_sources": _string_list(item.get("requirement_sources")),
        "requirement_reasons": _string_list(item.get("requirement_reasons")),
        "expected_source": _text(expected_source),
        "expected_source_type": _text(expected_source_type) or "MARKET_DATA_ARTIFACT",
        "source_exists": source_exists,
        "source_ingested": ingested,
        "source_fresh": fresh,
        "data_routed_to_sleeve": routed,
        "data_quality_status": quality,
        "missing_data_items": sorted(set(missing)),
        "missing_fields": sorted(set(fields)),
        "stale_data_items": sorted(set(stale)),
        "unsupported_data_items": sorted(set(unsupported)),
        "resolution_type": resolution,
    }
    row["item_hash"] = stable_hash_v1({**row, "item_hash": ""})
    return row


def _rollup_resolution(items: list[dict[str, Any]], requirements: list[dict[str, Any]]) -> str:
    if not requirements:
        return "DATA_REQUIREMENT_UNDECLARED"
    order = [
        "UNSUPPORTED_DATA_DEPENDENCY",
        "DATA_REQUIREMENT_UNDECLARED",
        "SOURCE_NOT_AVAILABLE",
        "SOURCE_NOT_CONFIGURED",
        "SOURCE_AVAILABLE_INGESTION_MISSING",
        "SOURCE_AVAILABLE_BUT_STALE",
        "SOURCE_AVAILABLE_BUT_INCOMPLETE",
        "SOURCE_AVAILABLE_ROUTING_MISSING",
        "UNKNOWN_DETERMINISTIC_BLOCKER",
    ]
    present = {_text(item.get("resolution_type")) for item in items}
    for code in order:
        if code in present:
            return code
    return "NO_ACTION_REQUIRED"


def _owner(resolution: str, items: list[dict[str, Any]]) -> tuple[str, bool]:
    if resolution == "NO_ACTION_REQUIRED":
        return "NONE", False
    if resolution == "SOURCE_NOT_AVAILABLE" or any(_text(item.get("expected_source_type")) == "EXTERNAL_SOURCE_REQUIRED" for item in items):
        return "DAVID", True
    return "AEGIS_SYSTEM", False


def _next_action(resolution: str, items: list[dict[str, Any]]) -> str:
    if resolution == "SOURCE_AVAILABLE_ROUTING_MISSING":
        return "Add or repair declarative routing/manifest materialization from existing ingested source data to the sleeve-required runtime path; do not change sleeve strategy logic."
    if resolution == "SOURCE_AVAILABLE_INGESTION_MISSING":
        return "Run or repair market data ingestion for the declared source and item; do not fabricate data."
    if resolution == "SOURCE_AVAILABLE_BUT_STALE":
        return "Refresh the existing source and regenerate downstream diagnostics after fresh data is ingested."
    if resolution == "SOURCE_AVAILABLE_BUT_INCOMPLETE":
        missing = ", ".join(sorted({value for item in items for value in _string_list(item.get("missing_data_items"))})[:8])
        return f"Ingest the missing fields/bars from the existing configured source: {missing}."
    if resolution == "SOURCE_NOT_CONFIGURED":
        return "Declare and configure an authoritative source for the required data item."
    if resolution == "SOURCE_NOT_AVAILABLE":
        return "David must provide the external source, vendor, file, calendar, API access, or subscription required by this data dependency."
    if resolution == "DATA_REQUIREMENT_UNDECLARED":
        return "Declare the exact required market data item and fields in an authoritative contract or registry before repairing ingestion or routing."
    if resolution == "UNSUPPORTED_DATA_DEPENDENCY":
        return "Add governed support for this dependency type before it can participate in signal generation."
    if resolution == "NO_ACTION_REQUIRED":
        return "No missing market data action is required by deterministic evidence."
    return "Collect additional deterministic evidence for this blocker."


def _summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {code: sum(1 for row in rows if row["resolution_type"] == code) for code in RESOLUTION_TYPES}
    return {
        "total_missing_market_data_sleeves": len(rows),
        "source_available_routing_missing_count": counts["SOURCE_AVAILABLE_ROUTING_MISSING"],
        "source_available_ingestion_missing_count": counts["SOURCE_AVAILABLE_INGESTION_MISSING"],
        "source_available_but_stale_count": counts["SOURCE_AVAILABLE_BUT_STALE"],
        "source_available_but_incomplete_count": counts["SOURCE_AVAILABLE_BUT_INCOMPLETE"],
        "source_not_configured_count": counts["SOURCE_NOT_CONFIGURED"],
        "source_not_available_count": counts["SOURCE_NOT_AVAILABLE"],
        "data_requirement_undeclared_count": counts["DATA_REQUIREMENT_UNDECLARED"],
        "unsupported_data_dependency_count": counts["UNSUPPORTED_DATA_DEPENDENCY"],
        "david_action_required_count": sum(1 for row in rows if row["david_action_required"]),
        "aegis_system_action_required_count": sum(1 for row in rows if row["owner"] == "AEGIS_SYSTEM"),
        "no_action_required_count": counts["NO_ACTION_REQUIRED"],
    }


def _answer(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No T02 MISSING_MARKET_DATA dormant sleeves were present for this day."
    parts = ", ".join(f"{row['sleeve_id']}={row['resolution_type']}" for row in rows)
    return f"{len(rows)} dormant sleeves were blocked by missing market data. Deterministic resolutions: {parts}."


def _input_status(payloads: Mapping[str, Any], item_id: str) -> dict[str, Any]:
    for row in [*_rows(payloads.get("market_data_inputs"), "input_records"), *_rows(payloads.get("market_data_readiness"), "input_statuses")]:
        if _text(row.get("data_item_id")) == item_id:
            return row
    return {}


def _coverage_row(payloads: Mapping[str, Any], symbol: str) -> dict[str, Any]:
    for row in _rows(payloads.get("market_data_coverage"), "coverage_rows"):
        if _text(row.get("symbol")) == symbol:
            return row
    return {}


def _attempts(payloads: Mapping[str, Any], symbol: str) -> list[dict[str, Any]]:
    return [row for row in _rows(payloads.get("provider_attempts"), "attempts") if _text(row.get("symbol")) == symbol]


def _dataset_symbols(payloads: Mapping[str, Any]) -> set[str]:
    data = _dict(payloads.get("dataset_manifest"))
    return set(_string_list(data.get("symbols")) + _string_list(data.get("symbols_requested")))


def _status_ok(row: Mapping[str, Any]) -> bool:
    return _upper(row.get("status") or row.get("validation_status")) in {"VALID", "CURRENT", "READY", "CERTIFIED"}


def _status_current(row: Mapping[str, Any]) -> bool:
    if not row:
        return False
    return _upper(row.get("status") or row.get("validation_status") or row.get("market_data_validation_status")) in {"VALID", "CURRENT", "CERTIFIED"}


def _routed_to_sleeve(payloads: Mapping[str, Any], sleeve_id: str, item_id: str, symbol: str, runtime_paths: list[str], historical: bool) -> bool:
    if runtime_paths:
        return all(Path(path).exists() for path in runtime_paths)
    if historical:
        return True
    for row in _rows(payloads.get("sleeve_input_contracts"), "contracts"):
        if _text(row.get("sleeve_id")) == sleeve_id and any(_text(req.get("data_item_id")) == item_id for req in _list(row.get("required_inputs")) if isinstance(req, Mapping)):
            return True
    for row in _rows(payloads.get("market_data_demand"), "demand_rows"):
        if _text(row.get("symbol")) == symbol and sleeve_id in _string_list(row.get("consumer_sleeves")):
            return True
    return False


def _jsonl_has_day(path: Path, day: str) -> bool:
    if not path.exists():
        return False
    try:
        with path.open() as handle:
            for line in handle:
                if day in line:
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    if _text(obj.get("day_utc")) == day or _text(obj.get("timestamp_utc")).startswith(day):
                        return True
    except OSError:
        return False
    return False


def _source_name(input_status: Mapping[str, Any], coverage: Mapping[str, Any], attempts: list[dict[str, Any]], dataset_file: Path) -> str:
    source = _text(input_status.get("source_artifact_path") or input_status.get("cache_path") or coverage.get("provider_result", {}).get("raw_output_path"))
    if source:
        return source
    if attempts:
        return _text(attempts[0].get("provider"))
    return str(dataset_file)


def _source_type(input_status: Mapping[str, Any], coverage: Mapping[str, Any], dataset_file: Path) -> str:
    provider = _text(input_status.get("source_vendor") or input_status.get("provider") or coverage.get("issue_provider"))
    if provider:
        return provider
    return "HISTORICAL_JSONL" if dataset_file.exists() else "MARKET_DATA_ARTIFACT"


def _quality_status(items: list[dict[str, Any]]) -> str:
    statuses = {_text(item.get("data_quality_status")) for item in items}
    for status in ("UNSUPPORTED", "UNDECLARED", "SOURCE_NOT_CONFIGURED", "NOT_INGESTED", "STALE", "INCOMPLETE", "MISSING_ROUTE"):
        if status in statuses:
            return status
    return "CURRENT" if items else "UNKNOWN"


def _requirement_sources(requirements: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "data_item_id": item["data_item_id"],
            "requirement_sources": _string_list(item.get("requirement_sources")),
            "requirement_reasons": _string_list(item.get("requirement_reasons")),
        }
        for item in requirements
    ]


def _join_unique(values: Any) -> str:
    return ", ".join(sorted({_text(value) for value in values if _text(value)}))


def _symbol(item_id: str) -> str:
    if item_id.startswith("market.price."):
        return _clean_symbol(item_id.removeprefix("market.price."))
    return ""


def _clean_symbol(value: str) -> str:
    return re.sub(r"[^A-Z0-9]+$", "", _text(value).upper())


def _missing_paths(text: str) -> list[str]:
    return re.findall(r"/[^\s;,\"]+", text or "")


def _first_match(rows: list[dict[str, Any]], sleeve_id: str, hypothesis_id: str) -> dict[str, Any]:
    for row in rows:
        text = json.dumps(row, sort_keys=True, default=str)
        if _text(row.get("sleeve_id") or row.get("engine_id")) == sleeve_id or (hypothesis_id and _text(row.get("hypothesis_id")) == hypothesis_id) or sleeve_id in text:
            return row
    return {}


def _rows(payload: Any, *keys: str) -> list[dict[str, Any]]:
    data = _dict(payload)
    for key in keys:
        value = data.get(key)
        if isinstance(value, list):
            return [dict(row) for row in value if isinstance(row, Mapping)]
    return []


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [_text(item) for item in value if _text(item)]
    if _text(value):
        return [_text(value)]
    return []


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()
