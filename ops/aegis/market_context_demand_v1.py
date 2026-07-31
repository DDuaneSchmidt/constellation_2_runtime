from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

from ops.aegis.context_requirement_profile_v1 import (
    load_or_build_context_requirement_profile_v1,
    profile_path_v1,
    profile_summary_for_output_v1,
    requirement_for_item_v1,
    severity_is_blocking_v1,
)
from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1
from ops.aegis.market_context_provider_health_v1 import build_provider_health_v1, provider_health_map_v1, provider_health_path_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/market_context_demand.v1.schema.json"
REPORT_FAMILY = "aegis_market_context_demand_v1"
FORMULA_VERSION = "aegis_market_context_formulas.v1"
CONSUMER_ARTIFACT = "event_market_snapshot_v1"
REPAIR_COMMAND = "npm run aegis:repair-runtime-readiness"
FETCH_COMMAND = "npm run aegis:refresh-market-data"
_ALLOWED_CERTIFIED_STATUSES = {"CURRENT"}

_CONTEXT_REQUIREMENTS = [
    {
        "context_item_id": "advance_decline_delta",
        "purpose": "BREADTH_REGIME",
        "freshness_requirement": "same trading session required",
        "source_requirement": "provider breadth source certified in aegis_market_data_v1",
        "type": "provider_breadth",
        "field": "advance_decline_delta",
    },
    {
        "context_item_id": "breadth_down_pct",
        "purpose": "BREADTH_REGIME",
        "freshness_requirement": "same trading session required",
        "source_requirement": "provider breadth source certified in aegis_market_data_v1",
        "type": "provider_breadth",
        "field": "breadth_down_pct",
    },
    {
        "context_item_id": "qqq_return_pct",
        "purpose": "INDEX_RETURN_CONTEXT",
        "freshness_requirement": "same trading session required",
        "source_requirement": "current certified market.price.QQQ plus previous certified history row",
        "type": "derived_return",
        "symbol": "QQQ",
        "window": 1,
    },
    {
        "context_item_id": "spy_return_pct",
        "purpose": "INDEX_RETURN_CONTEXT",
        "freshness_requirement": "same trading session required",
        "source_requirement": "current certified market.price.SPY plus previous certified history row",
        "type": "derived_return",
        "symbol": "SPY",
        "window": 1,
    },
    {
        "context_item_id": "spy_20d_return_pct",
        "purpose": "TREND_REGIME",
        "freshness_requirement": "same trading session required with 20 prior sessions",
        "source_requirement": "current certified market.price.SPY plus 20-session certified history",
        "type": "derived_return",
        "symbol": "SPY",
        "window": 20,
    },
    {
        "context_item_id": "spy_above_50dma",
        "purpose": "TREND_REGIME",
        "freshness_requirement": "same trading session required with 50-session lookback",
        "source_requirement": "current certified market.price.SPY plus 50-session certified history",
        "type": "derived_above_sma",
        "symbol": "SPY",
        "window": 50,
    },
    {
        "context_item_id": "vix_level",
        "purpose": "VOLATILITY_REGIME",
        "freshness_requirement": "same trading session required",
        "source_requirement": "current certified market.volatility.VIX",
        "type": "registry_value",
        "data_item_id": "market.volatility.VIX",
    },
    {
        "context_item_id": "vix_change_pct",
        "purpose": "VOLATILITY_REGIME",
        "freshness_requirement": "same trading session required",
        "source_requirement": "current certified market.volatility.VIX plus previous certified history row",
        "type": "derived_return",
        "symbol": "VIX",
        "window": 1,
        "registry_item_id": "market.volatility.VIX",
    },
]


def validate_market_context_demand_v1(payload: dict[str, Any]) -> None:
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_PATH)


def market_context_demand_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_FAMILY / day_utc / "market_context_demand.v1.json"


def write_market_context_demand_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    validate_market_context_demand_v1(payload)
    path = market_context_demand_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return {"json": str(path)}


def build_market_context_demand_v1(*, truth_root: Path, day_utc: str, generated_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated_at = generated_at_utc or now_utc_v1()
    calendar_path, calendar_context = _calendar_context_v1(root, day_utc)
    market_path, market_payload = latest_json_v1(root, "aegis_market_data_v1", day_utc, "market_data.v1.json")
    registry_path, registry_payload = latest_json_v1(root, "aegis_data_registry_v1", day_utc, "data_registry.v1.json")
    market_payload = market_payload if isinstance(market_payload, dict) else {}
    registry_payload = registry_payload if isinstance(registry_payload, dict) else {}
    registry_items = _registry_map(registry_payload)
    profile_payload = load_or_build_context_requirement_profile_v1(truth_root=root, day_utc=day_utc, generated_at_utc=generated_at)
    provider_health_payload = build_provider_health_v1(truth_root=root, day_utc=day_utc, generated_at_utc=generated_at)
    provider_health = provider_health_map_v1(provider_health_payload)
    provider_health_json_path = provider_health_path_v1(truth_root=root, day_utc=day_utc)
    base_source_hashes = {str(path): _sha256_file(Path(path)) for path in [market_path, registry_path] if path}
    if calendar_path:
        base_source_hashes[str(calendar_path)] = _sha256_file(calendar_path)
    base_source_hashes[str(provider_health_json_path)] = str(provider_health_payload.get("canonical_json_hash") or "")
    context_items = [
        _apply_calendar_context_v1(
            _build_requirement_row(
                requirement=requirement,
                truth_root=root,
                day_utc=day_utc,
                market_payload=market_payload,
                registry_items=registry_items,
                market_path=Path(market_path) if market_path else None,
                base_source_hashes=base_source_hashes,
                provider_health=provider_health,
                profile_payload=profile_payload,
            ),
            calendar_context=calendar_context,
        )
        for requirement in _CONTEXT_REQUIREMENTS
    ]
    source_artifacts: list[str] = []
    source_hashes = dict(base_source_hashes)
    profile_report_path = str(profile_path_v1(truth_root=root, day_utc=day_utc))
    source_hashes[profile_report_path] = str(profile_payload.get("canonical_json_hash") or "")
    for path in [market_path, registry_path, str(calendar_path) if calendar_path else "", str(provider_health_json_path), profile_report_path]:
        if path:
            source_artifacts.append(str(path))
    for row in context_items:
        for path in row.get("source_artifact_paths") or []:
            if path and path not in source_artifacts:
                source_artifacts.append(path)
            if path and path not in source_hashes and Path(path).exists():
                source_hashes[path] = _sha256_file(Path(path))
    counts = Counter(str(row.get("fulfillment_status") or "UNKNOWN") for row in context_items)
    payload = {
        "schema_id": "aegis_market_context_demand",
        "schema_version": "v1",
        "artifact_id": "aegis_market_context_demand_v1",
        "day_utc": day_utc,
        "generated_at_utc": generated_at,
        "consumer_artifact": CONSUMER_ARTIFACT,
        "market_context_items": context_items,
        "provider_health_path": str(provider_health_json_path),
        "provider_health": provider_health_payload,
        "status_counts": dict(sorted(counts.items())),
        "source_artifacts": sorted(set(source_artifacts)),
        "source_hashes": {key: source_hashes[key] for key in sorted(source_hashes)},
        "next_repair_commands": sorted({row.get("next_repair_command") or REPAIR_COMMAND for row in context_items if _context_item_blocks_v1(row)}),
        "canonical_json_hash": "",
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def market_context_snapshot_inputs_v1(payload: dict[str, Any], *, truth_root: Path, day_utc: str) -> dict[str, Any]:
    rows = payload.get("market_context_items") if isinstance(payload.get("market_context_items"), list) else []
    item_map = {str(row.get("context_item_id") or ""): row for row in rows if isinstance(row, dict)}
    root = Path(truth_root).resolve()
    _calendar_path, calendar_context = _calendar_context_v1(root, day_utc)
    market_path, market_payload = latest_json_v1(root, "aegis_market_data_v1", day_utc, "market_data.v1.json")
    registry_path, registry_payload = latest_json_v1(root, "aegis_data_registry_v1", day_utc, "data_registry.v1.json")
    market_payload = market_payload if isinstance(market_payload, dict) else {}
    registry_payload = registry_payload if isinstance(registry_payload, dict) else {}
    symbols = market_payload.get("symbols") if isinstance(market_payload.get("symbols"), dict) else {}
    breadth = market_payload.get("breadth") if isinstance(market_payload.get("breadth"), dict) else {}
    source_lineage = [{"artifact_type": "market_context_demand_v1", "path": str(market_context_demand_path_v1(truth_root=root, day_utc=day_utc))}]
    for path in payload.get("source_artifacts") or []:
        source_lineage.append({"artifact_type": "market_context_source", "path": str(path)})
    blockers = [
        {
            "context_item_id": row.get("context_item_id"),
            "status": row.get("fulfillment_status"),
            "failure_reason": row.get("failure_reason"),
            "next_repair_command": row.get("next_repair_command") or REPAIR_COMMAND,
        }
        for row in rows
        if isinstance(row, dict) and _context_item_blocks_v1(row)
    ]
    return {
        "snapshot_id": f"event_market_snapshot:{day_utc}:market_context_demand_v1",
        "day_utc": day_utc,
        "market_data_status": market_payload.get("status") or "UNKNOWN",
        "market_data_mode": market_payload.get("market_data_mode") or market_payload.get("mode") or "",
        "market_open_status": str(calendar_context.get("market_open_status") or "UNKNOWN"),
        "trading_day_type": str(calendar_context.get("trading_day_type") or "UNKNOWN"),
        "source_timestamp_utc": _latest_timestamp(rows),
        "latest_source_timestamp_utc": _latest_timestamp(rows),
        "data_snapshot_refs": [str(market_context_demand_path_v1(truth_root=root, day_utc=day_utc))],
        "source_lineage": source_lineage,
        "source_hashes": payload.get("source_hashes") if isinstance(payload.get("source_hashes"), dict) else {},
        "provider_statuses": market_payload.get("provider_results") if isinstance(market_payload.get("provider_results"), list) else [],
        "global_context_status": _global_context_status(registry_payload),
        "per_symbol_status": {
            symbol: {
                "freshness_status": row.get("freshness_status"),
                "market_session_date": row.get("market_session_date"),
                "provider": row.get("provider") or row.get("source"),
                "data_timestamp_utc": row.get("data_timestamp_utc"),
            }
            for symbol, row in symbols.items()
            if isinstance(row, dict)
        },
        "breadth_status": {
            "freshness_status": breadth.get("freshness_status"),
            "market_session_date": breadth.get("market_session_date"),
            "provider": breadth.get("source"),
            "data_timestamp_utc": breadth.get("data_timestamp_utc"),
        },
        "market_context_demand_path": str(market_context_demand_path_v1(truth_root=root, day_utc=day_utc)),
        "market_context_items": rows,
        "market_context_status_counts": payload.get("status_counts") if isinstance(payload.get("status_counts"), dict) else {},
        "market_context_blockers": blockers,
        "market_context_overall_status": "CONTEXT_CERTIFIED" if not blockers else "CONTEXT_BLOCKED",
        "context_requirement_profile": profile_summary_for_output_v1(load_or_build_context_requirement_profile_v1(truth_root=root, day_utc=day_utc)),
        "spy": _instrument_from_market_payload("SPY", symbols, item_map.get("spy_return_pct")),
        "qqq": _instrument_from_market_payload("QQQ", symbols, item_map.get("qqq_return_pct")),
        "vix": _instrument_from_market_payload("VIX", symbols, item_map.get("vix_change_pct"), level_item=item_map.get("vix_level")),
        "breadth": {
            "advance_decline_delta": _certified_value(item_map.get("advance_decline_delta")),
            "breadth_down_pct": _certified_value(item_map.get("breadth_down_pct")),
        },
        "trend_metrics": {
            "spy_20d_return_pct": _certified_value(item_map.get("spy_20d_return_pct")),
            "spy_above_50dma": _certified_value(item_map.get("spy_above_50dma")),
        },
        "volatility_metrics": {
            "vix_level": _certified_value(item_map.get("vix_level")),
            "vix_change_pct": _certified_value(item_map.get("vix_change_pct")),
        },
        "inputs": {
            "qqq_return_pct": _certified_value(item_map.get("qqq_return_pct")),
            "spy_return_pct": _certified_value(item_map.get("spy_return_pct")),
            "advance_decline_delta": _certified_value(item_map.get("advance_decline_delta")),
            "breadth_down_pct": _certified_value(item_map.get("breadth_down_pct")),
            "vix_level": _certified_value(item_map.get("vix_level")),
            "vix_change_pct": _certified_value(item_map.get("vix_change_pct")),
            "spy_20d_return_pct": _certified_value(item_map.get("spy_20d_return_pct")),
            "spy_above_50dma": _certified_value(item_map.get("spy_above_50dma")),
        },
    }


def _read_jsonl_rows_v1(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if isinstance(row, dict):
                rows.append(row)
    except (OSError, json.JSONDecodeError):
        return []
    return rows


def _calendar_context_v1(truth_root: Path, day_utc: str) -> tuple[Path | None, dict[str, str]]:
    path = Path(truth_root).resolve() / "market_calendar_v1" / "NYSE" / f"{day_utc[:4]}.jsonl"
    for row in _read_jsonl_rows_v1(path):
        row_day = str(row.get("date") or row.get("day_utc") or "")[:10]
        if row_day != day_utc:
            continue
        if row.get("is_trading_session") is True:
            return path, {"trading_day_type": "TRADING_DAY", "market_open_status": "OPEN"}
        if row.get("is_trading_session") is False:
            return path, {"trading_day_type": "NON_TRADING_DAY", "market_open_status": "CLOSED"}
    return (path if path.exists() else None), {"trading_day_type": "UNKNOWN", "market_open_status": "UNKNOWN"}


def _apply_calendar_context_v1(row: dict[str, Any], *, calendar_context: dict[str, str]) -> dict[str, Any]:
    if str(calendar_context.get("trading_day_type") or "").upper() != "NON_TRADING_DAY":
        return row
    row["demanded"] = False
    row["fulfillment_status"] = "CONTEXT_NOT_DEMANDED"
    row["certification_status"] = "BLOCKED"
    row["certified"] = False
    row["computed"] = False
    row["fetched"] = False
    row["value"] = ""
    row["failure_reason"] = "Market context is not demanded because the requested day is not a trading session."
    row["next_repair_command"] = ""
    row["blocker_severity"] = "INFORMATIONAL"
    requirement = row.get("context_requirement") if isinstance(row.get("context_requirement"), dict) else {}
    if requirement:
        row["context_requirement"] = {**requirement, "blocker_severity": "INFORMATIONAL"}
    row["same_day_required_now"] = False
    row["policy_mode"] = str(row.get("policy_mode") or "NON_TRADING_DAY_NO_CONTEXT_DEMANDED")
    return row


def _build_requirement_row(*, requirement: dict[str, Any], truth_root: Path, day_utc: str, market_payload: dict[str, Any], registry_items: dict[str, dict[str, Any]], market_path: Path | None, base_source_hashes: dict[str, str], provider_health: dict[str, dict[str, Any]], profile_payload: dict[str, Any]) -> dict[str, Any]:
    row = {
        "context_item_id": requirement["context_item_id"],
        "purpose": requirement["purpose"],
        "freshness_requirement": requirement["freshness_requirement"],
        "source_requirement": requirement["source_requirement"],
        "consumer_artifact": CONSUMER_ARTIFACT,
        "demanded": True,
        "fulfillment_status": "CONTEXT_NOT_FETCHED",
        "certification_status": "BLOCKED",
        "fetched": False,
        "computed": False,
        "certified": False,
        "provider": "",
        "session_date": "",
        "timestamp_utc": "",
        "formula_version": "" if requirement["type"] in {"provider_breadth", "registry_value"} else FORMULA_VERSION,
        "formula_description": "" if requirement["type"] in {"provider_breadth", "registry_value"} else _formula_description(requirement),
        "source_artifact_paths": [],
        "source_hashes": {},
        "value": "",
        "failure_reason": "",
        "next_repair_command": FETCH_COMMAND,
        "source_label": "",
        "policy_mode": "",
        "same_day_required_now": False,
        "active_context_profile_id": "",
        "context_requirement": {},
        "blocker_severity": "BLOCKING",
        "consuming_capability": "",
        "allowed_fallback_reference_types": [],
        "context_requirement_profile_path": "",
        "context_requirement_profile_hash": "",
    }
    _apply_requirement_profile_fields(row, profile_payload=profile_payload, data_item_id=str(requirement.get("data_item_id") or _data_item_id_for_requirement(requirement)))
    if requirement["type"] == "provider_breadth":
        return _provider_breadth_row(row=row, requirement=requirement, day_utc=day_utc, market_payload=market_payload, market_path=market_path, base_source_hashes=base_source_hashes, provider_health=provider_health)
    if requirement["type"] == "registry_value":
        return _registry_value_row(row=row, requirement=requirement, day_utc=day_utc, registry_items=registry_items, provider_health=provider_health)
    return _derived_row(row=row, requirement=requirement, truth_root=truth_root, day_utc=day_utc, registry_items=registry_items, provider_health=provider_health)


def _provider_breadth_row(*, row: dict[str, Any], requirement: dict[str, Any], day_utc: str, market_payload: dict[str, Any], market_path: Path | None, base_source_hashes: dict[str, str], provider_health: dict[str, dict[str, Any]]) -> dict[str, Any]:
    breadth = market_payload.get("breadth") if isinstance(market_payload.get("breadth"), dict) else {}
    health = provider_health.get(requirement["context_item_id"], {})
    if market_path:
        row["source_artifact_paths"] = [str(market_path)]
        row["source_hashes"] = {str(market_path): base_source_hashes.get(str(market_path), "")}
    _merge_provider_health(row, health)
    row["provider"] = str(health.get("provider") or breadth.get("source") or "")
    row["session_date"] = str(health.get("session_date") or breadth.get("market_session_date") or "")
    row["timestamp_utc"] = str(health.get("timestamp_utc") or breadth.get("data_timestamp_utc") or "")
    value = breadth.get(requirement["field"])
    if value in (None, ""):
        value = health.get("value")
    freshness = str(breadth.get("freshness_status") or "MISSING").upper()
    if value in (None, ""):
        health_status = str(health.get("health_status") or "CONTEXT_NOT_FETCHED")
        row["fulfillment_status"] = "CONTEXT_NOT_FETCHED" if health_status == "PROVIDER_NOT_CONFIGURED" else health_status
        base_reason = str(health.get("failure_reason") or f"{requirement['context_item_id']} missing from breadth source")
        row["failure_reason"] = f"PROVIDER_NOT_CONFIGURED: {base_reason}" if health_status == "PROVIDER_NOT_CONFIGURED" else base_reason
        row["next_repair_command"] = str(health.get("next_repair_action") or row["next_repair_command"])
        return row
    row["fetched"] = True
    row["value"] = _fmt(value)
    if row["session_date"] != day_utc or freshness == "STALE":
        row["fulfillment_status"] = "CONTEXT_STALE"
        row["failure_reason"] = str(health.get("failure_reason") or f"breadth source session {row['session_date'] or 'UNKNOWN'} is not current")
        row["next_repair_command"] = str(health.get("next_repair_action") or row["next_repair_command"])
        return row
    if freshness != "CURRENT":
        row["fulfillment_status"] = "CONTEXT_UNCERTIFIED"
        row["failure_reason"] = str(health.get("failure_reason") or f"breadth freshness {freshness or 'UNKNOWN'} is not certified")
        row["next_repair_command"] = str(health.get("next_repair_action") or row["next_repair_command"])
        return row
    row["certified"] = True
    row["certification_status"] = "CERTIFIED_REFERENCE" if _is_reference_certified_v1(health) else "CERTIFIED"
    row["fulfillment_status"] = "CONTEXT_CERTIFIED"
    return row


def _registry_value_row(*, row: dict[str, Any], requirement: dict[str, Any], day_utc: str, registry_items: dict[str, dict[str, Any]], provider_health: dict[str, dict[str, Any]]) -> dict[str, Any]:
    item = registry_items.get(requirement["data_item_id"], {})
    health = provider_health.get(requirement["context_item_id"], {})
    row["source_artifact_paths"] = [str(item.get("source_artifact_path") or "")] if item else []
    row["source_artifact_paths"] = [path for path in row["source_artifact_paths"] if path]
    row["source_hashes"] = {path: str(item.get("source_hash") or "") for path in row["source_artifact_paths"]}
    _merge_provider_health(row, health)
    row["provider"] = str(health.get("provider") or item.get("provider") or "")
    row["session_date"] = str(health.get("session_date") or item.get("market_session_date") or "")
    row["timestamp_utc"] = str(health.get("timestamp_utc") or item.get("data_timestamp_utc") or "")
    status = str(item.get("status") or "MISSING").upper()
    value = item.get("value")
    if value in (None, "") and str(health.get("health_status") or "") == "CONTEXT_CERTIFIED":
        value = health.get("value")
    if value in (None, ""):
        health_status = str(health.get("health_status") or ("CONTEXT_NOT_FETCHED" if status == "MISSING" else "CONTEXT_UNCERTIFIED"))
        row["fulfillment_status"] = "CONTEXT_NOT_FETCHED" if health_status == "PROVIDER_NOT_CONFIGURED" else health_status
        base_reason = str(health.get("failure_reason") or f"{requirement['data_item_id']} has no usable value")
        row["failure_reason"] = f"PROVIDER_NOT_CONFIGURED: {base_reason}" if health_status == "PROVIDER_NOT_CONFIGURED" else base_reason
        row["next_repair_command"] = str(health.get("next_repair_action") or row["next_repair_command"])
        return row
    row["fetched"] = True
    row["value"] = _fmt(value)
    if _is_reference_certified_v1(health):
        row["certified"] = True
        row["certification_status"] = "CERTIFIED_REFERENCE"
        row["fulfillment_status"] = "CONTEXT_CERTIFIED"
        return row
    if row["session_date"] != day_utc or status == "STALE":
        row["fulfillment_status"] = "CONTEXT_STALE"
        row["failure_reason"] = str(health.get("failure_reason") or f"{requirement['data_item_id']} session {row['session_date'] or 'UNKNOWN'} is not current")
        return row
    if status not in _ALLOWED_CERTIFIED_STATUSES:
        row["fulfillment_status"] = "CONTEXT_UNCERTIFIED"
        row["failure_reason"] = str(health.get("failure_reason") or f"{requirement['data_item_id']} registry status {status or 'UNKNOWN'} is not certified")
        return row
    row["certified"] = True
    row["certification_status"] = "CERTIFIED_REFERENCE" if _is_reference_certified_v1(health) else "CERTIFIED"
    row["fulfillment_status"] = "CONTEXT_CERTIFIED"
    return row


def _derived_row(*, row: dict[str, Any], requirement: dict[str, Any], truth_root: Path, day_utc: str, registry_items: dict[str, dict[str, Any]], provider_health: dict[str, dict[str, Any]]) -> dict[str, Any]:
    symbol = str(requirement["symbol"]).upper()
    registry_item_id = str(requirement.get("registry_item_id") or (f"market.price.{symbol}" if symbol != "VIX" else "market.volatility.VIX"))
    item = registry_items.get(registry_item_id, {})
    status = str(item.get("status") or "MISSING").upper()
    health = provider_health.get("vix_level", {}) if symbol == "VIX" else {}
    current_value = _float(item.get("value"))
    if current_value is None and str(health.get("health_status") or "") == "CONTEXT_CERTIFIED":
        current_value = _float(health.get("value"))
    row["provider"] = str(item.get("provider") or "")
    row["session_date"] = str(item.get("market_session_date") or "")
    row["timestamp_utc"] = str(item.get("data_timestamp_utc") or "")
    source_path = str(item.get("source_artifact_path") or "")
    history_path, history = _history_rows(truth_root, symbol)
    row["source_artifact_paths"] = [path for path in [source_path, str(history_path)] if path]
    row["source_hashes"] = {path: _sha256_file(Path(path)) for path in row["source_artifact_paths"] if Path(path).exists()}
    if current_value is None:
        row["fulfillment_status"] = str(health.get("health_status") or ("CONTEXT_NOT_FETCHED" if status == "MISSING" else "CONTEXT_UNCERTIFIED"))
        row["failure_reason"] = str(health.get("failure_reason") or f"{registry_item_id} has no current certified value")
        row["next_repair_command"] = str(health.get("next_repair_action") or row["next_repair_command"])
        return row
    row["fetched"] = True
    if _is_reference_certified_v1(health):
        row["provider"] = str(health.get("provider") or row.get("provider") or "")
        row["session_date"] = str(health.get("session_date") or row.get("session_date") or "")
        row["timestamp_utc"] = str(health.get("timestamp_utc") or row.get("timestamp_utc") or "")
    elif row["session_date"] != day_utc or status == "STALE":
        row["value"] = _fmt(current_value)
        row["fulfillment_status"] = "CONTEXT_STALE"
        row["failure_reason"] = str(health.get("failure_reason") or f"{registry_item_id} session {row['session_date'] or 'UNKNOWN'} is not current")
        return row
    if not _is_reference_certified_v1(health) and status not in _ALLOWED_CERTIFIED_STATUSES:
        row["value"] = _fmt(current_value)
        row["fulfillment_status"] = "CONTEXT_UNCERTIFIED"
        row["failure_reason"] = str(health.get("failure_reason") or f"{registry_item_id} status {status or 'UNKNOWN'} is not certified")
        return row
    if requirement["type"] == "derived_return":
        result = _compute_return(current_value=current_value, history=history, day_utc=day_utc, window=int(requirement.get("window") or 1), symbol=symbol)
    else:
        result = _compute_above_sma(current_value=current_value, history=history, day_utc=day_utc, window=int(requirement.get("window") or 50), symbol=symbol)
    row["computed"] = result["computed"]
    row["value"] = _fmt(result["value"]) if result["value"] is not None else ""
    if not result["computed"]:
        row["fulfillment_status"] = result["status"]
        row["failure_reason"] = result["reason"]
        return row
    row["certified"] = True
    row["certification_status"] = "CERTIFIED"
    row["fulfillment_status"] = "CONTEXT_CERTIFIED"
    return row


def _compute_return(*, current_value: float, history: list[dict[str, Any]], day_utc: str, window: int, symbol: str) -> dict[str, Any]:
    closes = _history_closes_before_day(history, day_utc)
    if len(closes) < window:
        return {"computed": False, "status": "CONTEXT_NOT_COMPUTABLE", "reason": f"{symbol} needs {window} prior sessions of history", "value": None}
    prior = closes[-window]
    if prior in (None, 0):
        return {"computed": False, "status": "CONTEXT_NOT_COMPUTABLE", "reason": f"{symbol} prior close unavailable for {window}-session return", "value": None}
    return {"computed": True, "status": "CONTEXT_CERTIFIED", "reason": "", "value": _fmt(((current_value - prior) / prior) * 100.0)}


def _compute_above_sma(*, current_value: float, history: list[dict[str, Any]], day_utc: str, window: int, symbol: str) -> dict[str, Any]:
    closes = _history_closes_before_day(history, day_utc)
    if len(closes) < window - 1:
        return {"computed": False, "status": "CONTEXT_NOT_COMPUTABLE", "reason": f"{symbol} needs {window} sessions of history for {window}DMA", "value": None}
    sample = closes[-(window - 1):] + [current_value]
    if len(sample) != window:
        return {"computed": False, "status": "CONTEXT_NOT_COMPUTABLE", "reason": f"{symbol} {window}DMA sample incomplete", "value": None}
    return {"computed": True, "status": "CONTEXT_CERTIFIED", "reason": "", "value": current_value > (sum(sample) / float(window))}


def _history_rows(root: Path, symbol: str) -> tuple[Path, list[dict[str, Any]]]:
    path = Path(root).resolve() / "market_data_snapshot_v1" / symbol / "2026.jsonl"
    if not path.exists():
        folder = Path(root).resolve() / "market_data_snapshot_v1" / symbol
        years = sorted(folder.glob("*.jsonl")) if folder.exists() else []
        path = years[-1] if years else path
    rows: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            if isinstance(payload, dict) and str(payload.get("symbol") or "").upper() == symbol:
                rows.append(payload)
    except Exception:
        rows = []
    rows.sort(key=lambda row: str(row.get("date") or row.get("timestamp_utc") or ""))
    return path, rows


def _history_closes_before_day(history: list[dict[str, Any]], day_utc: str) -> list[float]:
    values: list[float] = []
    for row in history:
        row_day = str(row.get("date") or row.get("timestamp_utc") or "")[:10]
        if row_day >= day_utc:
            continue
        close = _float(row.get("close"))
        if close is not None:
            values.append(close)
    return values


def _registry_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = payload.get("data_items") if isinstance(payload.get("data_items"), list) else []
    return {str(row.get("data_item_id") or ""): row for row in rows if isinstance(row, dict)}


def _formula_description(requirement: dict[str, Any]) -> str:
    if requirement["type"] == "derived_return":
        return f"pct_change(current_session_close, close_{int(requirement.get('window') or 1)}_sessions_back)"
    return f"current_session_close > sma_{int(requirement.get('window') or 50)}"


def _global_context_status(payload: dict[str, Any]) -> dict[str, Any]:
    rows = payload.get("data_items") if isinstance(payload.get("data_items"), list) else []
    return {
        str(row.get("data_item_id") or ""): {
            "status": row.get("status"),
            "provider": row.get("provider"),
            "data_timestamp_utc": row.get("data_timestamp_utc"),
        }
        for row in rows
        if isinstance(row, dict) and str(row.get("data_item_id") or "").startswith(("market.price.", "market.volatility.", "market.breadth."))
    }


def _instrument_from_market_payload(symbol: str, symbols: dict[str, Any], return_item: dict[str, Any] | None, *, level_item: dict[str, Any] | None = None) -> dict[str, Any]:
    row = symbols.get(symbol) if isinstance(symbols.get(symbol), dict) else {}
    price_value = _certified_value(level_item) if level_item is not None else row.get("last_price") or row.get("close") or ""
    return {
        "symbol": symbol,
        "price": _stringify(price_value),
        "prev_close": _stringify(row.get("prev_close") or ""),
        "return_pct": _stringify(_certified_value(return_item)),
        "source_label": str((level_item or {}).get("source_label") or ""),
        "session_date": str((level_item or {}).get("session_date") or row.get("market_session_date") or ""),
        "policy_mode": str((level_item or {}).get("policy_mode") or ""),
        "same_day_required_now": bool((level_item or {}).get("same_day_required_now", False)),
    }


def _latest_timestamp(rows: list[dict[str, Any]]) -> str:
    values = [str(row.get("timestamp_utc") or "") for row in rows if isinstance(row, dict) and str(row.get("timestamp_utc") or "")]
    return max(values) if values else ""


def _certified_value(row: dict[str, Any] | None) -> Any:
    if not isinstance(row, dict):
        return ""
    if not _is_context_certified_v1(row):
        return ""
    value = row.get("value")
    return value if value is not None else ""


def _is_context_certified_v1(row: dict[str, Any] | None) -> bool:
    if not isinstance(row, dict):
        return False
    return str(row.get("fulfillment_status") or "").upper() == "CONTEXT_CERTIFIED" and str(row.get("certification_status") or "").upper() in {"CERTIFIED", "CERTIFIED_REFERENCE"}


def _stringify(value: Any) -> str:
    if isinstance(value, bool):
        return value
    if value in (None, ""):
        return ""
    return str(value)


def _fmt(value: Any) -> str:
    if isinstance(value, bool):
        return value
    try:
        return f"{float(value):.6f}".rstrip("0").rstrip(".")
    except Exception:
        return str(value)


def _context_item_blocks_v1(row: dict[str, Any] | None) -> bool:
    if not isinstance(row, dict) or _is_context_certified_v1(row):
        return False
    return severity_is_blocking_v1(row.get("context_requirement") if isinstance(row.get("context_requirement"), dict) else {"blocker_severity": row.get("blocker_severity")})


def _data_item_id_for_requirement(requirement: dict[str, Any]) -> str:
    if requirement.get("type") == "provider_breadth":
        field = str(requirement.get("field") or "")
        return "market.breadth.down_pct" if field == "breadth_down_pct" else f"market.breadth.{field}"
    if requirement.get("type") == "registry_value":
        return str(requirement.get("data_item_id") or "")
    symbol = str(requirement.get("symbol") or "").upper()
    return "market.volatility.VIX" if symbol == "VIX" else f"market.price.{symbol}"


def _apply_requirement_profile_fields(row: dict[str, Any], *, profile_payload: dict[str, Any], data_item_id: str) -> None:
    requirement = requirement_for_item_v1(profile_payload, data_item_id, str(row.get("context_item_id") or ""))
    summary = profile_summary_for_output_v1(profile_payload)
    row["active_context_profile_id"] = str(summary.get("active_profile_id") or "")
    row["context_requirement"] = requirement
    row["blocker_severity"] = str(requirement.get("blocker_severity") or "BLOCKING")
    row["consuming_capability"] = str(requirement.get("consuming_capability") or "")
    row["allowed_fallback_reference_types"] = [str(item) for item in requirement.get("allowed_fallback_reference_types") or []]
    row["context_requirement_profile_path"] = str(summary.get("path") or "")
    row["context_requirement_profile_hash"] = str(summary.get("canonical_json_hash") or "")


def _merge_provider_health(row: dict[str, Any], health: dict[str, Any]) -> None:
    checked_paths = [str(path) for path in (health.get("checked_evidence_paths") or []) if str(path)]
    for path in checked_paths:
        if path not in row["source_artifact_paths"]:
            row["source_artifact_paths"].append(path)
    for path, value in (health.get("checked_evidence_hashes") or {}).items():
        if path and value:
            row["source_hashes"][str(path)] = str(value)
    row["source_label"] = str(health.get("source_label") or row.get("source_label") or "")
    row["policy_mode"] = str(health.get("policy_mode") or row.get("policy_mode") or "")
    row["same_day_required_now"] = bool(health.get("same_day_required_now", row.get("same_day_required_now", False)))


def _is_reference_certified_v1(health: dict[str, Any]) -> bool:
    return str(health.get("certification_status") or "").upper() == "CERTIFIED_REFERENCE" and str(health.get("health_status") or "") == "CONTEXT_CERTIFIED"


def _float(value: Any) -> float | None:
    try:
        text = str(value).strip().replace("%", "").replace(",", "")
        return float(text) if text else None
    except Exception:
        return None


def _sha256_file(path: Path) -> str:
    try:
        import hashlib
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except Exception:
        return ""
