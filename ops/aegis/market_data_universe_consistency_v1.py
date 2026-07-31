from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import latest_json_v1, read_json_v1, write_json_v1
from ops.aegis.market_data.symbol_alias_registry_v1 import canonicalize_symbol_list_v1, normalize_market_symbol_v1
from ops.aegis.research_mapping_rules_v1 import report_path_v1, stable_hash_v1, text_v1

FAMILY = "aegis_market_data_universe_consistency_v1"
FILENAME = "market_data_universe_consistency.v1.json"
POLICY_VERSION = "AEGIS_MARKET_DATA_UNIVERSE_CONSISTENCY_V1"
OIL_HYPOTHESIS_ID = "ehp_cdbd8fe683acb622"
OIL_NAME = "Oil shock reversals across energy ETFs"
OIL_REQUIRED_SYMBOLS = ["DBC", "SPY", "USO", "XLE"]
SAFETY = {
    "research_only": True,
    "no_broker_execution": True,
    "no_trade_advice": True,
    "no_live_trading": True,
    "no_real_capital": True,
    "no_autonomous_execution": True,
    "no_order_management": True,
    "no_forced_candidates": True,
    "no_strategy_rule_changes": True,
    "no_paper_lifecycle_changes": True,
    "no_exit_rule_changes": True,
    "broker_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allowed": False,
    "autonomous_execution_allowed": False,
    "safety_gates_changed": False,
}


def market_data_universe_consistency_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, FAMILY, str(day_utc), FILENAME)


def build_market_data_universe_consistency_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    paths, payloads = _load_inputs(root, day)
    demand = payloads.get("demand", {})
    market = payloads.get("market", {})
    coverage = payloads.get("coverage", {})
    mark = payloads.get("mark_coverage", {})
    ledger = payloads.get("ledger", {})
    requested = _symbols(demand.get("requested_symbols"))
    returned = _symbols((market.get("symbols") or {}).keys() if isinstance(market.get("symbols"), dict) else [])
    certified = _certified_symbols(day, market, coverage)
    consumers = _consumer_rows(payloads=payloads, paths=paths, requested=requested, returned=returned, certified=certified)
    consolidated_required = _symbols(symbol for row in consumers for symbol in row.get("required_symbols", []))
    missing_required_symbols = _symbols(symbol for row in consumers for symbol in row.get("missing_from_request", []) + row.get("requested_but_not_returned", []) + row.get("returned_but_not_certified", []))
    blockers = [row for row in consumers if row.get("coverage_status") not in {"COMPLETE", "NO_SYMBOLS_REQUIRED", "NO_MARKET_SETUP"}]
    oil = next((row for row in consumers if row.get("consumer_name") == "Oil Shock candidate flow"), {})
    paper = next((row for row in consumers if row.get("consumer_name") == "open paper positions"), {})
    overall_status = "COMPLETE" if not blockers else "INCOMPLETE"
    message = "Market data universe complete for current research consumers."
    if blockers:
        top = blockers[0]
        missing = top.get("missing_from_request") or top.get("requested_but_not_returned") or top.get("returned_but_not_certified") or []
        symbol_text = ", ".join(missing) if missing else "required market data"
        message = f"Market data universe incomplete: {top.get('consumer_name')} requires {symbol_text}. Owner: {top.get('owner')}. No David action required." if top.get("owner") != "DAVID" else f"Market data universe incomplete: {top.get('consumer_name')} requires {symbol_text}. Owner: DAVID."
    payload = {
        "schema_id": "aegis_market_data_universe_consistency",
        "schema_version": "v1",
        "artifact_id": FAMILY,
        "day_utc": day,
        "generated_at_utc": _now(),
        "policy_version": POLICY_VERSION,
        "status": overall_status,
        "coverage_status": overall_status,
        "overall_universe_coverage": overall_status,
        "ui_message": message,
        "consolidated_required_symbol_count": len(consolidated_required),
        "requested_symbol_count": len(requested),
        "returned_symbol_count": len(returned),
        "certified_symbol_count": len(certified),
        "missing_required_symbol_count": len(missing_required_symbols),
        "consolidated_required_symbols": consolidated_required,
        "requested_symbols": requested,
        "returned_symbols": returned,
        "certified_symbols": certified,
        "missing_required_symbols": missing_required_symbols,
        "consumer_rows": consumers,
        "top_affected_consumers": [row.get("consumer_name") for row in blockers[:5]],
        "remaining_blockers": [{"consumer_name": row.get("consumer_name"), "blocker_code": row.get("blocker_code"), "owner": row.get("owner"), "symbols": row.get("missing_from_request") or row.get("requested_but_not_returned") or row.get("returned_but_not_certified") or []} for row in blockers],
        "oil_shock_symbol_coverage": oil,
        "paper_position_symbol_coverage": paper,
        "paper_position_mark_coverage": {
            "open_position_count": coverage.get("open_position_count") or mark.get("total_positions") or 0,
            "marked_position_count": coverage.get("marked_position_count") or mark.get("marked_positions") or 0,
            "mark_coverage_by_position_pct": coverage.get("mark_coverage_by_position_pct") if coverage.get("mark_coverage_by_position_pct") is not None else mark.get("coverage_pct"),
            "missing_mark_symbols": coverage.get("missing_mark_symbols") or [],
        },
        "david_action_required": any(bool(row.get("david_action_required")) for row in consumers),
        "source_artifact_paths": {key: str(path or "") for key, path in paths.items()},
        "source_artifact_hashes": {key: _sha256(path) for key, path in paths.items()},
        **SAFETY,
        "safety": dict(SAFETY),
    }
    payload["content_hash"] = stable_hash_v1({key: value for key, value in payload.items() if key not in {"generated_at_utc", "content_hash"}})
    return payload


def write_market_data_universe_consistency_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any]) -> Path:
    return write_json_v1(market_data_universe_consistency_path_v1(truth_root=truth_root, day_utc=day_utc), payload)


def _load_inputs(root: Path, day: str) -> tuple[dict[str, Path | None], dict[str, dict[str, Any]]]:
    specs = {
        "demand": ("aegis_market_data_demand_v1", "market_data_demand.v1.json"),
        "market": ("aegis_market_data_v1", "market_data.v1.json"),
        "coverage": ("aegis_market_data_coverage_v1", "market_data_coverage.v1.json"),
        "ledger": ("aegis_paper_position_ledger_v1", "paper_position_ledger.v1.json"),
        "producer": ("aegis_oil_shock_candidate_producer_v1", "oil_shock_candidate_producer.v1.json"),
        "flow": ("aegis_oil_shock_candidate_flow_v1", "oil_shock_candidate_flow.v1.json"),
        "enablement": ("aegis_oil_shock_candidate_flow_enablement_v1", "oil_shock_candidate_flow_enablement.v1.json"),
        "evidence_packets": ("aegis_hypothesis_evidence_packet_v1", "evidence_packets.v1.json"),
        "paper_blueprint": ("aegis_paper_sleeve_blueprint_v1", "paper_sleeve_blueprint.v1.json"),
        "macro": ("aegis_macro_calendar_data_readiness_v1", "macro_calendar_data_readiness.v1.json"),
        "outcome": ("aegis_outcome_registry_v1", "outcome_registry.v1.json"),
        "mark_coverage": ("aegis_mark_coverage_v1", "mark_coverage.v1.json"),
        "quality": ("aegis_research_quality_engine_v1", "research_quality_engine.v1.json"),
        "follow_through": ("aegis_research_follow_through_control_v1", "research_follow_through_control.v1.json"),
        "exit_recommendations": ("aegis_exit_recommendations_v1", "exit_recommendations.v1.json"),
    }
    paths: dict[str, Path | None] = {}
    payloads: dict[str, dict[str, Any]] = {}
    for key, (family, filename) in specs.items():
        path, payload = latest_json_v1(root, family, day, filename)
        paths[key] = path
        payloads[key] = payload if isinstance(payload, dict) else {}
    return paths, payloads


def _consumer_rows(*, payloads: dict[str, dict[str, Any]], paths: dict[str, Path | None], requested: list[str], returned: list[str], certified: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    ledger_symbols = _open_position_symbols(payloads.get("ledger", {}))
    rows.append(_row("open paper positions", "paper_mark_coverage", ledger_symbols, requested, returned, certified, [paths.get("ledger"), paths.get("coverage"), paths.get("mark_coverage")]))
    producer = payloads.get("producer", {})
    oil_required = _symbols(producer.get("required_market_symbols") or OIL_REQUIRED_SYMBOLS)
    rows.append(_row("Oil Shock candidate flow", "generated_hypothesis_candidate_producer", oil_required, requested, returned, certified, [paths.get("producer"), paths.get("flow"), paths.get("enablement")], no_market_setup=_is_no_market_setup(payloads)))
    rows.extend(_active_sleeve_rows(payloads, paths, requested, returned, certified))
    rows.extend(_generated_hypothesis_rows(payloads, paths, requested, returned, certified))
    rows.append(_row("macro calendar readiness", "macro_calendar_readiness", _symbols(payloads.get("macro", {}).get("required_symbols") or []), requested, returned, certified, [paths.get("macro")], david_action=bool(payloads.get("macro", {}).get("david_action_required")) and bool(payloads.get("macro", {}).get("required_symbols"))))
    rows.append(_row("exit evaluation", "exit_evaluation", ledger_symbols, requested, returned, certified, [paths.get("exit_recommendations"), paths.get("ledger")]))
    rows.append(_row("outcome validation", "outcome_validation", ledger_symbols, requested, returned, certified, [paths.get("outcome"), paths.get("ledger")]))
    rows.append(_row("mark coverage", "mark_coverage", ledger_symbols, requested, returned, certified, [paths.get("coverage"), paths.get("mark_coverage"), paths.get("ledger")]))
    rows.append(_row("research quality / follow-through", "research_quality_follow_through", _symbols([*ledger_symbols, *oil_required]), requested, returned, certified, [paths.get("quality"), paths.get("follow_through")]))
    return rows


def _active_sleeve_rows(payloads: dict[str, dict[str, Any]], paths: dict[str, Path | None], requested: list[str], returned: list[str], certified: list[str]) -> list[dict[str, Any]]:
    demand_rows = payloads.get("demand", {}).get("demand_rows") if isinstance(payloads.get("demand", {}).get("demand_rows"), list) else []
    by_consumer: dict[str, set[str]] = {}
    for row in demand_rows:
        if not isinstance(row, dict) or not row.get("required"):
            continue
        for consumer in row.get("consumer_sleeves") or []:
            name = text_v1(consumer)
            if name.startswith("C2_"):
                by_consumer.setdefault(name, set()).add(normalize_market_symbol_v1(row.get("symbol")))
    return [_row(f"active sleeve {name}", "active_sleeve", sorted(symbols), requested, returned, certified, [paths.get("demand")]) for name, symbols in sorted(by_consumer.items())]


def _generated_hypothesis_rows(payloads: dict[str, dict[str, Any]], paths: dict[str, Path | None], requested: list[str], returned: list[str], certified: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    packets = payloads.get("evidence_packets", {}).get("evidence_packets")
    for item in packets if isinstance(packets, list) else []:
        if not isinstance(item, dict):
            continue
        name = text_v1(item.get("hypothesis_name") or item.get("name") or item.get("hypothesis_id") or "generated hypothesis")
        symbols = _symbols(item.get("instrument_universe") or item.get("required_market_symbols") or (OIL_REQUIRED_SYMBOLS if item.get("hypothesis_id") == OIL_HYPOTHESIS_ID or name == OIL_NAME else []))
        out.append(_row(f"generated hypothesis {name}", "generated_hypothesis", symbols, requested, returned, certified, [paths.get("evidence_packets")]))
    if not any("Oil shock" in row.get("consumer_name", "") or "Oil Shock" in row.get("consumer_name", "") for row in out):
        out.append(_row(f"generated hypothesis {OIL_NAME}", "generated_hypothesis", OIL_REQUIRED_SYMBOLS, requested, returned, certified, [paths.get("evidence_packets"), paths.get("producer")]))
    return out


def _row(consumer_name: str, consumer_type: str, required_symbols: list[str], requested: list[str], returned: list[str], certified: list[str], source_paths: list[Path | None], david_action: bool = False, no_market_setup: bool = False) -> dict[str, Any]:
    required = _symbols(required_symbols)
    req_set, ret_set, cert_set = set(requested), set(returned), set(certified)
    missing_from_request = sorted(set(required) - req_set)
    requested_but_not_returned = sorted(set(required).intersection(req_set) - ret_set)
    returned_but_not_certified = sorted(set(required).intersection(ret_set) - cert_set)
    if not required:
        status, blocker, owner = "NO_SYMBOLS_REQUIRED", "NONE", "NONE"
    elif no_market_setup and not (missing_from_request or requested_but_not_returned or returned_but_not_certified):
        status, blocker, owner = "NO_MARKET_SETUP", "NO_MARKET_SETUP", "MARKET_CONDITIONS"
    elif missing_from_request:
        status, blocker, owner = "BLOCKED", "SYMBOL_NOT_REQUESTED", "AEGIS_SYSTEM"
    elif requested_but_not_returned:
        status, blocker, owner = "BLOCKED", "SYMBOL_REQUESTED_NOT_RETURNED", "MARKET_PROVIDER"
    elif returned_but_not_certified:
        status, blocker, owner = "BLOCKED", "SYMBOL_RETURNED_NOT_CERTIFIED", "AEGIS_SYSTEM"
    else:
        status, blocker, owner = "COMPLETE", "NONE", "NONE"
    paths = [str(path) for path in source_paths if path]
    return {
        "consumer_name": consumer_name,
        "consumer_type": consumer_type,
        "required_symbols": required,
        "requested_symbols": sorted(set(required).intersection(req_set)),
        "returned_symbols": sorted(set(required).intersection(ret_set)),
        "certified_symbols": sorted(set(required).intersection(cert_set)),
        "missing_from_request": missing_from_request,
        "requested_but_not_returned": requested_but_not_returned,
        "returned_but_not_certified": returned_but_not_certified,
        "coverage_status": status,
        "blocker_code": blocker,
        "owner": "DAVID" if david_action else owner,
        "david_action_required": bool(david_action),
        "source_artifact_paths": paths,
        "source_artifact_hashes": {str(path): _sha256(path) for path in source_paths if path},
    }


def _certified_symbols(day: str, market: dict[str, Any], coverage: dict[str, Any]) -> list[str]:
    out = set(_symbols(coverage.get("certified_symbols") or []))
    symbols = market.get("symbols") if isinstance(market.get("symbols"), dict) else {}
    for symbol, row in symbols.items():
        if not isinstance(row, dict):
            continue
        session = text_v1(row.get("market_session_date") or row.get("returned_data_date") or market.get("market_session_date"))
        if text_v1(row.get("freshness_status")).upper() == "CURRENT" and (not session or session == day):
            out.add(normalize_market_symbol_v1(symbol))
    return sorted(symbol for symbol in out if symbol)


def _open_position_symbols(payload: dict[str, Any]) -> list[str]:
    rows = payload.get("open_positions") if isinstance(payload.get("open_positions"), list) else []
    return _symbols(row.get("symbol") for row in rows if isinstance(row, dict))


def _is_no_market_setup(payloads: dict[str, dict[str, Any]]) -> bool:
    for key in ("producer", "flow", "enablement"):
        payload = payloads.get(key, {})
        text = " ".join(str(x) for x in [payload.get("producer_status"), payload.get("candidate_flow_status"), payload.get("exact_blocker"), *(payload.get("reason_codes") if isinstance(payload.get("reason_codes"), list) else [])]).upper()
        if "NO_MARKET_SETUP" in text:
            return True
    return False


def _symbols(values: Any) -> list[str]:
    if isinstance(values, str):
        values = [values]
    return canonicalize_symbol_list_v1(values if values is not None else [])


def _sha256(path: Path | None) -> str:
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest() if path else ""
    except Exception:
        return ""


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
