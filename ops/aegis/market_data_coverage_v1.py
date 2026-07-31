from __future__ import annotations

import csv
import hashlib
from io import StringIO
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, write_json_v1
from ops.aegis.market_data.symbol_alias_registry_v1 import canonicalize_symbol_list_v1, normalize_market_symbol_v1

REPORT_FAMILY = "aegis_market_data_coverage_v1"
REPAIR_COMMAND = "TARGET_DAY={day} npm run aegis:repair-input-contracts"


def build_market_data_coverage_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    demand_path, demand = latest_json_v1(root, "aegis_market_data_demand_v1", day_utc, "market_data_demand.v1.json")
    market_path, market = latest_json_v1(root, "aegis_market_data_v1", day_utc, "market_data.v1.json")
    registry_path, registry = latest_json_v1(root, "aegis_data_registry_v1", day_utc, "data_registry.v1.json")
    inputs_path, inputs = latest_json_v1(root, "market_data_inputs_v1", day_utc, "market_data_inputs.v1.json")
    contracts_path, contracts = latest_json_v1(root, "aegis_sleeve_input_contracts_v1", day_utc, "sleeve_input_contracts.v1.json")
    ledger_path, ledger = latest_json_v1(root, "aegis_paper_position_ledger_v1", day_utc, "paper_position_ledger.v1.json")
    demand_rows = demand.get("demand_rows") if isinstance(demand.get("demand_rows"), list) else []
    required_symbols = canonicalize_symbol_list_v1(demand.get("required_symbols") if isinstance(demand.get("required_symbols"), list) else [row.get("symbol") for row in demand_rows if isinstance(row, dict) and row.get("required")])
    market_symbols = market.get("symbols") if isinstance(market.get("symbols"), dict) else {}
    registry_by_id = {str(row.get("data_item_id") or ""): row for row in registry.get("data_items", []) if isinstance(row, dict)}
    inputs_by_id = {str(row.get("data_item_id") or ""): row for row in inputs.get("input_records", []) if isinstance(row, dict)}
    attempts = market.get("provider_attempts") if isinstance(market.get("provider_attempts"), list) else []
    attempts_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in attempts:
        if not isinstance(row, dict):
            continue
        symbol = normalize_market_symbol_v1(row.get("symbol") or row.get("canonical_symbol"))
        if symbol:
            attempts_by_symbol.setdefault(symbol, []).append(row)
    demand_by_symbol = {str(row.get("symbol") or "").upper(): row for row in demand_rows if isinstance(row, dict)}
    rows: list[dict[str, Any]] = []
    certified: list[str] = []
    missing: list[str] = []
    stale: list[str] = []
    unavailable: list[str] = []
    for symbol in required_symbols:
        item_id = "market.volatility.VIX" if symbol == "VIX" else f"market.price.{symbol}"
        market_row = market_symbols.get(symbol) if isinstance(market_symbols.get(symbol), dict) else {}
        registry_row = registry_by_id.get(item_id, {})
        input_row = inputs_by_id.get(item_id, {})
        market_status = str(market_row.get("freshness_status") or ("MISSING" if not market_row else "UNKNOWN")).upper()
        registry_status = str(registry_row.get("status") or "MISSING").upper()
        input_status = str(input_row.get("validation_status") or input_row.get("status") or "MISSING").upper()
        session = str(market_row.get("market_session_date") or registry_row.get("market_session_date") or input_row.get("market_session_date") or "")
        is_current = market_status == "CURRENT" and registry_status == "CURRENT" and input_status in {"VALID", "CURRENT"} and (not session or session == day_utc)
        is_missing = market_status == "MISSING" or registry_status == "MISSING" or input_status in {"MISSING", "UNAVAILABLE_EXTERNAL_SOURCE"}
        is_stale = (market_status == "STALE" or registry_status == "STALE" or input_status == "STALE" or (session and session != day_utc)) and not is_missing
        status = "CERTIFIED" if is_current else ("MISSING" if is_missing else ("STALE" if is_stale else "UNAVAILABLE"))
        if status == "CERTIFIED":
            certified.append(symbol)
        elif status == "MISSING":
            missing.append(symbol)
        elif status == "STALE":
            stale.append(symbol)
        else:
            unavailable.append(symbol)
        provider_attempts = attempts_by_symbol.get(symbol, [])
        issue_cause = _issue_cause(status=status, provider_attempts=provider_attempts, market_status=market_status, registry_status=registry_status, input_status=input_status)
        issue_provider = _issue_provider(provider_attempts, market_row)
        demand_row = demand_by_symbol.get(symbol, {})
        rows.append({
            "symbol": symbol,
            "required_data_item": item_id,
            "coverage_status": status,
            "certified_current_session": is_current,
            "source_universe": str(demand_row.get("source_universe") or ""),
            "source_universe_path": str(demand_row.get("source_universe_path") or ""),
            "symbol_universe_hash": str(demand_row.get("symbol_universe_hash") or ""),
            "demanded": bool(demand_row),
            "demand_sources": list(demand_row.get("demand_sources") or []),
            "consumer_sleeves": list(demand_row.get("consumer_sleeves") or []),
            "provider_attempted": bool(provider_attempts),
            "provider_result": _provider_result(provider_attempts, market),
            "issue_cause": issue_cause,
            "issue_provider": issue_provider,
            "provider_attempts": provider_attempts,
            "market_data_status": market_status,
            "data_registry_status": registry_status,
            "market_data_inputs_status": input_status,
            "market_session_date": session,
            "data_timestamp_utc": str(market_row.get("data_timestamp_utc") or registry_row.get("data_timestamp_utc") or input_row.get("data_timestamp_utc") or input_row.get("source_timestamp_utc") or ""),
            "stale_reason": _stale_reason(day_utc=day_utc, status=status, market_status=market_status, registry_status=registry_status, input_status=input_status, session=session, attempts=provider_attempts, market_row=market_row, input_row=input_row),
        })
    required_count = len(required_symbols)
    certified_count = len(certified)
    missing_count = len(missing)
    stale_count = len(stale)
    coverage_pct = round((certified_count / required_count) * 100, 4) if required_count else 100.0
    consumer_sleeves = _consumer_sleeves(rows)
    blocking_consumers = _blocking_consumers(rows, contracts)
    ledger_missing = ledger_path is None or not ledger
    ledger_day = str(ledger.get("day_utc") or "") if isinstance(ledger, dict) else ""
    ledger_invalid = bool(ledger_missing or (ledger_day and ledger_day != day_utc))
    open_position_mark_coverage = _open_position_mark_coverage(
        root=root,
        day_utc=day_utc,
        ledger=ledger,
        ledger_path=ledger_path,
        ledger_invalid=ledger_invalid,
        market=market,
        inputs=inputs,
        requested_symbols=canonicalize_symbol_list_v1(demand.get("requested_symbols") if isinstance(demand.get("requested_symbols"), list) else []),
        attempts_by_symbol=attempts_by_symbol,
    )
    if ledger_invalid:
        coverage_status = "INVALID"
    elif str(open_position_mark_coverage.get("coverage_status") or "").upper() == "BLOCKED":
        coverage_status = "BLOCKED"
    else:
        coverage_status = "READY" if missing_count == 0 and stale_count == 0 and not unavailable else "BLOCKED"
    return {
        "schema_id": "aegis_market_data_coverage",
        "schema_version": "v1",
        "artifact_id": "aegis_market_data_coverage_v1",
        "day_utc": day_utc,
        "generated_at_utc": now_utc_v1(),
        "status": coverage_status,
        "coverage_status": coverage_status,
        "invalid_reason_codes": ["STALE_OR_MISSING_LEDGER_INPUT"] if ledger_invalid else [],
        "required_symbol_count": required_count,
        "certified_symbol_count": certified_count,
        "missing_symbol_count": missing_count,
        "stale_symbol_count": stale_count,
        "unavailable_symbol_count": len(unavailable),
        "coverage_pct": coverage_pct,
        "certified_symbols": certified,
        "missing_symbols": missing,
        "stale_symbols": stale,
        "unavailable_symbols": unavailable,
        "provider_attempts": attempts,
        "consumer_sleeves": consumer_sleeves,
        "blocking_consumers": blocking_consumers,
        "coverage_rows": rows,
        "issue_groups": _issue_groups(rows),
        "stale_symbols_by_cause_provider": _issue_groups([row for row in rows if row.get("coverage_status") == "STALE"]),
        "repair_action": REPAIR_COMMAND.format(day=day_utc),
        "open_position_mark_coverage": open_position_mark_coverage,
        "open_position_count": open_position_mark_coverage.get("open_position_count", 0),
        "marked_position_count": open_position_mark_coverage.get("marked_position_count", 0),
        "missing_mark_position_count": open_position_mark_coverage.get("missing_mark_position_count", 0),
        "unique_missing_symbol_count": open_position_mark_coverage.get("unique_missing_symbol_count", 0),
        "mark_coverage_by_position_pct": open_position_mark_coverage.get("mark_coverage_by_position_pct", 100.0),
        "mark_coverage_by_entry_notional_pct": open_position_mark_coverage.get("mark_coverage_by_entry_notional_pct", 100.0),
        "missing_mark_symbols": open_position_mark_coverage.get("missing_symbols", []),
        "missing_reason_by_symbol": open_position_mark_coverage.get("missing_reason_by_symbol", {}),
        "position_mark_rows": open_position_mark_coverage.get("position_rows", []),
        "position_exclusion_rows": open_position_mark_coverage.get("position_exclusion_rows", []),
        "remaining_blocker_reason_codes": open_position_mark_coverage.get("remaining_blocker_reason_codes", []),
        "raw_available_but_not_canonical": open_position_mark_coverage.get("raw_available_but_not_canonical", []),
        "fetch_missing_symbols": open_position_mark_coverage.get("fetch_missing_symbols", []),
        "sources": {
            "market_data_demand": _source_ref(demand_path),
            "market_data": _source_ref(market_path),
            "data_registry": _source_ref(registry_path),
            "market_data_inputs": _source_ref(inputs_path),
            "sleeve_input_contracts": _source_ref(contracts_path),
            "paper_position_ledger": _source_ref(ledger_path),
        },
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }


def write_market_data_coverage_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "market_data_coverage.v1.json", payload)
    txt_path = out_dir / "market_data_coverage.v1.txt"
    csv_path = out_dir / "market_data_coverage.matrix.csv"
    txt_path.write_text(render_market_data_coverage_v1(payload), encoding="utf-8")
    csv_path.write_text(render_market_data_coverage_csv_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "txt": str(txt_path), "matrix": str(csv_path)}


def render_market_data_coverage_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS MARKET DATA COVERAGE v1",
        f"day_utc: {payload.get('day_utc')}",
        f"status: {payload.get('status')}",
        f"coverage_status: {payload.get('coverage_status')}",
        f"invalid_reason_codes: {', '.join(payload.get('invalid_reason_codes') or [])}",
        f"required_symbol_count: {payload.get('required_symbol_count')}",
        f"certified_symbol_count: {payload.get('certified_symbol_count')}",
        f"missing_symbol_count: {payload.get('missing_symbol_count')}",
        f"stale_symbol_count: {payload.get('stale_symbol_count')}",
        f"coverage_pct: {payload.get('coverage_pct')}",
        f"open_position_count: {payload.get('open_position_count')}",
        f"marked_position_count: {payload.get('marked_position_count')}",
        f"missing_mark_position_count: {payload.get('missing_mark_position_count')}",
        f"mark_coverage_by_position_pct: {payload.get('mark_coverage_by_position_pct')}",
        f"mark_coverage_by_entry_notional_pct: {payload.get('mark_coverage_by_entry_notional_pct')}",
        f"missing_mark_symbols: {', '.join(payload.get('missing_mark_symbols') or [])}",
        f"raw_available_but_not_canonical: {', '.join(payload.get('raw_available_but_not_canonical') or [])}",
        f"fetch_missing_symbols: {', '.join(payload.get('fetch_missing_symbols') or [])}",
        f"remaining_blocker_reason_codes: {', '.join(payload.get('remaining_blocker_reason_codes') or [])}",
        f"blocking_consumers: {', '.join(payload.get('blocking_consumers') or [])}",
        f"repair_action: {payload.get('repair_action')}",
        "broker_execution_allowed: false",
        "autonomous_execution_allowed: false",
        "trade_advice_allowed: false",
    ]
    return "\n".join(lines) + "\n"


def render_market_data_coverage_csv_v1(payload: dict[str, Any]) -> str:
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=["symbol", "coverage_status", "issue_cause", "issue_provider", "demanded", "provider_attempted", "market_data_status", "data_registry_status", "market_data_inputs_status", "market_session_date", "consumer_sleeves", "stale_reason"])
    writer.writeheader()
    for row in payload.get("coverage_rows") or []:
        writer.writerow({
            "symbol": row.get("symbol", ""),
            "coverage_status": row.get("coverage_status", ""),
            "issue_cause": row.get("issue_cause", ""),
            "issue_provider": row.get("issue_provider", ""),
            "demanded": bool(row.get("demanded")),
            "provider_attempted": bool(row.get("provider_attempted")),
            "market_data_status": row.get("market_data_status", ""),
            "data_registry_status": row.get("data_registry_status", ""),
            "market_data_inputs_status": row.get("market_data_inputs_status", ""),
            "market_session_date": row.get("market_session_date", ""),
            "consumer_sleeves": "|".join(row.get("consumer_sleeves") or []),
            "stale_reason": row.get("stale_reason", ""),
        })
    return out.getvalue()


def _open_position_mark_coverage(
    *,
    root: Path,
    day_utc: str,
    ledger: dict[str, Any],
    ledger_path: Path | None,
    ledger_invalid: bool,
    market: dict[str, Any],
    inputs: dict[str, Any],
    requested_symbols: list[str],
    attempts_by_symbol: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    if ledger_invalid:
        return {
            "coverage_status": "INVALID",
            "invalid_reason_codes": ["STALE_OR_MISSING_LEDGER_INPUT"],
            "open_position_count": 0,
            "marked_position_count": 0,
            "missing_mark_position_count": 0,
            "unique_missing_symbol_count": 0,
            "mark_coverage_by_position_pct": 0.0,
            "mark_coverage_by_entry_notional_pct": 0.0,
            "total_entry_notional": 0.0,
            "marked_entry_notional": 0.0,
            "missing_symbols": [],
            "missing_reason_by_symbol": {},
            "raw_available_but_not_canonical": [],
            "fetch_missing_symbols": [],
            "position_rows": [],
            "position_exclusion_rows": [],
            "remaining_blocker_reason_codes": ["STALE_OR_MISSING_LEDGER_INPUT"],
            "source_artifact_path": str(ledger_path or ""),
        }
    open_positions = ledger.get("open_positions") if isinstance(ledger.get("open_positions"), list) else []
    market_symbols = market.get("symbols") if isinstance(market.get("symbols"), dict) else {}
    input_status_by_symbol = _input_status_by_symbol(inputs)
    raw_symbols = _raw_symbols(root=root, day_utc=day_utc)
    requested_symbol_set = set(canonicalize_symbol_list_v1(requested_symbols))
    open_count = 0
    marked_count = 0
    total_entry_notional = 0.0
    marked_entry_notional = 0.0
    missing_symbols: set[str] = set()
    missing_reason_by_symbol: dict[str, str] = {}
    raw_available: set[str] = set()
    fetch_missing: set[str] = set()
    rows: list[dict[str, Any]] = []
    exclusions: list[dict[str, Any]] = []
    blocker_codes: set[str] = set()
    for row in open_positions:
        if not isinstance(row, dict):
            continue
        symbol = normalize_market_symbol_v1(row.get("symbol"))
        if not symbol:
            exclusions.append({
                "position_id": str(row.get("position_id") or ""),
                "mark_status": "POSITION_EXCLUDED_WITH_REASON",
                "exclusion_reason": "MISSING_POSITION_SYMBOL",
            })
            blocker_codes.add("POSITION_EXCLUDED_WITH_REASON")
            continue
        open_count += 1
        notional = _float(row.get("entry_notional") or _mul(row.get("quantity"), row.get("entry_price")))
        total_entry_notional += notional
        market_row = market_symbols.get(symbol) if isinstance(market_symbols.get(symbol), dict) else {}
        mark_status = _position_mark_status(day_utc=day_utc, symbol=symbol, market_row=market_row, requested_symbols=requested_symbol_set)
        if mark_status == "MARK_AVAILABLE":
            marked_count += 1
            marked_entry_notional += notional
        else:
            missing_symbols.add(symbol)
            blocker_codes.add(mark_status)
            reason = _mark_missing_reason(symbol=symbol, day_utc=day_utc, market_row=market_row, raw_symbols=raw_symbols, input_status_by_symbol=input_status_by_symbol, attempts=attempts_by_symbol.get(symbol, []), mark_status=mark_status)
            missing_reason_by_symbol.setdefault(symbol, reason)
            if symbol in raw_symbols:
                raw_available.add(symbol)
            else:
                fetch_missing.add(symbol)
        rows.append({
            "position_id": str(row.get("position_id") or ""),
            "symbol": symbol,
            "entry_notional": round(notional, 6),
            "mark_status": mark_status,
            "certified_mark_present": mark_status == "MARK_AVAILABLE",
            "missing_reason": "" if mark_status == "MARK_AVAILABLE" else missing_reason_by_symbol.get(symbol, "MISSING_MARK"),
        })
    missing_count = max(open_count - marked_count, 0)
    return {
        "coverage_status": "READY" if missing_count == 0 else "BLOCKED",
        "invalid_reason_codes": [],
        "open_position_count": open_count,
        "marked_position_count": marked_count,
        "missing_mark_position_count": missing_count,
        "unique_missing_symbol_count": len(missing_symbols),
        "mark_coverage_by_position_pct": round((marked_count / open_count) * 100, 4) if open_count else 100.0,
        "mark_coverage_by_entry_notional_pct": round((marked_entry_notional / total_entry_notional) * 100, 4) if total_entry_notional else 100.0,
        "total_entry_notional": round(total_entry_notional, 6),
        "marked_entry_notional": round(marked_entry_notional, 6),
        "missing_symbols": sorted(missing_symbols),
        "missing_reason_by_symbol": {key: missing_reason_by_symbol[key] for key in sorted(missing_reason_by_symbol)},
        "raw_available_but_not_canonical": sorted(raw_available),
        "fetch_missing_symbols": sorted(fetch_missing),
        "position_rows": rows,
        "position_exclusion_rows": exclusions,
        "remaining_blocker_reason_codes": sorted(blocker_codes),
        "source_artifact_path": str(ledger_path or ""),
    }


def _certified_mark(*, day_utc: str, row: dict[str, Any]) -> bool:
    if not row:
        return False
    if str(row.get("freshness_status") or "").upper() != "CURRENT":
        return False
    if str(row.get("market_session_date") or "") != day_utc:
        return False
    return _float(row.get("last_price") or row.get("close")) > 0




def _position_mark_status(*, day_utc: str, symbol: str, market_row: dict[str, Any], requested_symbols: set[str]) -> str:
    if symbol not in requested_symbols:
        return "SYMBOL_NOT_REQUESTED"
    if not market_row:
        return "SYMBOL_NOT_IN_MARKET_DATA"
    if not str(market_row.get("data_timestamp_utc") or market_row.get("source_timestamp_utc") or market_row.get("retrieved_at_utc") or ""):
        return "MISSING_MARK_TIMESTAMP"
    if _float(market_row.get("last_price") or market_row.get("close")) <= 0:
        return "MISSING_MARK"
    if str(market_row.get("freshness_status") or "").upper() != "CURRENT":
        return "STALE_MARK"
    if str(market_row.get("market_session_date") or "") != day_utc:
        return "STALE_MARK"
    if not _certified_mark(day_utc=day_utc, row=market_row):
        return "MARK_CERTIFICATION_FAILED"
    return "MARK_AVAILABLE"

def _input_status_by_symbol(inputs: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for row in inputs.get("input_records", []) if isinstance(inputs.get("input_records"), list) else []:
        if not isinstance(row, dict):
            continue
        symbol = normalize_market_symbol_v1(row.get("symbol") or str(row.get("data_item_id") or "").rsplit(".", 1)[-1])
        if symbol:
            out[symbol] = str(row.get("validation_status") or row.get("status") or "UNKNOWN").upper()
    return out


def _raw_symbols(*, root: Path, day_utc: str) -> set[str]:
    raw_dir = root / "reports" / "aegis_market_data_v1" / day_utc / "raw"
    if not raw_dir.exists():
        return set()
    return {normalize_market_symbol_v1(path.stem) for path in raw_dir.glob("**/*") if path.is_file() and normalize_market_symbol_v1(path.stem)}


def _mark_missing_reason(*, symbol: str, day_utc: str, market_row: dict[str, Any], raw_symbols: set[str], input_status_by_symbol: dict[str, str], attempts: list[dict[str, Any]], mark_status: str) -> str:
    prefix = mark_status if mark_status in {"SYMBOL_NOT_REQUESTED", "SYMBOL_NOT_IN_MARKET_DATA", "MISSING_MARK_TIMESTAMP", "MISSING_MARK", "STALE_MARK", "MARK_CERTIFICATION_FAILED"} else "MISSING_MARK"
    if market_row:
        freshness = str(market_row.get("freshness_status") or "UNKNOWN").upper()
        session = str(market_row.get("market_session_date") or "")
        if freshness != "CURRENT":
            return f"{prefix}:CANONICAL_MARK_NOT_CURRENT:{freshness}"
        if session and session != day_utc:
            return f"{prefix}:CANONICAL_MARK_WRONG_SESSION:{session}"
        if not str(market_row.get("data_timestamp_utc") or market_row.get("source_timestamp_utc") or market_row.get("retrieved_at_utc") or ""):
            return f"{prefix}:CANONICAL_MARK_TIMESTAMP_MISSING"
        return f"{prefix}:CANONICAL_MARK_PRICE_MISSING"
    if symbol in raw_symbols:
        return f"{prefix}:RAW_AVAILABLE_BUT_NOT_CANONICAL"
    input_status = input_status_by_symbol.get(symbol)
    if input_status:
        return f"{prefix}:MARKET_DATA_INPUT_{input_status}_BUT_NOT_CANONICAL"
    if attempts:
        latest = attempts[-1]
        return f"{prefix}:PROVIDER_ATTEMPT_{str(latest.get('status') or 'UNKNOWN').upper()}"
    return f"{prefix}:FETCH_REQUIRED_NO_CANONICAL_RAW"


def _mul(left: Any, right: Any) -> float:
    return _float(left) * _float(right)


def _float(value: Any) -> float:
    try:
        if value in (None, "") or isinstance(value, bool):
            return 0.0
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


def _issue_cause(*, status: str, provider_attempts: list[dict[str, Any]], market_status: str, registry_status: str, input_status: str) -> str:
    if status == "CERTIFIED":
        return "CERTIFIED"
    latest = provider_attempts[-1] if provider_attempts else {}
    normalized = str(latest.get("normalized_status") or "").upper()
    raw = str(latest.get("status") or "").upper()
    if normalized in {"PROVIDER_TIMEOUT", "PROVIDER_NO_DATA", "CACHE_STALE", "SYMBOL_UNSUPPORTED"}:
        return normalized
    if raw in {"TIMEOUT", "NOT_ATTEMPTED_DEADLINE_EXHAUSTED"}:
        return "PROVIDER_TIMEOUT"
    if market_status == "STALE" or registry_status == "STALE" or input_status == "STALE":
        return "CACHE_STALE"
    if status == "MISSING":
        return "PROVIDER_NO_DATA"
    return "PROVIDER_NO_DATA"


def _issue_provider(provider_attempts: list[dict[str, Any]], market_row: dict[str, Any]) -> str:
    if provider_attempts:
        return str(provider_attempts[-1].get("provider") or "")
    return str(market_row.get("provider") or market_row.get("source") or "")


def _issue_groups(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        status = str(row.get("coverage_status") or "UNKNOWN")
        if status == "CERTIFIED":
            continue
        cause = str(row.get("issue_cause") or "PROVIDER_NO_DATA")
        provider = str(row.get("issue_provider") or "UNKNOWN")
        key = (status, cause, provider)
        group = grouped.setdefault(key, {"coverage_status": status, "cause": cause, "provider": provider, "symbols": [], "symbol_count": 0, "consumer_sleeves": []})
        symbol = str(row.get("symbol") or "")
        if symbol and symbol not in group["symbols"]:
            group["symbols"].append(symbol)
        for sleeve in row.get("consumer_sleeves") or []:
            sleeve = str(sleeve)
            if sleeve and sleeve not in group["consumer_sleeves"]:
                group["consumer_sleeves"].append(sleeve)
    for group in grouped.values():
        group["symbols"] = sorted(group["symbols"])
        group["symbol_count"] = len(group["symbols"])
        group["consumer_sleeves"] = sorted(group["consumer_sleeves"])
    return sorted(grouped.values(), key=lambda row: (-int(row.get("symbol_count") or 0), str(row.get("cause") or ""), str(row.get("provider") or "")))


def _provider_result(attempts: list[dict[str, Any]], market: dict[str, Any]) -> dict[str, Any]:
    if attempts:
        latest = attempts[-1]
        return {key: latest.get(key) for key in ("provider", "status", "accepted_reason", "rejected_reason", "exception_class", "source_timestamp", "raw_output_path")}
    return {"provider": market.get("source") or "", "status": "NOT_ATTEMPTED"}


def _stale_reason(*, day_utc: str, status: str, market_status: str, registry_status: str, input_status: str, session: str, attempts: list[dict[str, Any]], market_row: dict[str, Any], input_row: dict[str, Any]) -> str:
    if status == "CERTIFIED":
        return ""
    parts = [f"market_data_status={market_status}", f"data_registry_status={registry_status}", f"market_data_inputs_status={input_status}"]
    if session:
        parts.append(f"market_session_date={session}")
        if session != day_utc:
            parts.append(f"required_day={day_utc}")
    if input_row.get("reason"):
        parts.append(f"input_reason={input_row.get('reason')}")
    if market_row.get("freshness_reason"):
        parts.append(f"market_reason={market_row.get('freshness_reason')}")
    if attempts:
        latest = attempts[-1]
        if latest.get("status"):
            parts.append(f"provider_attempt_status={latest.get('status')}")
        if latest.get("rejected_reason"):
            parts.append(f"provider_rejected_reason={latest.get('rejected_reason')}")
        if latest.get("exception_class"):
            parts.append(f"provider_exception={latest.get('exception_class')}")
    else:
        parts.append("provider_attempt_status=NOT_ATTEMPTED")
    return ";".join(str(part) for part in parts if part)


def _consumer_sleeves(rows: list[dict[str, Any]]) -> list[str]:
    return sorted({str(item) for row in rows for item in row.get("consumer_sleeves", []) if str(item).startswith("C2_")})


def _blocking_consumers(rows: list[dict[str, Any]], contracts: dict[str, Any]) -> list[str]:
    blocked_symbols = {row["symbol"] for row in rows if row.get("coverage_status") != "CERTIFIED"}
    blocked: set[str] = set()
    for contract in contracts.get("contracts", []) if isinstance(contracts.get("contracts"), list) else []:
        sleeve_id = str(contract.get("sleeve_id") or "")
        for item in contract.get("required_inputs", []) if isinstance(contract.get("required_inputs"), list) else []:
            item_id = str(item.get("data_item_id") or "") if isinstance(item, dict) else ""
            symbol = normalize_market_symbol_v1(item_id.rsplit(".", 1)[-1]) if item_id.startswith("market.price.") else ("VIX" if item_id == "market.volatility.VIX" else "")
            if symbol in blocked_symbols:
                blocked.add(sleeve_id)
    return sorted(blocked)


def _source_ref(path: Path | None) -> dict[str, str | bool]:
    return {"path": str(path or ""), "found": bool(path), "hash": _sha256(path) if path else ""}


def _sha256(path: Path | None) -> str:
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest() if path else ""
    except Exception:
        return ""
