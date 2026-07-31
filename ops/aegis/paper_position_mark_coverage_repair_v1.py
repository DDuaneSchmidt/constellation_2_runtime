from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, write_json_v1
from ops.aegis.market_data.symbol_alias_registry_v1 import canonicalize_symbol_list_v1, normalize_market_symbol_v1
from ops.aegis.market_data_coverage_v1 import build_market_data_coverage_v1

REPORT_FAMILY = "aegis_paper_position_mark_coverage_repair_v1"
REPORT_FILENAME = "paper_position_mark_coverage_repair.v1.json"

SAFETY = {
    "read_only": True,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "trade_advice_allowed": False,
    "autonomous_execution_allowed": False,
    "live_trading_allowed": False,
    "real_capital_allocation_allowed": False,
    "order_management_allowed": False,
    "safety_gates_changed": False,
}


def repair_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def build_paper_position_mark_coverage_repair_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    ledger_path, ledger = latest_json_v1(root, "aegis_paper_position_ledger_v1", day, "paper_position_ledger.v1.json")
    demand_path, demand = latest_json_v1(root, "aegis_market_data_demand_v1", day, "market_data_demand.v1.json")
    market_path, market = latest_json_v1(root, "aegis_market_data_v1", day, "market_data.v1.json")
    coverage_path, stored_coverage = latest_json_v1(root, "aegis_market_data_coverage_v1", day, "market_data_coverage.v1.json")
    computed_coverage = build_market_data_coverage_v1(truth_root=root, day_utc=day)

    open_positions = [row for row in ledger.get("open_positions", []) if isinstance(row, dict)] if isinstance(ledger, dict) else []
    open_symbols = canonicalize_symbol_list_v1(row.get("symbol") for row in open_positions)
    requested_symbols = canonicalize_symbol_list_v1(demand.get("requested_symbols") if isinstance(demand.get("requested_symbols"), list) else [])
    returned_symbols = canonicalize_symbol_list_v1((market.get("symbols") or {}).keys() if isinstance(market.get("symbols"), dict) else [])
    position_rows = computed_coverage.get("position_mark_rows") if isinstance(computed_coverage.get("position_mark_rows"), list) else []
    returned_not_certified = sorted({str(row.get("symbol") or "") for row in position_rows if row.get("mark_status") not in {"MARK_AVAILABLE", "SYMBOL_NOT_IN_MARKET_DATA", "SYMBOL_NOT_REQUESTED"} and str(row.get("symbol") or "")})
    missing_from_request = sorted(set(open_symbols) - set(requested_symbols))
    requested_not_returned = sorted(set(requested_symbols) - set(returned_symbols))
    missing_mark_symbols = computed_coverage.get("missing_mark_symbols") if isinstance(computed_coverage.get("missing_mark_symbols"), list) else []
    blockers = computed_coverage.get("remaining_blocker_reason_codes") if isinstance(computed_coverage.get("remaining_blocker_reason_codes"), list) else []

    stored_open = stored_coverage.get("open_position_count") if isinstance(stored_coverage, dict) else None
    computed_open = computed_coverage.get("open_position_count")
    stored_false_green = bool(open_positions and (stored_open in {0, None}) and float(stored_coverage.get("mark_coverage_by_position_pct") or 0) == 100.0) if isinstance(stored_coverage, dict) else False

    payload = {
        "schema_id": "aegis_paper_position_mark_coverage_repair",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day,
        "generated_at_utc": now_utc_v1(),
        "status": "BLOCKED" if blockers or missing_mark_symbols or missing_from_request or requested_not_returned else "READY",
        "repair_summary": {
            "authoritative_position_universe": "aegis_paper_position_ledger_v1.open_positions[].symbol",
            "stored_market_data_coverage_false_green": stored_false_green,
            "coverage_recomputed_from_current_ledger": True,
        },
        "investigation": {
            "open_position_count": len(open_positions),
            "unique_open_position_symbols": open_symbols,
            "unique_open_position_symbol_count": len(open_symbols),
            "requested_market_data_symbols": requested_symbols,
            "requested_market_data_symbol_count": len(requested_symbols),
            "symbols_missing_from_request": missing_from_request,
            "symbols_missing_from_request_count": len(missing_from_request),
            "symbols_requested_but_not_returned": requested_not_returned,
            "symbols_requested_but_not_returned_count": len(requested_not_returned),
            "symbols_returned_but_not_certified": returned_not_certified,
            "symbols_returned_but_not_certified_count": len(returned_not_certified),
            "final_marked_count": int(computed_coverage.get("marked_position_count") or 0),
            "final_unmarked_count": int(computed_coverage.get("missing_mark_position_count") or 0),
            "final_coverage_percentage": computed_coverage.get("mark_coverage_by_position_pct"),
            "remaining_blocker_reason_codes": blockers,
            "missing_reason_by_symbol": computed_coverage.get("missing_reason_by_symbol") or {},
        },
        "source_artifacts": {
            "paper_position_ledger": _source_ref(ledger_path),
            "market_data_demand": _source_ref(demand_path),
            "market_data": _source_ref(market_path),
            "market_data_coverage_stored": _source_ref(coverage_path),
        },
        "computed_market_data_coverage": {
            "status": computed_coverage.get("status"),
            "coverage_status": computed_coverage.get("coverage_status"),
            "open_position_count": computed_open,
            "marked_position_count": computed_coverage.get("marked_position_count"),
            "missing_mark_position_count": computed_coverage.get("missing_mark_position_count"),
            "mark_coverage_by_position_pct": computed_coverage.get("mark_coverage_by_position_pct"),
        },
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = hashlib.sha256(json.dumps({**payload, "content_hash": ""}, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")).hexdigest()
    return payload


def write_paper_position_mark_coverage_repair_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    body = payload or build_paper_position_mark_coverage_repair_v1(truth_root=truth_root, day_utc=day_utc)
    return write_json_v1(repair_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def _source_ref(path: Path | None) -> dict[str, Any]:
    return {"path": str(path or ""), "found": bool(path), "hash": _sha(path)}


def _sha(path: Path | None) -> str:
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest() if path else ""
    except OSError:
        return ""
