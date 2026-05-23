from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


INTRADAY_OPERATIONAL = "INTRADAY_OPERATIONAL"
PROVISIONAL_INTRADAY = "PROVISIONAL_INTRADAY"
FINAL_EOD_CERTIFIED = "FINAL_EOD_CERTIFIED"
FINAL_EOD = "FINAL_EOD"
DEFAULT_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")


def default_truth_root_v1() -> Path:
    return Path(os.environ.get("AEGIS_TRUTH_ROOT") or DEFAULT_TRUTH_ROOT).expanduser().resolve()


def governed_market_data_availability_v1(
    *,
    truth_root: Path | None = None,
    day_utc: str,
    required_symbols: list[str],
    final_eod_required: bool = False,
) -> dict[str, Any]:
    root = Path(truth_root or default_truth_root_v1()).expanduser().resolve()
    symbols = _canonical_symbols(required_symbols)
    report_path, report = _market_data_report(root=root, day_utc=day_utc)
    available: list[str] = []
    provisional: list[str] = []
    final: list[str] = []
    stale: list[str] = []
    unavailable: list[str] = []
    symbol_rows: dict[str, dict[str, Any]] = {}

    for symbol in symbols:
        row = _symbol_row(symbol=symbol, report=report)
        source = "market_data_report"
        if not row:
            row = _current_snapshot_row(root=root, day_utc=day_utc, symbol=symbol)
            source = "market_data_snapshot_v1"
        if row:
            row = dict(row)
            row["availability_source"] = source
            symbol_rows[symbol] = row
        state = _symbol_state(symbol=symbol, row=row, report=report, day_utc=day_utc)
        if state == "FINAL_EOD":
            final.append(symbol)
            available.append(symbol)
        elif state == "PROVISIONAL_INTRADAY":
            provisional.append(symbol)
            if not final_eod_required:
                available.append(symbol)
        elif state == "STALE":
            stale.append(symbol)
        else:
            unavailable.append(symbol)

    missing = sorted(set(symbols) - set(available) - set(stale))
    if final_eod_required:
        final_missing = sorted(set(symbols) - set(final))
        ready = bool(symbols) and not final_missing and not stale
        mode = FINAL_EOD_CERTIFIED
        operator_status = "Final EOD certification available." if ready else "Final EOD certification pending."
    else:
        final_missing = sorted(set(symbols) - set(final))
        ready = bool(symbols) and not missing and not stale
        mode = INTRADAY_OPERATIONAL
        operator_status = "Intraday market data available." if ready else "Waiting for current intraday market data."

    report_generated_at = str(report.get("generated_at_utc") or "")
    report_day = str(report.get("day_utc") or "")
    ttl_valid = bool(report) and (not report_day or report_day == day_utc) and bool(report_generated_at)
    if not report and any(symbol_rows.values()):
        ttl_valid = True
    if not ttl_valid and ready:
        ready = False
        operator_status = "Waiting for current intraday market data."

    return {
        "schema_id": "research_data_availability",
        "schema_version": "v1",
        "day_utc": day_utc,
        "truth_root": str(root),
        "required_data_mode": mode,
        "final_eod_required": bool(final_eod_required),
        "market_data_report_path": str(report_path) if report_path else "",
        "market_data_report_status": str(report.get("status") or ""),
        "market_data_mode": str(report.get("market_data_mode") or ""),
        "operator_market_data_state": str(report.get("operator_market_data_state") or ""),
        "final_eod_certification_status": str(report.get("final_eod_certification_status") or ""),
        "ttl_freshness_valid": ttl_valid,
        "required_symbols": symbols,
        "available_symbols": sorted(available),
        "provisional_intraday_symbols": sorted(provisional),
        "final_eod_symbols": sorted(final),
        "missing_symbols": missing,
        "stale_symbols": sorted(stale),
        "unavailable_symbols": sorted(set(unavailable) | set(missing)),
        "symbol_rows": symbol_rows,
        "ready": bool(ready),
        "operator_status": operator_status,
        "next_action": "Research can proceed." if ready else ("Wait for final EOD certification." if final_eod_required else "Refresh current intraday market data for missing symbols."),
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }


def market_symbols_from_required_inputs_v1(required_inputs: list[str]) -> list[str]:
    aliases = {
        "SPY_QQQ_IWM_DIA": ["SPY", "QQQ", "IWM", "DIA"],
        "SPY_QQQ_IWM": ["SPY", "QQQ", "IWM"],
        "VIX": ["VIX"],
    }
    out: list[str] = []
    for item in required_inputs:
        text = str(item or "").upper()
        for needle, symbols in aliases.items():
            if needle in text:
                out.extend(symbols)
    return _canonical_symbols(out)


def _market_data_report(*, root: Path, day_utc: str) -> tuple[Path | None, dict[str, Any]]:
    candidates = [
        root / "reports" / "aegis_market_data_v1" / day_utc / "market_data.v1.json",
        root / "reports" / "market_data_intraday_operational_v1" / day_utc / "market_data_intraday_operational.v1.json",
        root / "reports" / "market_data_final_eod_v1" / day_utc / "market_data_final_eod.v1.json",
    ]
    for path in candidates:
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if isinstance(payload, dict):
                return path, payload
    return None, {}


def _symbol_row(*, symbol: str, report: dict[str, Any]) -> dict[str, Any]:
    symbols = report.get("symbols") if isinstance(report.get("symbols"), dict) else {}
    row = symbols.get(symbol) if isinstance(symbols.get(symbol), dict) else {}
    if row:
        return row
    if symbol in _upper_list(report.get("provisional_intraday_symbols")):
        return {"canonical_symbol": symbol, "freshness_status": "CURRENT", "data_finality": PROVISIONAL_INTRADAY, "candidate_generation_eligible": True}
    if symbol in _upper_list(report.get("final_eod_symbols")):
        return {"canonical_symbol": symbol, "freshness_status": "CURRENT", "data_finality": FINAL_EOD, "candidate_generation_eligible": True}
    if symbol in _upper_list(report.get("missing_symbols")):
        return {"canonical_symbol": symbol, "freshness_status": "MISSING"}
    if symbol in _upper_list(report.get("stale_symbols")):
        return {"canonical_symbol": symbol, "freshness_status": "STALE"}
    return {}


def _current_snapshot_row(*, root: Path, day_utc: str, symbol: str) -> dict[str, Any]:
    path = root / "market_data_snapshot_v1" / symbol / f"{day_utc[:4]}.jsonl"
    if not path.exists():
        return {}
    latest: dict[str, Any] = {}
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if isinstance(row, dict) and str(row.get("symbol") or row.get("canonical_symbol") or "").upper() == symbol:
                latest = row
    except Exception:
        return {}
    timestamp = str(latest.get("timestamp_utc") or latest.get("market_session_date") or latest.get("day_utc") or "")[:10]
    if timestamp != day_utc:
        return {}
    return latest


def _symbol_state(*, symbol: str, row: dict[str, Any], report: dict[str, Any], day_utc: str) -> str:
    if not row:
        return "MISSING"
    freshness = str(row.get("freshness_status") or "").upper()
    if freshness in {"STALE", "MISSING"}:
        return freshness
    data_finality = str(row.get("data_finality") or row.get("market_data_mode") or "").upper()
    session = str(row.get("market_session_date") or row.get("timestamp_utc") or row.get("day_utc") or "")[:10]
    if session and session != day_utc:
        return "STALE"
    if data_finality in {FINAL_EOD, FINAL_EOD_CERTIFIED}:
        return "FINAL_EOD"
    if data_finality == PROVISIONAL_INTRADAY:
        return "PROVISIONAL_INTRADAY"
    if symbol in _upper_list(report.get("provisional_intraday_symbols")):
        return "PROVISIONAL_INTRADAY"
    if symbol in _upper_list(report.get("final_eod_symbols")):
        return "FINAL_EOD"
    if row.get("close") is not None or row.get("last") is not None:
        mode = str(report.get("market_data_mode") or "").upper()
        if mode == INTRADAY_OPERATIONAL or str(report.get("operator_market_data_state") or "").upper() == "INTRADAY_OPERATIONAL_READY":
            return "PROVISIONAL_INTRADAY"
    return "MISSING"


def _canonical_symbols(values: list[str]) -> list[str]:
    out: set[str] = set()
    for value in values or []:
        text = str(value or "").strip().upper()
        if text:
            out.add(text)
    return sorted(out)


def _upper_list(value: Any) -> set[str]:
    rows = value if isinstance(value, list) else []
    return {str(item or "").strip().upper() for item in rows if str(item or "").strip()}


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
