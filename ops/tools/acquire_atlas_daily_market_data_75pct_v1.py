#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import json
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.direct_candidate_data_validation import run_direct_candidate_data_validation
from constellation_2.common.atlas_v2_research_os.market_data_coverage_tracker import run_market_data_coverage_tracker
from constellation_2.common.atlas_v2_research_os.market_data_schema_validation import validate_market_data_schema

SYMBOLS = ["DIA", "QQQ", "BAC", "META", "MSFT", "TSLA", "AMZN", "NFLX"]
START_DATE = "2021-06-05"
END_DATE = "2026-06-05"
REPORT_ROOT = REPO_ROOT / "reports" / "atlas_v2_research_os"
OUT_DIR = REPORT_ROOT / "market_data_acquisition"
AUTHORITY_BOUNDARY = {
    "market_data_acquisition_only": True,
    "trade_recommendation_authorized": False,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "portfolio_construction_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_production_promotion_authorized": False,
    "replay_override_authorized": False,
    "qualification_override_authorized": False,
    "governance_override_authorized": False,
}


@dataclass(frozen=True)
class FetchResult:
    symbol: str
    status: str
    rows: list[dict[str, Any]]
    provider: str
    error: str = ""
    diagnostic: dict[str, Any] | None = None


def main() -> int:
    before_coverage = _read_json(REPORT_ROOT / "market_data_coverage_tracker" / "latest.json", {})
    before_validation = _read_json(REPORT_ROOT / "direct_candidate_data_validation" / "latest.json", {})
    if not before_coverage:
        before_coverage = run_market_data_coverage_tracker(REPORT_ROOT, base_dir=REPO_ROOT)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    acquisition_rows: list[dict[str, Any]] = []
    validation_rows: list[dict[str, Any]] = []

    for symbol in SYMBOLS:
        destination = REPO_ROOT / "data" / "cache" / f"{symbol}_tiingo_adjusted_daily.csv"
        result = reuse_existing_file(symbol, destination) or fetch_yahoo_chart(symbol)
        if result.status == "SUCCESS":
            destination.parent.mkdir(parents=True, exist_ok=True)
            write_daily_csv(destination, result.rows)
            metadata = write_metadata(destination, result)
            schema = validate_market_data_schema(destination).to_dict()
            validation = validate_acquired_file(destination, schema)
            validation["metadata_path"] = str(metadata.relative_to(REPO_ROOT))
        else:
            validation = {
                "symbol": symbol,
                "status": "FETCH_FAILED",
                "path": str(destination.relative_to(REPO_ROOT)),
                "errors": [result.error],
                "warnings": [],
                "row_count": 0,
                "required_columns_present": False,
                "adjusted_close_present": False,
                "duplicate_dates": [],
                "date_coverage_ok": False,
            }
        acquisition_rows.append(
            {
                "symbol": symbol,
                "provider": result.provider,
                "status": result.status,
                "rows_fetched": len(result.rows),
                "path": str(destination.relative_to(REPO_ROOT)),
                "error": result.error,
                "diagnostic": result.diagnostic or {},
            }
        )
        validation_rows.append(validation)
        time.sleep(0.5)

    after_coverage = run_market_data_coverage_tracker(REPORT_ROOT, base_dir=REPO_ROOT)
    after_validation = run_direct_candidate_data_validation(REPORT_ROOT)
    report = build_validation_report(acquisition_rows, validation_rows, before_coverage, after_coverage, before_validation, after_validation)
    write_reports(report, before_coverage, after_coverage, before_validation, after_validation)
    success_count = sum(1 for row in acquisition_rows if row["status"] == "SUCCESS")
    print(json.dumps({"symbols_requested": len(SYMBOLS), "symbols_acquired": success_count, "report": str(OUT_DIR / "market_data_validation_001.json")}, sort_keys=True))
    return 0 if success_count == len(SYMBOLS) else 2


def fetch_yahoo_chart(symbol: str) -> FetchResult:
    start = int(datetime.fromisoformat(START_DATE).replace(tzinfo=timezone.utc).timestamp())
    end = int(datetime.fromisoformat(END_DATE).replace(tzinfo=timezone.utc).timestamp()) + 86400
    query = urlencode(
        {
            "period1": start,
            "period2": end,
            "interval": "1d",
            "events": "history",
            "includeAdjustedClose": "true",
        }
    )
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?{query}"
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; AtlasResearchOS/1.0; research-data-validation)",
            "Accept": "application/json",
        },
    )
    try:
        with urlopen(request, timeout=45) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        return FetchResult(symbol=symbol, status="FETCH_FAILED", rows=[], provider="yahoo_chart", error=f"Yahoo chart HTTP {exc.code}", diagnostic={"http_status": exc.code})
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        return FetchResult(symbol=symbol, status="FETCH_FAILED", rows=[], provider="yahoo_chart", error=f"Yahoo chart fetch failed: {exc}", diagnostic={"exception_type": type(exc).__name__})

    try:
        result = payload["chart"]["result"][0]
        timestamps = result["timestamp"]
        quote = result["indicators"]["quote"][0]
        adj = result["indicators"].get("adjclose", [{}])[0].get("adjclose", [])
    except (KeyError, IndexError, TypeError) as exc:
        return FetchResult(symbol=symbol, status="FETCH_FAILED", rows=[], provider="yahoo_chart", error="Yahoo chart returned unexpected schema", diagnostic={"exception_type": type(exc).__name__, "payload_preview": str(payload)[:500]})

    rows: list[dict[str, Any]] = []
    for index, ts in enumerate(timestamps):
        date_text = datetime.fromtimestamp(int(ts), tz=UTC).date().isoformat()
        if date_text < START_DATE or date_text > END_DATE:
            continue
        try:
            row = {
                "date": date_text,
                "open": quote["open"][index],
                "high": quote["high"][index],
                "low": quote["low"][index],
                "close": quote["close"][index],
                "adjClose": adj[index] if index < len(adj) else quote["close"][index],
                "volume": quote["volume"][index],
            }
        except (KeyError, IndexError):
            continue
        if any(row[key] is None for key in ["open", "high", "low", "close", "adjClose"]):
            continue
        rows.append(row)
    if not rows:
        return FetchResult(symbol=symbol, status="FETCH_FAILED", rows=[], provider="yahoo_chart", error="Yahoo chart returned no usable rows in requested range", diagnostic={"response_symbol": symbol})
    return FetchResult(symbol=symbol, status="SUCCESS", rows=rows, provider="yahoo_chart")


def reuse_existing_file(symbol: str, path: Path) -> FetchResult | None:
    if not path.exists():
        return None
    schema = validate_market_data_schema(path).to_dict()
    validation = validate_acquired_file(path, schema)
    if validation.get("status") != "PASS":
        return None
    rows: list[dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append({key: row.get(key) for key in ["date", "open", "high", "low", "close", "adjClose", "volume"]})
    if not rows:
        return None
    return FetchResult(symbol=symbol, status="SUCCESS", rows=rows, provider="local_existing")


def write_daily_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = ["date", "open", "high", "low", "close", "adjClose", "volume"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fields})


def write_metadata(path: Path, result: FetchResult) -> Path:
    metadata_path = path.with_suffix(".metadata.json")
    payload = {
        "symbol": result.symbol,
        "provider": result.provider,
        "dataset_type": "daily_adjusted_ohlcv",
        "frequency": "1d",
        "requested_start_date": START_DATE,
        "requested_end_date": END_DATE,
        "first_date": result.rows[0]["date"] if result.rows else "",
        "last_date": result.rows[-1]["date"] if result.rows else "",
        "row_count": len(result.rows),
        "file_path": str(path.relative_to(REPO_ROOT)),
        "sha256": _sha256(path),
        "adjustment_mode": "provider_adjusted_close",
        "known_limitations": [
            "Research data only.",
            "No live trading, capital allocation, position sizing, broker execution, replay override, qualification override, governance override, or candidate promotion authority.",
        ],
    }
    metadata_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return metadata_path


def validate_acquired_file(path: Path, schema: dict[str, Any]) -> dict[str, Any]:
    errors = list(schema.get("errors") or [])
    warnings = list(schema.get("warnings") or [])
    required = {"date", "open", "high", "low", "close", "adjClose", "volume"}
    duplicate_dates: list[str] = []
    columns: set[str] = set()
    dates: list[str] = []
    adjusted_close_present = False
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        columns = set(reader.fieldnames or [])
        adjusted_close_present = "adjClose" in columns
        seen: set[str] = set()
        for row in reader:
            date_text = str(row.get("date") or "")[:10]
            if not date_text:
                continue
            if date_text in seen:
                duplicate_dates.append(date_text)
            seen.add(date_text)
            dates.append(date_text)
            try:
                open_px = float(row["open"])
                high_px = float(row["high"])
                low_px = float(row["low"])
                close_px = float(row["close"])
                adj_close = float(row["adjClose"])
                volume = float(row["volume"])
            except (KeyError, TypeError, ValueError):
                errors.append(f"Invalid numeric row for {date_text}")
                continue
            if high_px < max(open_px, low_px, close_px):
                errors.append(f"OHLC invariant failed for {date_text}: high")
            if low_px > min(open_px, high_px, close_px):
                errors.append(f"OHLC invariant failed for {date_text}: low")
            if min(open_px, high_px, low_px, close_px, adj_close) <= 0:
                errors.append(f"Nonpositive price for {date_text}")
            if volume < 0:
                errors.append(f"Negative volume for {date_text}")
    required_columns_present = required.issubset(columns)
    if not required_columns_present:
        errors.append("Missing required columns: " + ", ".join(sorted(required - columns)))
    if not dates:
        errors.append("No nonempty rows.")
    # START_DATE is 2021-06-05, a non-trading Saturday; the first valid trading row is 2021-06-07.
    date_coverage_ok = bool(dates) and min(dates) <= "2021-06-07" and max(dates) >= "2026-06-01"
    if not date_coverage_ok:
        warnings.append("Date coverage does not span requested range through latest expected market date.")
    status = "PASS" if not errors and required_columns_present and adjusted_close_present and dates and not duplicate_dates else "FAIL"
    return {
        "symbol": path.name.split("_", 1)[0],
        "path": str(path.relative_to(REPO_ROOT)),
        "status": status,
        "schema_status": schema.get("status"),
        "row_count": len(dates),
        "first_date": min(dates) if dates else "",
        "last_date": max(dates) if dates else "",
        "required_columns_present": required_columns_present,
        "adjusted_close_present": adjusted_close_present,
        "duplicate_dates": sorted(set(duplicate_dates)),
        "date_coverage_ok": date_coverage_ok,
        "errors": sorted(set(errors)),
        "warnings": sorted(set(warnings)),
    }


def build_validation_report(
    acquisition_rows: list[dict[str, Any]],
    validation_rows: list[dict[str, Any]],
    before_coverage: dict[str, Any],
    after_coverage: dict[str, Any],
    before_validation: dict[str, Any],
    after_validation: dict[str, Any],
) -> dict[str, Any]:
    passed = [row for row in validation_rows if row.get("status") == "PASS"]
    return {
        "schema_id": "atlas_v2_research_os_market_data_acquisition_001",
        "schema_version": "1.0",
        "created_at": _now(),
        "symbols_requested": SYMBOLS,
        "symbols_acquired": [row["symbol"] for row in acquisition_rows if row["status"] == "SUCCESS"],
        "symbols_validated": [row["symbol"] for row in passed],
        "requested_history": {"start": START_DATE, "end": END_DATE},
        "required_fields": ["date", "open", "high", "low", "close", "adjClose", "volume"],
        "acquisition": acquisition_rows,
        "validations": validation_rows,
        "coverage_delta": coverage_delta(before_coverage, after_coverage),
        "validation_delta": validation_delta(before_validation, after_validation),
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
    }


def coverage_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    before_summary = before.get("summary", {})
    after_summary = after.get("summary", {})
    before_pair = candidate_symbol_pair_coverage(before)
    after_pair = candidate_symbol_pair_coverage(after)
    return {
        "before_coverage_percent": before.get("coverage_percent", before_summary.get("coverage_percent")),
        "after_coverage_percent": after.get("coverage_percent", after_summary.get("coverage_percent")),
        "before_candidate_symbol_pair_coverage_percent": before_pair["coverage_percent"],
        "after_candidate_symbol_pair_coverage_percent": after_pair["coverage_percent"],
        "before_candidate_symbol_pairs_covered": before_pair["covered_pairs"],
        "after_candidate_symbol_pairs_covered": after_pair["covered_pairs"],
        "candidate_symbol_pair_total": after_pair["total_pairs"] or before_pair["total_pairs"],
        "before_symbols_with_data": before_summary.get("symbols_with_data"),
        "after_symbols_with_data": after_summary.get("symbols_with_data"),
        "before_symbols_missing_data": before_summary.get("symbols_missing_data"),
        "after_symbols_missing_data": after_summary.get("symbols_missing_data"),
        "before_validation_block_rate": before.get("validation_block_rate", before_summary.get("validation_block_rate")),
        "after_validation_block_rate": after.get("validation_block_rate", after_summary.get("validation_block_rate")),
        "available_required_symbols": after_summary.get("available_required_symbols", []),
        "missing_symbols": after_summary.get("missing_symbols", []),
    }


def candidate_symbol_pair_coverage(dashboard: dict[str, Any]) -> dict[str, Any]:
    rows = dashboard.get("candidate_symbol_coverage") or []
    total = 0
    covered = 0
    for row in rows:
        total += int(row.get("required_symbol_count") or 0)
        covered += int(row.get("available_symbol_count") or 0)
    return {
        "covered_pairs": covered,
        "total_pairs": total,
        "coverage_percent": round((covered / total) * 100.0, 2) if total else 0.0,
    }


def validation_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    before_summary = before.get("summary", {})
    after_summary = after.get("summary", {})
    return {
        "before_classification_counts": before_summary.get("classification_counts", {}),
        "after_classification_counts": after_summary.get("classification_counts", {}),
        "before_insufficient_data": before_summary.get("insufficient_data"),
        "after_insufficient_data": after_summary.get("insufficient_data"),
        "before_direct_data_exists_count": before_summary.get("direct_data_exists_count"),
        "after_direct_data_exists_count": after_summary.get("direct_data_exists_count"),
        "before_direct_replays_run": before_summary.get("direct_replays_run"),
        "after_direct_replays_run": after_summary.get("direct_replays_run"),
    }


def write_reports(
    report: dict[str, Any],
    before_coverage: dict[str, Any],
    after_coverage: dict[str, Any],
    before_validation: dict[str, Any],
    after_validation: dict[str, Any],
) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "market_data_validation_001.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (OUT_DIR / "market_data_acquisition_001.md").write_text(render_acquisition_summary(report), encoding="utf-8")
    (OUT_DIR / "coverage_delta_report_001.md").write_text(render_coverage_delta(report["coverage_delta"]), encoding="utf-8")
    (OUT_DIR / "validation_delta_report_001.md").write_text(render_validation_delta(report["validation_delta"]), encoding="utf-8")


def render_acquisition_summary(report: dict[str, Any]) -> str:
    lines = [
        "# Market Data Acquisition 001",
        "",
        f"Created: {report['created_at']}",
        "",
        "Scope: daily adjusted OHLCV acquisition for Atlas direct validation coverage. No candidate, replay, qualification, governance, production, capital, trading, broker, sizing, paper-placement, or promotion authority.",
        "",
        f"Symbols requested: {', '.join(report['symbols_requested'])}",
        f"Symbols acquired: {', '.join(report['symbols_acquired']) or 'NONE'}",
        f"Symbols validated: {', '.join(report['symbols_validated']) or 'NONE'}",
        "",
        "## Acquisition Results",
    ]
    for row in report["acquisition"]:
        lines.append(f"- {row['symbol']}: {row['status']} rows={row['rows_fetched']} path={row['path']}")
        if row.get("error"):
            lines.append(f"  error: {row['error']}")
    lines.extend(["", "## Authority Boundary", "", _authority_text(), ""])
    return "\n".join(lines)


def render_coverage_delta(delta: dict[str, Any]) -> str:
    lines = [
        "# Coverage Delta Report 001",
        "",
        f"Coverage percent: {delta.get('before_coverage_percent')} -> {delta.get('after_coverage_percent')}",
        f"Candidate-symbol pair coverage: {delta.get('before_candidate_symbol_pair_coverage_percent')} -> {delta.get('after_candidate_symbol_pair_coverage_percent')} ({delta.get('after_candidate_symbol_pairs_covered')}/{delta.get('candidate_symbol_pair_total')})",
        f"Symbols with data: {delta.get('before_symbols_with_data')} -> {delta.get('after_symbols_with_data')}",
        f"Symbols missing data: {delta.get('before_symbols_missing_data')} -> {delta.get('after_symbols_missing_data')}",
        f"Validation block rate: {delta.get('before_validation_block_rate')} -> {delta.get('after_validation_block_rate')}",
        f"Available required symbols: {', '.join(delta.get('available_required_symbols') or []) or 'NONE'}",
        f"Missing symbols: {', '.join(delta.get('missing_symbols') or []) or 'NONE'}",
        "",
        "Authority: " + _authority_text(),
        "",
    ]
    return "\n".join(lines)


def render_validation_delta(delta: dict[str, Any]) -> str:
    lines = [
        "# Validation Delta Report 001",
        "",
        f"Classification counts: {delta.get('before_classification_counts')} -> {delta.get('after_classification_counts')}",
        f"Insufficient data: {delta.get('before_insufficient_data')} -> {delta.get('after_insufficient_data')}",
        f"Direct data exists count: {delta.get('before_direct_data_exists_count')} -> {delta.get('after_direct_data_exists_count')}",
        f"Direct replays run: {delta.get('before_direct_replays_run')} -> {delta.get('after_direct_replays_run')}",
        "",
        "Authority: " + _authority_text(),
        "",
    ]
    return "\n".join(lines)


def _authority_text() -> str:
    return "market-data acquisition/reporting only; no live trading, broker execution, trade recommendations, capital authority, position sizing, candidate promotion, replay override, qualification override, or governance override."


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
