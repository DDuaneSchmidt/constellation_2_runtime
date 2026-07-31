from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .local_market_data_import import discover_local_market_data_files
from .market_data_schema_validation import validate_market_data_schema

REPORT_DIRNAME = "market_data_coverage_tracker"

AUTHORITY_BOUNDARY = {
    "coverage_measurement_only": True,
    "read_only_inputs": True,
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

STATUS_FULL = "FULL_COVERAGE"
STATUS_PARTIAL = "PARTIAL_COVERAGE"
STATUS_NONE = "NO_COVERAGE"
STATUS_NO_SYMBOLS = "NO_SYMBOLS_ATTRIBUTED"


def run_market_data_coverage_tracker(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, base_dir: str | Path | None = None) -> dict[str, Any]:
    dashboard = build_market_data_coverage_dashboard(root=root, created_at=created_at, base_dir=base_dir)
    write_market_data_coverage_dashboard(dashboard, root=root)
    return dashboard


def build_market_data_coverage_dashboard(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, base_dir: str | Path | None = None) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    candidates = _candidate_rows(root_path)
    valid_files = _valid_market_data_files(base_dir=base_dir)
    available_by_symbol = _available_by_symbol(valid_files)
    candidate_symbol_coverage = [_candidate_coverage(row, available_by_symbol) for row in candidates]

    required_symbols = sorted({symbol for row in candidate_symbol_coverage for symbol in row.get("required_symbols", [])})
    available_required_symbols = sorted(symbol for symbol in required_symbols if symbol in available_by_symbol)
    missing_symbols = sorted(symbol for symbol in required_symbols if symbol not in available_by_symbol)
    candidate_count = len(candidate_symbol_coverage)
    full_rows = [row for row in candidate_symbol_coverage if row["coverage_status"] == STATUS_FULL]
    blocked_rows = [row for row in candidate_symbol_coverage if row["coverage_status"] != STATUS_FULL]
    coverage_percent = _percent(len(available_required_symbols), len(required_symbols))
    validation_block_rate = _percent(len(blocked_rows), candidate_count)
    status_counts = Counter(row["coverage_status"] for row in candidate_symbol_coverage)

    return {
        "schema_id": "atlas_v2_research_os_market_data_coverage_tracker",
        "schema_version": "1.0",
        "report_type": "MARKET_DATA_COVERAGE_TRACKER",
        "created_at": created,
        "day": created[:10],
        "source_reports": {
            "candidate_symbol_attribution": str(root_path / "candidate_symbol_attribution" / "latest.json"),
            "direct_candidate_data_validation": str(root_path / "direct_candidate_data_validation" / "latest.json"),
            "local_market_data_roots": [str(Path(base_dir or Path.cwd()) / rel) for rel in ("data/cache", "data/historical", "data")],
        },
        "coverage_percent": coverage_percent,
        "candidate_symbol_coverage": candidate_symbol_coverage,
        "full_candidate_coverage": {
            "count": len(full_rows),
            "total_candidates": candidate_count,
            "percent": _percent(len(full_rows), candidate_count),
            "candidate_ids": [row["candidate_id"] for row in full_rows],
        },
        "validation_block_rate": validation_block_rate,
        "summary": {
            "candidates_reviewed": candidate_count,
            "candidate_symbol_count": len(required_symbols),
            "symbols_with_data": len(available_required_symbols),
            "symbols_missing_data": len(missing_symbols),
            "coverage_percent": coverage_percent,
            "full_candidate_coverage_count": len(full_rows),
            "partial_candidate_coverage_count": status_counts.get(STATUS_PARTIAL, 0),
            "no_candidate_coverage_count": status_counts.get(STATUS_NONE, 0),
            "no_symbols_attributed_count": status_counts.get(STATUS_NO_SYMBOLS, 0),
            "validation_block_rate": validation_block_rate,
            "coverage_status_counts": dict(status_counts),
            "available_required_symbols": available_required_symbols,
            "missing_symbols": missing_symbols,
            "schema_valid_market_data_files": len(valid_files),
        },
        "available_market_data": valid_files,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Coverage measurement only.",
            "Reads existing reports and local CSV metadata only.",
            "No external API calls.",
            "No replay execution.",
            "No candidate changes.",
            "No qualification changes.",
            "No governance changes.",
            "No live trading.",
            "No broker execution.",
            "No capital allocation.",
            "No position sizing.",
            "No trade recommendations.",
            "No automatic paper trade placement.",
            "No candidate production promotion.",
        ],
    }


def write_market_data_coverage_dashboard(dashboard: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    day = str(dashboard.get("day") or _today())
    out_dir = root_path / day
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "coverage_dashboard.json"
    md_path = out_dir / "coverage_dashboard.md"
    latest_json = root_path / "latest.json"
    latest_md = root_path / "latest_summary.md"
    payload = json.dumps(dashboard, indent=2, sort_keys=True) + "\n"
    summary = render_coverage_dashboard_summary(dashboard)
    for path in [json_path, latest_json]:
        path.write_text(payload, encoding="utf-8")
    for path in [md_path, latest_md]:
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": md_path, "latest_json": latest_json, "latest_summary": latest_md}


def render_coverage_dashboard_summary(dashboard: dict[str, Any]) -> str:
    summary = dashboard.get("summary", {})
    full = dashboard.get("full_candidate_coverage", {})
    lines = [
        "# Market Data Coverage Dashboard",
        "",
        f"Coverage percent: {dashboard.get('coverage_percent')}%",
        f"Candidate symbol coverage: {summary.get('symbols_with_data')} of {summary.get('candidate_symbol_count')} required symbols",
        f"Full candidate coverage: {full.get('count')} of {full.get('total_candidates')} candidates ({full.get('percent')}%)",
        f"Validation block rate: {dashboard.get('validation_block_rate')}%",
        f"Missing symbols: {', '.join(summary.get('missing_symbols') or []) or 'NONE'}",
        "",
        "## Candidate Symbol Coverage",
    ]
    for row in dashboard.get("candidate_symbol_coverage", []):
        lines.append(
            "- {candidate_id}: {status}; symbols={available}/{total}; available={available_symbols}; missing={missing_symbols}".format(
                candidate_id=row.get("candidate_id"),
                status=row.get("coverage_status"),
                available=row.get("available_symbol_count"),
                total=row.get("required_symbol_count"),
                available_symbols=",".join(row.get("available_symbols") or []) or "NONE",
                missing_symbols=",".join(row.get("missing_symbols") or []) or "NONE",
            )
        )
    lines.extend(
        [
            "",
            "## Authority Boundary",
            "",
            "Coverage measurement only. No replay execution, candidate changes, qualification changes, governance changes, live trading, broker execution, capital allocation, position sizing, trade recommendations, automatic paper trade placement, or candidate production promotion.",
            "",
        ]
    )
    return "\n".join(lines)


def _candidate_rows(root: Path) -> list[dict[str, Any]]:
    attribution = _read_json(root / "candidate_symbol_attribution" / "latest.json", {})
    rows = list(attribution.get("candidate_symbol_attributions") or attribution.get("candidate_attributions") or [])
    if rows:
        return rows
    validation = _read_json(root / "direct_candidate_data_validation" / "latest.json", {})
    return list(validation.get("candidate_validations") or [])


def _candidate_coverage(candidate: dict[str, Any], available_by_symbol: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    required_symbols = _symbol_list(
        candidate.get("candidate_symbols"),
        candidate.get("resolved_symbols"),
        candidate.get("candidate_universe_symbols"),
        candidate.get("symbol"),
        candidate.get("candidate_symbol"),
    )
    available_symbols = [symbol for symbol in required_symbols if symbol in available_by_symbol]
    missing_symbols = [symbol for symbol in required_symbols if symbol not in available_by_symbol]
    if not required_symbols:
        status = STATUS_NO_SYMBOLS
    elif not available_symbols:
        status = STATUS_NONE
    elif missing_symbols:
        status = STATUS_PARTIAL
    else:
        status = STATUS_FULL
    local_files = sorted({file["path"] for symbol in available_symbols for file in available_by_symbol.get(symbol, [])})
    available_timeframes = sorted({file["timeframe"] for symbol in available_symbols for file in available_by_symbol.get(symbol, [])})
    return {
        "candidate_id": str(candidate.get("candidate_id") or ""),
        "mechanism": str(candidate.get("mechanism") or "UNKNOWN"),
        "regime": str(candidate.get("regime") or "UNKNOWN"),
        "required_symbols": required_symbols,
        "required_symbol_count": len(required_symbols),
        "available_symbols": available_symbols,
        "available_symbol_count": len(available_symbols),
        "missing_symbols": missing_symbols,
        "missing_symbol_count": len(missing_symbols),
        "coverage_percent": _percent(len(available_symbols), len(required_symbols)),
        "coverage_status": status,
        "available_timeframes": available_timeframes,
        "local_files": local_files,
        "blocked_for_validation": status != STATUS_FULL,
    }


def _valid_market_data_files(*, base_dir: str | Path | None = None) -> list[dict[str, Any]]:
    files = discover_local_market_data_files(base_dir=base_dir)
    valid_rows: list[dict[str, Any]] = []
    for file in files:
        result = validate_market_data_schema(file)
        if result.status != "PASS":
            continue
        row = result.to_dict()
        row["path"] = file.path
        row["symbol"] = str(row.get("symbol") or file.symbol).upper()
        row["timeframe"] = str(row.get("timeframe") or file.timeframe).lower()
        valid_rows.append(row)
    return sorted(valid_rows, key=lambda item: (item.get("symbol", ""), item.get("timeframe", ""), item.get("path", "")))


def _available_by_symbol(valid_files: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in valid_files:
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        grouped.setdefault(symbol, []).append(row)
    return grouped


def _symbol_list(*values: Any) -> list[str]:
    symbols: list[str] = []
    for value in values:
        if isinstance(value, str):
            symbols.extend(part.strip().upper() for part in value.replace(";", ",").split(",") if part.strip())
        elif isinstance(value, list):
            symbols.extend(str(part).strip().upper() for part in value if str(part).strip())
    return sorted({symbol for symbol in symbols if symbol})


def _percent(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return _now()[:10]
