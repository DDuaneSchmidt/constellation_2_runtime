from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .direct_candidate_data_validation import run_direct_candidate_data_validation
from .local_market_data_import import build_market_data_import_report, write_market_data_import_report
from .market_data_import_models import AUTHORITY_BOUNDARY, CandidateMarketDataCoverage
from .market_data_readiness import run_market_data_readiness

REPORT_DIRNAME = "market_data_import"
STATUS_READY = "READY_FOR_DIRECT_REPLAY"
STATUS_PARTIAL = "PARTIAL_COVERAGE"
STATUS_MISSING_INTRADAY = "MISSING_INTRADAY_DATA"
STATUS_MISSING_SYMBOL = "MISSING_SYMBOL_DATA"
STATUS_SCHEMA_INVALID = "SCHEMA_INVALID"
STATUS_INSUFFICIENT_DATE_RANGE = "INSUFFICIENT_DATE_RANGE"


def check_candidate_data_coverage(candidate: dict[str, Any], valid_files: list[dict[str, Any]]) -> dict[str, Any]:
    required_symbols = _strings(candidate.get("candidate_symbols") or candidate.get("resolved_symbols"))
    required_timeframes = _strings(candidate.get("candidate_timeframes") or ["daily"], upper=False)
    file_index = defaultdict(list)
    for row in valid_files:
        if row.get("status") == "PASS":
            file_index[(str(row.get("symbol") or "").upper(), str(row.get("timeframe") or "").lower())].append(row)
    available_symbols: set[str] = set()
    available_timeframes: set[str] = set()
    local_files: list[str] = []
    row_count = 0
    date_starts: list[str] = []
    date_ends: list[str] = []
    missing_symbols: list[str] = []
    missing_timeframes: list[str] = []
    for symbol in required_symbols:
        symbol_files = [row for (file_symbol, _), rows in file_index.items() if file_symbol == symbol for row in rows]
        if symbol_files:
            available_symbols.add(symbol)
        else:
            missing_symbols.append(symbol)
        for timeframe in required_timeframes:
            normalized = _normalize_timeframe(timeframe)
            matching = file_index.get((symbol, normalized), [])
            if matching:
                available_timeframes.add(normalized)
                for row in matching:
                    local_files.append(str(row.get("path")))
                    row_count += int(row.get("row_count") or 0)
                    if row.get("date_start"):
                        date_starts.append(str(row["date_start"]))
                    if row.get("date_end"):
                        date_ends.append(str(row["date_end"]))
            else:
                missing_timeframes.append(f"{symbol}:{normalized}")
    blockers: list[str] = []
    if missing_symbols:
        blockers.append("Missing symbol data: " + ", ".join(sorted(set(missing_symbols))))
    if missing_timeframes:
        blockers.append("Missing timeframe data: " + ", ".join(sorted(set(missing_timeframes))))
    if missing_symbols:
        status = STATUS_MISSING_SYMBOL
    elif missing_timeframes and any(":" in item and not item.endswith(":daily") for item in missing_timeframes):
        status = STATUS_MISSING_INTRADAY
    elif missing_timeframes:
        status = STATUS_PARTIAL
    elif row_count < 30:
        status = STATUS_INSUFFICIENT_DATE_RANGE
        blockers.append("Fewer than 30 normalized rows are available.")
    else:
        status = STATUS_READY
    return CandidateMarketDataCoverage(
        candidate_id=str(candidate.get("candidate_id") or ""),
        required_symbols=required_symbols,
        required_timeframes=required_timeframes,
        available_symbols=sorted(available_symbols),
        missing_symbols=sorted(set(missing_symbols)),
        available_timeframes=sorted(available_timeframes),
        missing_timeframes=sorted(set(missing_timeframes)),
        date_start=min(date_starts) if date_starts else "",
        date_end=max(date_ends) if date_ends else "",
        row_count=row_count,
        coverage_status=status,
        local_files=sorted(set(local_files)),
        blockers=blockers,
    ).to_dict()


def build_market_data_coverage_report(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    attribution = _read_json(root_path / "candidate_symbol_attribution" / "latest.json", {})
    candidates = list(attribution.get("candidate_symbol_attributions") or [])[:8]
    import_report = build_market_data_import_report(root=root_path, created_at=created_at)
    valid_files = list(import_report.get("schema_validations") or [])
    coverage_rows = [check_candidate_data_coverage(candidate, valid_files) for candidate in candidates]
    created = created_at or _now()
    status_counts = Counter(row["coverage_status"] for row in coverage_rows)
    missing_symbols = sorted({symbol for row in coverage_rows for symbol in row.get("missing_symbols", [])})
    missing_timeframes = sorted({timeframe for row in coverage_rows for timeframe in row.get("missing_timeframes", [])})
    return {
        "schema_id": "atlas_v2_research_os_market_data_coverage",
        "schema_version": "1.0",
        "report_type": "MARKET_DATA_COVERAGE",
        "created_at": created,
        "day": created[:10],
        "summary": {
            "candidates_reviewed": len(coverage_rows),
            "ready_for_direct_replay": status_counts.get(STATUS_READY, 0),
            "blocked_candidates": len(coverage_rows) - status_counts.get(STATUS_READY, 0),
            "coverage_status_counts": dict(status_counts),
            "missing_symbols": missing_symbols,
            "missing_timeframes": missing_timeframes,
            "local_market_data_files_discovered": import_report.get("summary", {}).get("files_discovered", 0),
            "schema_valid_files": import_report.get("summary", {}).get("schema_valid_files", 0),
        },
        "candidate_coverage": coverage_rows,
        "local_market_data_import_summary": import_report.get("summary", {}),
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": [
            "Coverage analysis only.",
            "No external API calls.",
            "No broker access.",
            "No live trading.",
            "No capital allocation.",
            "No position sizing.",
            "No trade recommendations.",
            "No automatic paper trade placement.",
            "No candidate production promotion.",
        ],
    }


def write_market_data_coverage_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    day = str(report.get("day") or _today())
    out_dir = root_path / day
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "market_data_coverage_report.json"
    summary_path = out_dir / "market_data_coverage_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_market_data_coverage_summary(report)
    for path in [json_path, root_path / "latest_coverage.json"]:
        path.write_text(payload, encoding="utf-8")
    for path in [summary_path, root_path / "latest_coverage_summary.md"]:
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": root_path / "latest_coverage.json", "latest_summary": root_path / "latest_coverage_summary.md"}


def run_market_data_coverage_report(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    import_report = build_market_data_import_report(root=root, created_at=created_at)
    write_market_data_import_report(import_report, root=root)
    readiness = run_market_data_readiness(root=root, created_at=created_at)
    coverage = build_market_data_coverage_report(root=root, created_at=created_at)
    coverage["market_data_readiness_summary"] = readiness.get("summary", {})
    write_market_data_coverage_report(coverage, root=root)
    return coverage


def rerun_direct_validation_for_ready_candidates(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    return run_direct_candidate_data_validation(root=root, created_at=created_at)


def render_market_data_coverage_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Market Data Coverage",
        "",
        f"Candidates reviewed: {summary.get('candidates_reviewed')}",
        f"Ready for direct replay: {summary.get('ready_for_direct_replay')}",
        f"Blocked candidates: {summary.get('blocked_candidates')}",
        f"Missing symbols: {', '.join(summary.get('missing_symbols') or []) or 'NONE'}",
        "",
        "## Candidate Coverage",
    ]
    for row in report.get("candidate_coverage", []):
        lines.append(f"- {row['candidate_id']}: {row['coverage_status']} symbols={','.join(row['required_symbols'])} timeframes={','.join(row['required_timeframes'])}")
        if row.get("blockers"):
            lines.append(f"  blockers: {'; '.join(row['blockers'])}")
    lines.extend(["", "Authority: market-data coverage only; no live/capital/broker/position-sizing authority.", ""])
    return "\n".join(lines)


def _strings(value: Any, *, upper: bool = True) -> list[str]:
    if isinstance(value, str):
        values = [part.strip() for part in value.replace(";", ",").split(",") if part.strip()]
    elif isinstance(value, list):
        values = [str(part).strip() for part in value if str(part).strip()]
    else:
        values = []
    return sorted({item.upper() if upper else _normalize_timeframe(item) for item in values})


def _normalize_timeframe(value: str) -> str:
    value = str(value or "").strip().lower()
    if value in {"1d", "day"}:
        return "daily"
    return value or "daily"


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
