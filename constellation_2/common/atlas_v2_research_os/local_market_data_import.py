from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .market_data_import_models import AUTHORITY_BOUNDARY, LocalMarketDataFile
from .market_data_schema_validation import infer_symbol_and_timeframe_from_filename, validate_market_data_schema

REPORT_DIRNAME = "market_data_import"
DATA_ROOTS = ("data/cache", "data/historical", "data")


def discover_local_market_data_files(base_dir: str | Path | None = None) -> list[LocalMarketDataFile]:
    root = Path(base_dir or Path.cwd())
    files: dict[str, LocalMarketDataFile] = {}
    for rel in DATA_ROOTS:
        directory = root / rel
        if not directory.exists():
            continue
        for path in sorted(directory.glob("*.csv")):
            symbol, timeframe = infer_symbol_and_timeframe_from_filename(path)
            files[str(path)] = LocalMarketDataFile(path=str(path), symbol=symbol, timeframe=timeframe, filename=path.name, source_root=str(directory))
    return list(files.values())


def build_market_data_import_report(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, base_dir: str | Path | None = None) -> dict[str, Any]:
    files = discover_local_market_data_files(base_dir=base_dir)
    validations = [validate_market_data_schema(file).to_dict() for file in files]
    created = created_at or _now()
    status_counts = Counter(row["status"] for row in validations)
    symbol_counts = Counter(row["symbol"] for row in validations if row["status"] == "PASS")
    timeframe_counts = Counter(row["timeframe"] for row in validations if row["status"] == "PASS")
    return {
        "schema_id": "atlas_v2_research_os_market_data_import",
        "schema_version": "1.0",
        "report_type": "LOCAL_MARKET_DATA_IMPORT",
        "created_at": created,
        "day": created[:10],
        "source_roots": [str(Path(base_dir or Path.cwd()) / rel) for rel in DATA_ROOTS],
        "summary": {
            "files_discovered": len(files),
            "schema_valid_files": status_counts.get("PASS", 0),
            "schema_invalid_files": len(validations) - status_counts.get("PASS", 0),
            "status_counts": dict(status_counts),
            "symbols_available": sorted(symbol_counts),
            "timeframes_available": sorted(timeframe_counts),
            "priority_symbols_available": [symbol for symbol in ["QQQ", "DIA", "SPY", "TLT", "USO"] if symbol in symbol_counts],
        },
        "files": [file.to_dict() for file in files],
        "schema_validations": validations,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "guardrails": _guardrails(),
    }


def write_market_data_import_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    day = str(report.get("day") or _today())
    out_dir = root_path / day
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "market_data_import_report.json"
    summary_path = out_dir / "market_data_import_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_market_data_import_summary(report)
    for path in [json_path, latest_json]:
        path.write_text(payload, encoding="utf-8")
    for path in [summary_path, latest_summary]:
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def run_market_data_import_report(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, base_dir: str | Path | None = None) -> dict[str, Any]:
    report = build_market_data_import_report(root=root, created_at=created_at, base_dir=base_dir)
    write_market_data_import_report(report, root=root)
    return report


def validate_local_market_data(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None, base_dir: str | Path | None = None) -> dict[str, Any]:
    return run_market_data_import_report(root=root, created_at=created_at, base_dir=base_dir)


def render_market_data_import_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Local Market Data Import",
        "",
        f"Files discovered: {summary.get('files_discovered')}",
        f"Schema-valid files: {summary.get('schema_valid_files')}",
        f"Schema-invalid files: {summary.get('schema_invalid_files')}",
        f"Symbols available: {', '.join(summary.get('symbols_available') or []) or 'NONE'}",
        f"Timeframes available: {', '.join(summary.get('timeframes_available') or []) or 'NONE'}",
        "",
        "## Files",
    ]
    for row in report.get("schema_validations", []):
        lines.append(f"- {row['symbol']} {row['timeframe']} {row['status']}: {row['path']} rows={row.get('row_count')}")
        if row.get("errors"):
            lines.append(f"  errors: {'; '.join(row['errors'])}")
    lines.extend(["", "Authority: local market-data import/schema validation only; no live/capital/broker/position-sizing authority.", ""])
    return "\n".join(lines)


def _guardrails() -> list[str]:
    return [
        "Local CSV discovery and schema validation only.",
        "No external API calls.",
        "No broker access.",
        "No live trading.",
        "No capital allocation.",
        "No position sizing.",
        "No trade recommendations.",
        "No automatic paper trade placement.",
        "No candidate production promotion.",
    ]


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return _now()[:10]
