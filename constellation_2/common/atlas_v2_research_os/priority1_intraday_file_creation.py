from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .direct_candidate_data_validation import run_direct_candidate_data_validation
from .historical_intraday_download_models import AUTHORITY_BOUNDARY
from .manual_intraday_csv_intake import (
    PRIORITY_1_OUTPUTS,
    discover_manual_intraday_csvs,
    run_manual_intraday_csv_intake,
)
from .safe_historical_intraday_downloader import dry_run_intraday_download
from .local_market_data_import import discover_local_market_data_files, run_market_data_import_report
from .market_data_coverage_report import run_market_data_coverage_report

REPORT_DIRNAME = "priority1_intraday_file_creation"
REQUIRED_OUTPUTS = [
    "data/cache/DIA_30m.csv",
    "data/cache/DIA_5m.csv",
    "data/cache/QQQ_30m.csv",
    "data/cache/QQQ_5m.csv",
    "data/cache/SPY_30m.csv",
    "data/cache/SPY_5m.csv",
]


def run_priority1_intraday_file_creation(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    created = created_at or _now()
    source_files = discover_source_files()
    source_candidates = [row for row in source_files if row.get("source_usable_for_priority1")]
    manual_report = run_manual_intraday_csv_intake(root=root, created_at=created)
    import_report = run_market_data_import_report(root=root, created_at=created)
    coverage_report = run_market_data_coverage_report(root=root, created_at=created)
    direct_report = run_direct_candidate_data_validation(root=root, created_at=created)
    provider_dry_run = {}
    if not source_candidates:
        provider_dry_run = dry_run_intraday_download(root=root, created_at=created)
    existing_outputs = [path for path in REQUIRED_OUTPUTS if Path(path).exists()]
    remaining_missing = [path for path in REQUIRED_OUTPUTS if not Path(path).exists()]
    report = {
        "schema_id": "atlas_v2_research_os_priority1_intraday_file_creation",
        "schema_version": "1.0",
        "report_type": "PRIORITY1_INTRADAY_FILE_CREATION",
        "created_at": created,
        "day": created[:10],
        "source_priority": [
            "user/vendor files in data/manual_intraday_import",
            "repo-local historical files",
            "configured historical provider dry-run only",
            "blocked report if no legitimate sources exist",
        ],
        "source_files_discovered": source_files,
        "files_normalized": manual_report.get("normalized_files_written", []),
        "files_resampled": manual_report.get("resampled_files", []),
        "files_created": existing_outputs,
        "files_rejected": [row for row in manual_report.get("schema_validations", []) if row.get("status") != "PASS"],
        "schema_warnings": sorted({warning for row in manual_report.get("schema_validations", []) for warning in row.get("warnings", [])}),
        "remaining_missing_files": remaining_missing,
        "provider_dry_run": {
            "ran": bool(provider_dry_run),
            "summary": provider_dry_run.get("summary", {}),
            "report": str(Path(root) / "historical_intraday_download" / "latest.json") if provider_dry_run else "",
        },
        "post_creation_validation": {
            "manual_intraday_csv_intake": manual_report.get("summary", {}),
            "market_data_import": import_report.get("summary", {}),
            "market_data_coverage": coverage_report.get("summary", {}),
            "direct_candidate_data_validation": direct_report.get("summary", {}),
        },
        "candidate_1_validation_result": _candidate_result(direct_report, "ptc_backtest_final_469607b8340421b7"),
        "candidate_2_validation_result": _candidate_result(direct_report, "ptc_backtest_final_3a4ac24107c77136"),
        "status": "BLOCKED_NO_LEGITIMATE_INTRADAY_SOURCE_FILES" if remaining_missing else "PRIORITY1_FILES_READY",
        "authority_boundary": dict(AUTHORITY_BOUNDARY | {"external_api_called": False}),
        "guardrails": [
            "No fabricated market data.",
            "No placeholder price files.",
            "No mock or random values.",
            "No broker APIs.",
            "Provider execution is not allowed in this build.",
            "No live trading.",
            "No capital allocation.",
            "No position sizing.",
            "No trade recommendations.",
            "No automatic paper placement.",
            "No candidate production promotion.",
        ],
    }
    write_priority1_intraday_file_creation_report(report, root=root)
    return report


def discover_source_files(base_dir: str | Path | None = None) -> list[dict[str, Any]]:
    root = Path(base_dir or Path.cwd())
    rows: list[dict[str, Any]] = []
    manual_rows = discover_manual_intraday_csvs(base_dir=root)
    for row in manual_rows:
        rows.append({**row, "source_type": "manual_intraday_import", "source_usable_for_priority1": row["symbol"] in {"DIA", "QQQ", "SPY"} and row["timeframe"] in {"1m", "5m", "30m"}})
    for file in discover_local_market_data_files(base_dir=root):
        key = (file.symbol, file.timeframe)
        rows.append(
            {
                "path": file.path,
                "filename": file.filename,
                "symbol": file.symbol,
                "timeframe": file.timeframe,
                "source_type": "repo_local_market_data",
                "source_usable_for_priority1": key in PRIORITY_1_OUTPUTS or (file.symbol in {"DIA", "QQQ", "SPY"} and file.timeframe == "1m"),
            }
        )
    rows.sort(key=lambda row: (row["source_type"], row["symbol"], row["timeframe"], row["path"]))
    return rows


def write_priority1_intraday_file_creation_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    out_dir = root_path / str(report.get("day") or _today())
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "priority1_intraday_file_creation_report.json"
    summary_path = out_dir / "priority1_intraday_file_creation_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_priority1_intraday_file_creation_summary(report)
    for path in [json_path, latest_json]:
        path.write_text(payload, encoding="utf-8")
    for path in [summary_path, latest_summary]:
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_priority1_intraday_file_creation_summary(report: dict[str, Any]) -> str:
    lines = [
        "# Priority-1 Intraday File Creation",
        "",
        f"Status: {report.get('status')}",
        f"Source files discovered: {len(report.get('source_files_discovered', []))}",
        f"Files normalized: {len(report.get('files_normalized', []))}",
        f"Files resampled: {len(report.get('files_resampled', []))}",
        f"Files created: {len(report.get('files_created', []))}",
        f"Remaining missing files: {len(report.get('remaining_missing_files', []))}",
        "",
        "## Remaining Missing Files",
    ]
    for path in report.get("remaining_missing_files", []):
        lines.append(f"- `{path}`")
    lines.extend(
        [
            "",
            f"Candidate 1: {report.get('candidate_1_validation_result', {}).get('classification')}",
            f"Candidate 2: {report.get('candidate_2_validation_result', {}).get('classification')}",
            "",
            "Authority: historical data file creation support only; no fabricated data and no live/capital/broker/position-sizing authority.",
            "",
        ]
    )
    return "\n".join(lines)


def _candidate_result(report: dict[str, Any], candidate_id: str) -> dict[str, Any]:
    return next((row for row in report.get("candidate_validations", []) if row.get("candidate_id") == candidate_id), {"candidate_id": candidate_id, "classification": "UNKNOWN"})


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return _now()[:10]
