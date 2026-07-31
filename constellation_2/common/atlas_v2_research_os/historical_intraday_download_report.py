from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "historical_intraday_download"


def write_intraday_download_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    day = str(report.get("day") or _today())
    out_dir = root_path / day
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "historical_intraday_download_report.json"
    summary_path = out_dir / "historical_intraday_download_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_intraday_download_summary(report)
    for path in [json_path, latest_json]:
        path.write_text(payload, encoding="utf-8")
    for path in [summary_path, latest_summary]:
        path.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_intraday_download_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Historical Intraday Download",
        "",
        f"Mode: {report.get('mode')}",
        f"Provider: {report.get('provider') or 'NONE'}",
        f"Dry run: {report.get('dry_run')}",
        f"Planned priority-1 downloads: {summary.get('planned_count')}",
        f"Downloaded files: {summary.get('downloaded_count')}",
        f"Blocked count: {summary.get('blocked_count')}",
        f"Failed count: {summary.get('failed_count')}",
        "",
        "## Plan",
    ]
    for row in report.get("download_plan", []):
        lines.append(f"- {row['symbol']} {row['timeframe']} -> {row['target_csv_path']} [{row['status']}]")
        if row.get("blocker"):
            lines.append(f"  blocker: {row['blocker']}")
    if report.get("download_results"):
        lines.extend(["", "## Results"])
        for row in report["download_results"]:
            lines.append(f"- {row['symbol']} {row['timeframe']} {row['status']} rows={row.get('rows_written', 0)} target={row['target_csv_path']}")
    lines.extend(["", "Authority: historical market data only; no live/capital/broker/position-sizing/trade authority.", ""])
    return "\n".join(lines)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today() -> str:
    return _now()[:10]
