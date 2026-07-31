from __future__ import annotations

import json
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .certification_runner import run_research_os_certification


def certification_report_root(root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    return Path(root) / "certification"


def build_research_os_certification_report(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Any]:
    return run_research_os_certification(root, day=day).to_dict()


def write_research_os_certification_report(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Path]:
    day_value = day or date.today().isoformat()
    report = build_research_os_certification_report(root, day=day_value)
    out_root = certification_report_root(root)
    out_dir = out_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "research_os_certification.v1.json"
    summary_path = out_dir / "research_os_certification_summary.md"
    latest_json = out_root / "latest.json"
    latest_summary = out_root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_certification_summary(report)
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    summary_path.write_text(summary, encoding="utf-8")
    latest_summary.write_text(summary, encoding="utf-8")
    return {
        "json": json_path,
        "summary": summary_path,
        "latest_json": latest_json,
        "latest_summary": latest_summary,
    }


def render_certification_summary(report: dict[str, Any]) -> str:
    counts = Counter(check["status"] for check in report.get("checks", []))
    check_lines = [
        f"- {check['check_id']}: {check['status']} - {check['summary']}"
        for check in report.get("checks", [])
    ]
    return "\n".join(
        [
            "# Atlas V2 Research OS Certification",
            "",
            f"Day: {report['day']}",
            f"Status: {report['status']}",
            f"Status counts: {json.dumps(dict(counts), sort_keys=True)}",
            "",
            "Authority boundary:",
            f"- trading certified: {report['authority_boundary']['certifies_trading']}",
            f"- capital certified: {report['authority_boundary']['certifies_capital']}",
            f"- candidate promotion certified: {report['authority_boundary']['certifies_candidate_promotion']}",
            f"- live readiness certified: {report['authority_boundary']['certifies_live_readiness']}",
            "",
            "Checks:",
            *check_lines,
            "",
            f"Blockers: {json.dumps(report.get('blockers', []), sort_keys=True)}",
            f"Warnings: {json.dumps(report.get('warnings', []), sort_keys=True)}",
            "",
        ]
    )
