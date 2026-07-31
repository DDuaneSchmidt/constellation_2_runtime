from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from .research_effectiveness import evaluate_research_effectiveness
from .research_effectiveness_governance import validate_research_effectiveness_report

REPORT_ROOT = Path("reports/atlas_v2_research_os/research_effectiveness")


def build_research_effectiveness_report(activities: list[dict[str, Any]]) -> dict[str, Any]:
    report = evaluate_research_effectiveness(activities)
    report["top_contributors"] = sorted(report["contributions"], key=lambda row: (-float(row["useful_learning_score"]), row["activity_id"]))[:5]
    report["worst_contributors"] = sorted(report["contributions"], key=lambda row: (float(row["useful_learning_score"]), row["activity_id"]))[:5]
    report["high_cost_low_value_research"] = [
        row for row in report["contributions"] if float(row["cost_estimate"]) >= 5.0 and float(row["useful_learning_score"]) < 1.0
    ]
    report["high_value_low_cost_research"] = [
        row for row in report["contributions"] if float(row["cost_estimate"]) <= 2.0 and float(row["useful_learning_score"]) >= 1.0
    ]
    report["research_roi_estimates"] = {
        ranking: payload["entries"] for ranking, payload in report["rankings"].items()
    }
    validate_research_effectiveness_report(report)
    return report


def write_research_effectiveness_report(activities: list[dict[str, Any]], *, day: str | None = None) -> dict[str, Path]:
    report = build_research_effectiveness_report(activities)
    day_value = day or date.today().isoformat()
    day_root = REPORT_ROOT / day_value
    day_root.mkdir(parents=True, exist_ok=True)
    json_path = day_root / "research_effectiveness_report.json"
    summary_path = day_root / "research_effectiveness_summary.md"
    latest_json = REPORT_ROOT / "latest.json"
    latest_summary = REPORT_ROOT / "latest_summary.md"
    json_text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary_text = _summary(report)
    json_path.write_text(json_text, encoding="utf-8")
    summary_path.write_text(summary_text, encoding="utf-8")
    latest_json.write_text(json_text, encoding="utf-8")
    latest_summary.write_text(summary_text, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def _summary(report: dict[str, Any]) -> str:
    lines = [
        "# Atlas V2 Research OS Research Effectiveness",
        "",
        f"- core_question: {report['core_question']}",
        f"- activity_count: {report['activity_count']}",
        f"- certification: {report['certification']['result']}",
        f"- top_contributors: {len(report.get('top_contributors', []))}",
        f"- worst_contributors: {len(report.get('worst_contributors', []))}",
        f"- high_cost_low_value_research: {len(report.get('high_cost_low_value_research', []))}",
        f"- high_value_low_cost_research: {len(report.get('high_value_low_cost_research', []))}",
        "- authority: research prioritization only; no trading, capital allocation, or candidate promotion authority",
        "",
    ]
    return "\n".join(lines)
