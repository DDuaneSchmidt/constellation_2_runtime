from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .learning_validation import validate_learning_over_time
from .learning_validation_governance import validate_learning_validation_allowed

REPORT_ROOT = Path("reports/atlas_v2_research_os/learning_validation")


def build_learning_validation_report(snapshots: list[dict[str, Any]], *, horizon: str = "30_day") -> dict[str, Any]:
    report = validate_learning_over_time(snapshots, horizon=horizon)
    validate_learning_validation_allowed({**report, "certification_result": report["certification"]["result"]})
    return report


def write_learning_validation_report(snapshots: list[dict[str, Any]], *, horizon: str = "30_day", day: str | None = None) -> dict[str, Path]:
    report = build_learning_validation_report(snapshots, horizon=horizon)
    day_value = day or date.today().isoformat()
    day_root = REPORT_ROOT / day_value
    day_root.mkdir(parents=True, exist_ok=True)
    json_path = day_root / "learning_validation_report.json"
    summary_path = day_root / "learning_validation_summary.md"
    latest_json = REPORT_ROOT / "latest.json"
    latest_summary = REPORT_ROOT / "latest_summary.md"
    json_text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_learning_validation_summary(report)
    json_path.write_text(json_text, encoding="utf-8")
    summary_path.write_text(summary, encoding="utf-8")
    latest_json.write_text(json_text, encoding="utf-8")
    latest_summary.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_learning_validation_summary(report: dict[str, Any]) -> str:
    lines = [
        "# Atlas V2 Research OS Learning Validation",
        "",
        f"- certification: {report['certification']['result']}",
        f"- trend direction: {report['trend_direction']}",
        f"- confidence: {report['confidence']}",
        f"- improvements: {len(report.get('improvements', []))}",
        f"- regressions: {len(report.get('regressions', []))}",
        f"- plateaus: {len(report.get('plateaus', []))}",
        "",
        "Metrics:",
    ]
    for name, value in sorted(report.get("trend", {}).get("metrics", {}).items()):
        lines.append(f"- {name}: {value}")
    lines.append("")
    lines.append("Limitations:")
    lines.extend(f"- {item}" for item in report.get("limitations", []))
    lines.append("- Measurement only; no trading, capital, candidate promotion, sleeve, portfolio, or position sizing authority.")
    lines.append("")
    return "\n".join(lines)


def audit_learning_validation_report(report: dict[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    try:
        validate_learning_validation_allowed({**report, "certification_result": report.get("certification", {}).get("result")})
    except Exception as exc:
        failures.append(str(exc))
    return {"learning_validation_audit_ok": not failures, "failures": failures}
