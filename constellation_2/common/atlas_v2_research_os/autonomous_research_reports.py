from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .autonomous_research_governance import ALLOWED_RECOMMENDATIONS

REPORT_ROOT = Path("reports/atlas_v2_research_os/autonomous_research")


def build_autonomous_research_execution_report(result: dict[str, Any], *, day: str | None = None) -> dict[str, Any]:
    day_value = day or _day_from_timestamp(str(result.get("started_at") or ""))
    recommendation = _recommendation_for_result(result)
    return {
        "schema_id": "atlas_v2_research_os_autonomous_research_execution_v1",
        "schema_version": "v1",
        "day": day_value,
        "execution": dict(result),
        "execution_id": result.get("execution_id", ""),
        "selected_backlog_item": result.get("selected_backlog_item_id", ""),
        "selected_worker": result.get("selected_worker_id", ""),
        "artifacts_created": list(result.get("output_artifact_ids", [])),
        "memory_updates": list(result.get("memory_updates", [])),
        "backlog_updates": list(result.get("backlog_updates", [])),
        "certification_status": (result.get("certification_result", {}) or {}).get("status", "UNKNOWN"),
        "governance_status": (result.get("governance_result", {}) or {}).get("status", "UNKNOWN"),
        "lineage_status": (result.get("lineage_result", {}) or {}).get("status", "UNKNOWN"),
        "safety_gate_result": "PASS" if all(row.get("result") == "PASS" for row in result.get("safety_gate_results", [])) else "FAIL",
        "limitations": [
            "Bounded one-shot research execution only.",
            "No scheduler, daemon, recurring loop, trading, broker, capital, sleeve, portfolio, position sizing, recommendation, or promotion authority.",
        ],
        "recommendation": recommendation,
    }


def write_autonomous_research_execution_report(result: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Path]:
    report = build_autonomous_research_execution_report(result, day=day)
    report_root = Path(root) / "autonomous_research"
    day_dir = report_root / report["day"]
    day_dir.mkdir(parents=True, exist_ok=True)
    json_path = day_dir / "autonomous_research_execution.v1.json"
    md_path = day_dir / "autonomous_research_execution_summary.md"
    latest_json = report_root / "latest.json"
    latest_md = report_root / "latest_summary.md"
    json_payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_autonomous_research_execution_summary(report)
    json_path.write_text(json_payload, encoding="utf-8")
    latest_json.write_text(json_payload, encoding="utf-8")
    md_path.write_text(summary, encoding="utf-8")
    latest_md.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": md_path, "latest_json": latest_json, "latest_summary": latest_md}


def render_autonomous_research_execution_summary(report: dict[str, Any]) -> str:
    return "\n".join([
        "# Atlas V2 Research OS Bounded Research Execution",
        "",
        f"Execution id: {report['execution_id']}",
        f"Selected backlog item: {report['selected_backlog_item']}",
        f"Selected worker: {report['selected_worker']}",
        f"Artifacts created: {json.dumps(report['artifacts_created'], sort_keys=True)}",
        f"Memory updates: {json.dumps(report['memory_updates'], sort_keys=True)}",
        f"Backlog updates: {json.dumps(report['backlog_updates'], sort_keys=True)}",
        f"Certification status: {report['certification_status']}",
        f"Governance status: {report['governance_status']}",
        f"Lineage status: {report['lineage_status']}",
        f"Safety gate result: {report['safety_gate_result']}",
        f"Limitations: {json.dumps(report['limitations'], sort_keys=True)}",
        f"Recommendation: {report['recommendation']}",
        "",
    ])


def _recommendation_for_result(result: dict[str, Any]) -> str:
    status = str(result.get("status") or "")
    if status in {"COMPLETED", "DRY_RUN_COMPLETED"}:
        recommendation = "Continue research execution."
    elif status in {"SKIPPED_NO_COMPATIBLE_WORKER", "FAILED_SAFETY_GATE"}:
        recommendation = "Review blocked backlog item."
    elif status == "FAILED":
        recommendation = "Investigate repeated failure."
    else:
        recommendation = "Run more paper-forward observations."
    if recommendation not in ALLOWED_RECOMMENDATIONS:
        return "Review blocked backlog item."
    return recommendation


def _day_from_timestamp(value: str) -> str:
    if len(value) >= 10 and value[4:5] == "-" and value[7:8] == "-":
        return value[:10]
    return date.today().isoformat()
