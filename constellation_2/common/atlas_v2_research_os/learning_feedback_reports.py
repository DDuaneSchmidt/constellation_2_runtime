from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .learning_feedback_engine import run_learning_feedback_demo
from .learning_feedback_governance import LearningFeedbackGovernanceError, validate_learning_feedback_allowed
from .learning_feedback_models import ALLOWED_LEARNING_FEEDBACK_RECOMMENDATIONS

REPORT_ROOT = Path("reports/atlas_v2_research_os/learning_feedback")


def build_learning_feedback_report(root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Any]:
    demo = run_learning_feedback_demo(root)
    blocked = [record for record in demo.get("priority_adjustments", []) if record.get("governance_status") == "BLOCKED"]
    exclusions = [
        record
        for record in blocked
        if "retired" in str(record.get("metadata", {}).get("blocked_reason", "")).lower()
        or "quarantined" in str(record.get("metadata", {}).get("blocked_reason", "")).lower()
    ]
    return {
        "feedback_run_id": demo["feedback_run_id"],
        "memory_inputs": demo["lineage_links"].get("memory_to_backlog", []),
        "candidate_quality_inputs": [demo["candidate_quality_evaluation_id"]],
        "priority_adjustments": demo["priority_adjustments"],
        "backlog_items_created": demo["backlog_items"],
        "lineage_links": demo["lineage_links"],
        "governance_result": demo["governance_result"],
        "blocked_influence_attempts": blocked,
        "retired_quarantined_exclusions": exclusions,
        "limitations": demo["limitations"],
        "recommendation": "Continue measurement.",
        "allowed_recommendations": sorted(ALLOWED_LEARNING_FEEDBACK_RECOMMENDATIONS),
    }


def audit_learning_feedback(root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Any]:
    report = build_learning_feedback_report(root)
    failures: list[str] = []
    try:
        validate_learning_feedback_allowed({"recommendation": report["recommendation"], "metadata": {"candidate_factory_modified": False}})
    except LearningFeedbackGovernanceError as exc:
        failures.append(str(exc))
    for item in report.get("backlog_items_created", []):
        if not item.get("source_memory_ids"):
            failures.append(f"backlog item missing source memory lineage: {item.get('backlog_item_id')}")
        if item.get("metadata", {}).get("does_not_imply_validation") is not True:
            failures.append(f"backlog item missing validation limitation: {item.get('backlog_item_id')}")
    return {
        "learning_feedback_audit_ok": not failures,
        "failures": failures,
        "feedback_run_id": report["feedback_run_id"],
        "governance_result": "PASS" if not failures else "FAIL",
    }


def write_learning_feedback_report(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Path]:
    report = build_learning_feedback_report(root)
    day_value = day or date.today().isoformat()
    day_root = REPORT_ROOT / day_value
    day_root.mkdir(parents=True, exist_ok=True)
    json_path = day_root / "learning_feedback_report.json"
    summary_path = day_root / "learning_feedback_summary.md"
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
        "# Atlas V2 Research OS Learning Feedback",
        "",
        f"- feedback_run_id: {report['feedback_run_id']}",
        f"- memory_inputs: {len(report.get('memory_inputs', []))}",
        f"- candidate_quality_inputs: {len(report.get('candidate_quality_inputs', []))}",
        f"- priority_adjustments: {len(report.get('priority_adjustments', []))}",
        f"- backlog_items_created: {len(report.get('backlog_items_created', []))}",
        f"- governance_result: {report.get('governance_result')}",
        f"- recommendation: {report.get('recommendation')}",
        "",
        "Limitations:",
    ]
    lines.extend(f"- {item}" for item in report.get("limitations", []))
    lines.append("")
    return "\n".join(lines)
