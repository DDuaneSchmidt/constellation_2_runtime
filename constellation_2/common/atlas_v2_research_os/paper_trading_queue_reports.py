from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .paper_trading_queue import list_paper_trade_test_plans, list_paper_trading_queue, list_paper_trading_readiness_reviews, list_ready_for_review, prioritize_paper_trading_queue
from .paper_trading_queue_governance import validate_paper_trading_queue_allowed

REPORT_ROOT = Path("reports/atlas_v2_research_os/paper_trading_queue")


def build_paper_trading_queue_report(root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Any]:
    queue = list_paper_trading_queue(root)
    test_plans = list_paper_trade_test_plans(root)
    reviews = list_paper_trading_readiness_reviews(root)
    state_counts: dict[str, int] = {}
    for item in queue:
        state_counts[item["state"]] = state_counts.get(item["state"], 0) + 1
    report = {
        "schema_id": "atlas_v2_research_os_paper_trading_queue_report.v1",
        "schema_version": "v1",
        "queue_item_count": len(queue),
        "test_plan_count": len(test_plans),
        "readiness_review_count": len(reviews),
        "state_counts": state_counts,
        "ready_for_review": list_ready_for_review(root),
        "prioritized_queue": prioritize_paper_trading_queue(root),
        "test_plans": test_plans,
        "reviews": reviews,
        "governance_result": "PASS",
        "limitations": [
            "Paper queue prepares candidates for human-reviewed paper testing only.",
            "Report does not authorize broker execution, live trading, capital allocation, production promotion, portfolio construction, or real-capital position sizing.",
        ],
        "metadata": {
            "measurement_only": True,
            "live_trading_authorized": False,
            "capital_authorized": False,
            "broker_execution_authorized": False,
            "production_promotion_authorized": False,
        },
    }
    validate_paper_trading_queue_allowed(report)
    return report


def write_paper_trading_queue_report(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Path]:
    report = build_paper_trading_queue_report(root)
    day_value = day or date.today().isoformat()
    day_root = REPORT_ROOT / day_value
    day_root.mkdir(parents=True, exist_ok=True)
    json_path = day_root / "paper_trading_queue_report.json"
    summary_path = day_root / "paper_trading_queue_summary.md"
    latest_json = REPORT_ROOT / "latest.json"
    latest_summary = REPORT_ROOT / "latest_summary.md"
    json_text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_paper_trading_queue_summary(report)
    json_path.write_text(json_text, encoding="utf-8")
    summary_path.write_text(summary, encoding="utf-8")
    latest_json.write_text(json_text, encoding="utf-8")
    latest_summary.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_paper_trading_queue_summary(report: dict[str, Any]) -> str:
    lines = [
        "# Atlas V2 Research OS Paper Trading Queue",
        "",
        f"- queue items: {report['queue_item_count']}",
        f"- test plans: {report['test_plan_count']}",
        f"- readiness reviews: {report['readiness_review_count']}",
        f"- governance result: {report['governance_result']}",
        "",
        "State Counts:",
    ]
    for state, count in sorted(report.get("state_counts", {}).items()):
        lines.append(f"- {state}: {count}")
    lines.append("")
    lines.append("Ready For Review:")
    for item in report.get("ready_for_review", []):
        lines.append(f"- {item['queue_item_id']} candidate={item['candidate_id']} priority={item['priority']}")
    if not report.get("ready_for_review"):
        lines.append("- none")
    lines.append("")
    lines.append("Limitations:")
    lines.extend(f"- {item}" for item in report.get("limitations", []))
    lines.append("- No broker execution, live trading, capital authorization, production promotion, or real-capital position sizing authority.")
    lines.append("")
    return "\n".join(lines)


def audit_paper_trading_queue_report(report: dict[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    try:
        validate_paper_trading_queue_allowed(report)
    except Exception as exc:
        failures.append(str(exc))
    return {"paper_trading_queue_audit_ok": not failures, "failures": failures}
