from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.portfolio.evidence_inventory import RESEARCH_LABEL, _registry, _write_report
from research_lab.portfolio.sleeve_comparison import _has_critical_missing, build_sleeve_comparison_report
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import read_json, read_jsonl
from research_lab.storage.paths import ensure_store_layout


def _reason(row: dict[str, Any]) -> str:
    if _has_critical_missing(row):
        return "Missing critical research artifacts; resolve inventory gaps before proceeding."
    if row.get("paper_trial_status") == "active" and row.get("overall_health") == "watch":
        return "Active paper trial, watch health, continue_research, needs more observations."
    if int(row.get("total_candidates") or 0) < 30 or int(row.get("measured_candidates") or 0) < 30:
        return "Evidence exists but candidate/outcome sample is still thin."
    return "Research path is available for continued observation."


def _next_action(row: dict[str, Any]) -> str:
    if row.get("paper_trial_recommended_next_action"):
        return str(row["paper_trial_recommended_next_action"])
    if row.get("priority_bucket") == "blocked":
        return "resolve_missing_artifacts"
    return str(row.get("recommended_action") or "continue_research")


def _allowed_actions(action: str) -> list[str]:
    return {
        "record_more_observations": ["record-next-paper-observation", "paper-trial-ops-report"],
        "measure_due_outcomes": ["measure-due-paper-outcomes", "paper-trial-ops-report"],
        "continue_research": ["record-next-paper-observation", "sleeve-learning-report"],
        "resolve_missing_artifacts": ["build-portfolio-evidence-reports"],
    }.get(action, [action])


def build_research_backlog_priority_report(*, store_root: Path | None = None, created_at: str | None = None) -> dict[str, Any]:
    comparison = build_sleeve_comparison_report(store_root=store_root, created_at=created_at)
    priority_rows: list[dict[str, Any]] = []
    for rank, row in enumerate(comparison["comparison_rows"], start=1):
        action = _next_action(row)
        priority_rows.append(
            {
                "rank": rank,
                "sleeve_id": row["sleeve_id"],
                "priority_bucket": row["priority_bucket"],
                "reason": _reason(row),
                "recommended_next_action": action,
                "next_allowed_actions": _allowed_actions(action),
                "blocking_items": row.get("missing_artifacts") or [],
                "priority_score": row["priority_score"],
            }
        )
    priority_rows.sort(key=lambda row: (-float(row["priority_score"]), row["sleeve_id"]))
    for index, row in enumerate(priority_rows, start=1):
        row["rank"] = index
    payload = {
        "created_at": created_at or utc_now_iso(),
        "priority_rows": priority_rows,
        "recommended_next_actions": [row["recommended_next_action"] for row in priority_rows],
        "research_label": RESEARCH_LABEL,
        "schema_version": "research_backlog_priority_report.v1",
    }
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    payload["research_backlog_priority_report_id"] = f"rbpr_{short_hash(payload['content_hash'], 12)}"
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    validate_contract("research_backlog_priority_report", payload)
    return payload


def backlog_priority_markdown(payload: dict[str, Any]) -> str:
    lines = ["# Research Backlog Priority", "", payload["research_label"], "", "## Priority Rows"]
    for row in payload["priority_rows"]:
        lines.append(f"- {row['rank']}. {row['sleeve_id']}: {row['priority_bucket']} -> {row['recommended_next_action']} ({row['reason']})")
    return "\n".join(lines) + "\n"


def write_research_backlog_priority_report(*, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    payload = build_research_backlog_priority_report(store_root=store)
    row = {
        "research_backlog_priority_report_id": payload["research_backlog_priority_report_id"],
        "top_sleeve_id": payload["priority_rows"][0]["sleeve_id"] if payload["priority_rows"] else "",
        "top_priority_bucket": payload["priority_rows"][0]["priority_bucket"] if payload["priority_rows"] else "",
        "content_hash": payload["content_hash"],
        "created_at": payload["created_at"],
        "schema_version": payload["schema_version"],
    }
    return _write_report(
        store=store,
        family="backlog_priority",
        report_id=payload["research_backlog_priority_report_id"],
        payload=payload,
        registry_name="research_backlog_priority_reports.jsonl",
        registry_row=row,
        markdown=backlog_priority_markdown(payload),
        audit_action="research_backlog_priority_report_written",
        actor=actor,
    )


def latest_research_backlog_priority_report(*, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    rows = read_jsonl(_registry(store, "research_backlog_priority_reports.jsonl"))
    if not rows:
        return {}
    latest = sorted(rows, key=lambda row: str(row.get("created_at") or ""))[-1]
    return read_json(store / "portfolio_reports" / "backlog_priority" / f"{latest['research_backlog_priority_report_id']}.json")
