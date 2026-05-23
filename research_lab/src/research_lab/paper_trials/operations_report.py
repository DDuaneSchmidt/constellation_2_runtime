from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import validate_contract
from research_lab.paper_trials.due_outcomes import list_paper_due_outcomes
from research_lab.paper_trials.paper_trial_registry import build_paper_trial_summary, paper_trial_dir
from research_lab.storage.hashing import content_hash, utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


RESEARCH_LABEL = "Forward observational paper trial; no broker execution; no live trading; not achieved portfolio performance."


def validate_paper_trial_operations_report(report: dict[str, Any]) -> None:
    validate_contract("paper_trial_operations_report", report)


def _registry(store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "registries" / "paper_trial_operations_reports.jsonl"


def build_paper_trial_operations_report(
    paper_trial_id: str,
    *,
    as_of_date: str,
    store_root: Path | None = None,
) -> dict[str, Any]:
    summary = build_paper_trial_summary(paper_trial_id, store_root=store_root)
    due = list_paper_due_outcomes(paper_trial_id, as_of_date=as_of_date, store_root=store_root)
    latest_measurement_dates = [
        str(row.get("created_at") or "")
        for row in due["due_outcomes"]
        if row.get("status") in {"measured", "unavailable"} and row.get("measured_outcome_id")
    ]
    if due["due_count"] > 0:
        recommended = "measure_due_outcomes"
    elif int(summary["observation_count"]) < 10:
        recommended = "record_more_observations"
    elif int(summary["measured_outcome_count"]) < 30:
        recommended = "continue_paper_trial"
    else:
        recommended = "review_paper_trial"
    next_allowed = {
        "measure_due_outcomes": ["measure-due-paper-outcomes", "paper-trial-ops-report"],
        "record_more_observations": ["record-next-paper-observation", "list-paper-due-outcomes"],
        "continue_paper_trial": ["record-next-paper-observation", "review-paper-trial"],
        "review_paper_trial": ["review-paper-trial"],
    }[recommended]
    report = {
        "paper_trial_id": paper_trial_id,
        "status": summary["status"],
        "observation_count": summary["observation_count"],
        "candidate_count_total": summary["candidate_count_total"],
        "zero_candidate_observation_count": summary["zero_candidate_observation_count"],
        "pending_due_outcomes": due["pending_count"],
        "due_outcomes": due["due_count"],
        "measured_due_outcomes": due["measured_count"],
        "unavailable_due_outcomes": due["unavailable_count"],
        "measured_candidate_count": summary["measured_outcome_count"],
        "latest_observation_date": summary["latest_observation_date"],
        "latest_outcome_measurement_date": max(latest_measurement_dates) if latest_measurement_dates else None,
        "latest_review_decision": summary["latest_review_decision"],
        "recommended_next_action": recommended,
        "next_allowed_actions": next_allowed,
        "research_label": RESEARCH_LABEL,
        "created_at": utc_now_iso(),
        "schema_version": "paper_trial_operations_report.v1",
    }
    report["content_hash"] = content_hash(report, exclude={"created_at"}, sort_lists=True)
    validate_paper_trial_operations_report(report)
    return report


def write_paper_trial_operations_report(
    paper_trial_id: str,
    *,
    as_of_date: str,
    store_root: Path | None = None,
    actor: str = "Aegis",
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    report = build_paper_trial_operations_report(paper_trial_id, as_of_date=as_of_date, store_root=store)
    root = paper_trial_dir(paper_trial_id, store_root=store)
    write_json(root / "paper_trial_operations_report.json", report, overwrite=True)
    md = "\n".join(
        [
            "# Paper Trial Operations Report",
            "",
            report["research_label"],
            "",
            f"- paper_trial_id: {report['paper_trial_id']}",
            f"- status: {report['status']}",
            f"- observation_count: {report['observation_count']}",
            f"- candidate_count_total: {report['candidate_count_total']}",
            f"- pending_due_outcomes: {report['pending_due_outcomes']}",
            f"- due_outcomes: {report['due_outcomes']}",
            f"- measured_due_outcomes: {report['measured_due_outcomes']}",
            f"- measured_candidate_count: {report['measured_candidate_count']}",
            f"- recommended_next_action: {report['recommended_next_action']}",
        ]
    ) + "\n"
    (root / "paper_trial_operations_report.md").write_text(md, encoding="utf-8")
    row = {
        "paper_trial_id": paper_trial_id,
        "recommended_next_action": report["recommended_next_action"],
        "content_hash": report["content_hash"],
        "created_at": report["created_at"],
        "schema_version": report["schema_version"],
    }
    append_jsonl(_registry(store), row)
    write_audit_event(
        actor=actor,
        entity_type="paper_trial",
        entity_id=paper_trial_id,
        action="paper_trial_operations_report_written",
        new_state_hash=report["content_hash"],
        reason="Wrote paper trial operations report.",
        metadata={"registry_row": row},
        store_root=store,
    )
    return report
