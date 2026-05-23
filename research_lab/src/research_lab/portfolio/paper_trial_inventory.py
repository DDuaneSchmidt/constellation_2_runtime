from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.portfolio.evidence_inventory import RESEARCH_LABEL, _read_optional, _registry, _write_report
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import read_json, read_jsonl
from research_lab.storage.paths import ensure_store_layout


def build_paper_trial_inventory(*, store_root: Path | None = None, created_at: str | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    trials: list[dict[str, Any]] = []
    for summary_path in sorted((store / "paper_trials").glob("*/paper_trial_summary.json")):
        summary = read_json(summary_path)
        missing: list[dict[str, str]] = []
        operations = _read_optional(summary_path.parent / "paper_trial_operations_report.json", missing)
        trials.append(
            {
                "paper_trial_id": summary["paper_trial_id"],
                "sleeve_id": summary["sleeve_id"],
                "sleeve_version_id": summary["sleeve_version_id"],
                "status": summary["status"],
                "observation_count": int(summary.get("observation_count", 0)),
                "candidate_count_total": int(summary.get("candidate_count_total", 0)),
                "zero_candidate_observation_count": int(summary.get("zero_candidate_observation_count", 0)),
                "measured_candidate_count": int(operations.get("measured_candidate_count", summary.get("measured_outcome_count", 0))),
                "pending_due_outcomes": int(operations.get("pending_due_outcomes", 0)),
                "due_outcomes": int(operations.get("due_outcomes", 0)),
                "recommended_next_action": operations.get("recommended_next_action") or "record_more_observations",
                "next_allowed_actions": operations.get("next_allowed_actions") or summary.get("next_allowed_actions") or [],
                "latest_review_decision": summary.get("latest_review_decision"),
                "missing_artifacts": missing,
            }
        )
    trials.sort(key=lambda row: row["paper_trial_id"])
    payload = {
        "created_at": created_at or utc_now_iso(),
        "active_trials": sum(1 for row in trials if row["status"] == "active"),
        "paused_trials": sum(1 for row in trials if row["status"] == "paused"),
        "completed_trials": sum(1 for row in trials if row["status"] == "completed"),
        "cancelled_trials": sum(1 for row in trials if row["status"] == "cancelled"),
        "trials": trials,
        "research_label": RESEARCH_LABEL,
        "schema_version": "paper_trial_inventory.v1",
    }
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    payload["paper_trial_inventory_id"] = f"ptinv_{short_hash(payload['content_hash'], 12)}"
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    validate_contract("paper_trial_inventory", payload)
    return payload


def paper_trial_inventory_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Paper Trial Inventory",
        "",
        payload["research_label"],
        "",
        f"- active_trials: {payload['active_trials']}",
        f"- paused_trials: {payload['paused_trials']}",
        f"- completed_trials: {payload['completed_trials']}",
        f"- cancelled_trials: {payload['cancelled_trials']}",
        "",
        "## Trials",
    ]
    for row in payload["trials"]:
        lines.append(f"- {row['paper_trial_id']}: status={row['status']} observations={row['observation_count']} next={row['recommended_next_action']}")
    return "\n".join(lines) + "\n"


def write_paper_trial_inventory(*, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    payload = build_paper_trial_inventory(store_root=store)
    row = {
        "paper_trial_inventory_id": payload["paper_trial_inventory_id"],
        "active_trials": payload["active_trials"],
        "content_hash": payload["content_hash"],
        "created_at": payload["created_at"],
        "schema_version": payload["schema_version"],
    }
    return _write_report(
        store=store,
        family="paper_trial_inventory",
        report_id=payload["paper_trial_inventory_id"],
        payload=payload,
        registry_name="paper_trial_inventories.jsonl",
        registry_row=row,
        markdown=paper_trial_inventory_markdown(payload),
        audit_action="paper_trial_inventory_written",
        actor=actor,
    )


def latest_paper_trial_inventory(*, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    rows = read_jsonl(_registry(store, "paper_trial_inventories.jsonl"))
    if not rows:
        return {}
    latest = sorted(rows, key=lambda row: str(row.get("created_at") or ""))[-1]
    return read_json(store / "portfolio_reports" / "paper_trial_inventory" / f"{latest['paper_trial_inventory_id']}.json")
