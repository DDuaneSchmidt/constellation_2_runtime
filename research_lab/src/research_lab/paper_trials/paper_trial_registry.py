from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import validate_contract
from research_lab.costs.cost_model_registry import cost_model_snapshot_path
from research_lab.datasets.dataset_registry import dataset_snapshot_path
from research_lab.evidence.evidence_registry import evidence_manifest_path
from research_lab.regimes.regime_registry import regime_snapshot_path
from research_lab.sleeves.sleeve_registry import load_sleeve_version
from research_lab.storage.hashing import utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


def paper_trial_dir(paper_trial_id: str, *, store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "paper_trials" / paper_trial_id


def paper_trial_path(paper_trial_id: str, *, store_root: Path | None = None) -> Path:
    return paper_trial_dir(paper_trial_id, store_root=store_root) / "paper_trial.json"


def paper_trial_summary_path(paper_trial_id: str, *, store_root: Path | None = None) -> Path:
    return paper_trial_dir(paper_trial_id, store_root=store_root) / "paper_trial_summary.json"


def _registry(name: str, store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "registries" / name


def _require(path: Path, label: str) -> None:
    if not path.exists():
        raise RuntimeError(f"{label} missing: {path}")


def store_paper_trial(trial: dict[str, Any], *, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    validate_contract("paper_trial", trial)
    store = ensure_store_layout(store_root)
    load_sleeve_version(trial["sleeve_id"], trial["sleeve_version_id"], store_root=store)
    _require(dataset_snapshot_path(trial["dataset_snapshot_id"], store_root=store), "Dataset snapshot")
    _require(regime_snapshot_path(trial["regime_snapshot_id"], store_root=store), "Regime snapshot")
    _require(cost_model_snapshot_path(trial["cost_model_snapshot_id"], store_root=store), "Cost model snapshot")
    for evidence_id in trial["source_evidence_package_ids"]:
        _require(evidence_manifest_path(evidence_id, store_root=store), "Source evidence package")
    root = paper_trial_dir(trial["paper_trial_id"], store_root=store)
    root.mkdir(parents=True, exist_ok=False)
    for sub in ["observations", "outcomes", "reviews", "due_outcomes"]:
        (root / sub).mkdir(exist_ok=False)
    write_json(paper_trial_path(trial["paper_trial_id"], store_root=store), trial, overwrite=False)
    row = {
        "paper_trial_id": trial["paper_trial_id"],
        "sleeve_id": trial["sleeve_id"],
        "sleeve_version_id": trial["sleeve_version_id"],
        "status": trial["status"],
        "event_type": "created",
        "content_hash": trial["content_hash"],
        "created_at": trial["created_at"],
        "schema_version": trial["schema_version"],
    }
    append_jsonl(_registry("paper_trials.jsonl", store), row)
    write_audit_event(actor=actor, entity_type="paper_trial", entity_id=trial["paper_trial_id"], action="paper_trial_created", new_state_hash=trial["content_hash"], reason="Created forward observational paper trial.", metadata={"registry_row": row}, store_root=store)
    write_paper_trial_summary(trial["paper_trial_id"], store_root=store)
    return row


def load_paper_trial(paper_trial_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    return read_json(paper_trial_path(paper_trial_id, store_root=store_root))


def paper_trial_events(paper_trial_id: str, *, store_root: Path | None = None) -> list[dict[str, Any]]:
    return [row for row in read_jsonl(_registry("paper_trials.jsonl", store_root)) if row.get("paper_trial_id") == paper_trial_id]


def latest_paper_trial_status(paper_trial_id: str, *, store_root: Path | None = None) -> str:
    events = paper_trial_events(paper_trial_id, store_root=store_root)
    return events[-1]["status"] if events else load_paper_trial(paper_trial_id, store_root=store_root)["status"]


def change_paper_trial_status(paper_trial_id: str, new_status: str, *, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    allowed = {("draft", "active"), ("active", "paused"), ("active", "completed"), ("active", "cancelled"), ("paused", "active")}
    current = latest_paper_trial_status(paper_trial_id, store_root=store_root)
    if (current, new_status) not in allowed:
        raise RuntimeError(f"Invalid paper trial status transition: {current} -> {new_status}")
    trial = load_paper_trial(paper_trial_id, store_root=store_root)
    row = {
        "paper_trial_id": paper_trial_id,
        "sleeve_id": trial["sleeve_id"],
        "sleeve_version_id": trial["sleeve_version_id"],
        "status": new_status,
        "event_type": "status_changed",
        "previous_status": current,
        "created_at": utc_now_iso(),
        "schema_version": "paper_trial_status_event.v1",
    }
    append_jsonl(_registry("paper_trials.jsonl", store_root), row)
    action = "paper_trial_activated" if new_status == "active" else "paper_trial_status_changed"
    write_audit_event(actor=actor, entity_type="paper_trial", entity_id=paper_trial_id, action=action, reason=f"Paper trial status changed {current} -> {new_status}.", metadata={"status_event": row}, store_root=store_root)
    write_paper_trial_summary(paper_trial_id, store_root=store_root)
    return row


def _rows(registry: str, paper_trial_id: str, store_root: Path | None = None) -> list[dict[str, Any]]:
    return [row for row in read_jsonl(_registry(registry, store_root)) if row.get("paper_trial_id") == paper_trial_id]


def build_paper_trial_summary(paper_trial_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    trial = load_paper_trial(paper_trial_id, store_root=store_root)
    observations = _rows("paper_trial_observations.jsonl", paper_trial_id, store_root)
    outcomes = _rows("paper_trial_outcomes.jsonl", paper_trial_id, store_root)
    reviews = _rows("paper_trial_reviews.jsonl", paper_trial_id, store_root)
    status = latest_paper_trial_status(paper_trial_id, store_root=store_root)
    return {
        "paper_trial_id": paper_trial_id,
        "sleeve_id": trial["sleeve_id"],
        "sleeve_version_id": trial["sleeve_version_id"],
        "status": status,
        "observation_count": len(observations),
        "candidate_count_total": sum(int(row.get("candidate_count", 0)) for row in observations),
        "zero_candidate_observation_count": sum(1 for row in observations if row.get("status") == "zero_candidates"),
        "measured_outcome_count": sum(int(row.get("measured_candidate_count", 0)) for row in outcomes),
        "unavailable_outcome_count": sum(int(row.get("unavailable_candidate_count", 0)) for row in outcomes),
        "latest_observation_date": observations[-1]["observation_date"] if observations else None,
        "latest_review_decision": reviews[-1]["review_decision"] if reviews else None,
        "next_allowed_actions": ["record-paper-observation", "measure-paper-outcomes", "review-paper-trial"] if status == "active" else ["activate-paper-trial"] if status in {"draft", "paused"} else [],
        "research_label": "Forward observational paper trial; no broker execution; no live trading; not achieved portfolio performance.",
        "schema_version": "paper_trial_summary.v1",
    }


def write_paper_trial_summary(paper_trial_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    summary = build_paper_trial_summary(paper_trial_id, store_root=store_root)
    write_json(paper_trial_summary_path(paper_trial_id, store_root=store_root), summary, overwrite=True)
    md = "\n".join([
        "# Paper Trial Summary",
        "",
        summary["research_label"],
        "",
        f"- status: {summary['status']}",
        f"- observation_count: {summary['observation_count']}",
        f"- candidate_count_total: {summary['candidate_count_total']}",
        f"- zero_candidate_observation_count: {summary['zero_candidate_observation_count']}",
        f"- measured_outcome_count: {summary['measured_outcome_count']}",
        f"- latest_review_decision: {summary['latest_review_decision']}",
    ]) + "\n"
    (paper_trial_dir(paper_trial_id, store_root=store_root) / "paper_trial_summary.md").write_text(md, encoding="utf-8")
    return summary
