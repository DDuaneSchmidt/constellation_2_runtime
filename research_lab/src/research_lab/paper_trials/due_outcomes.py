from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import validate_contract
from research_lab.paper_trials.paper_trial_registry import load_paper_trial, paper_trial_dir
from research_lab.storage.duckdb_query import load_dataset_snapshot_rows
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


RESEARCH_LABEL = "Forward observational paper trial; no broker execution; no live trading; not achieved portfolio performance."


def validate_paper_trial_due_outcome(due_outcome: dict[str, Any]) -> None:
    validate_contract("paper_trial_due_outcome", due_outcome)


def due_outcomes_dir(paper_trial_id: str, *, store_root: Path | None = None) -> Path:
    return paper_trial_dir(paper_trial_id, store_root=store_root) / "due_outcomes"


def due_outcomes_registry_path(store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "registries" / "paper_trial_due_outcomes.jsonl"


def _day(value: Any) -> str:
    return str(value or "").strip()[:10]


def _trading_days(dataset_snapshot_id: str, *, store_root: Path | None = None) -> list[str]:
    return sorted({_day(row.get("date")) for row in load_dataset_snapshot_rows(dataset_snapshot_id, store_root=store_root) if _day(row.get("date"))})


def compute_due_date(dataset_snapshot_id: str, as_of_date: str, outcome_window: int, *, store_root: Path | None = None) -> str:
    days = _trading_days(dataset_snapshot_id, store_root=store_root)
    if not days:
        raise RuntimeError("Cannot compute due dates without dataset trading days.")
    target = _day(as_of_date)
    prior_or_equal = [idx for idx, day in enumerate(days) if day <= target]
    if not prior_or_equal:
        raise RuntimeError(f"as_of_date before dataset calendar: {as_of_date}")
    start_idx = prior_or_equal[-1]
    due_idx = start_idx + int(outcome_window)
    if due_idx >= len(days):
        return ""
    return days[due_idx]


def build_paper_trial_due_outcome(
    *,
    paper_trial_id: str,
    paper_trial_observation_id: str,
    candidate_batch_id: str,
    outcome_window: int,
    due_date: str,
    status: str,
    measured_outcome_id: str = "",
    created_at: str | None = None,
) -> dict[str, Any]:
    payload = {
        "paper_trial_id": paper_trial_id,
        "paper_trial_observation_id": paper_trial_observation_id,
        "candidate_batch_id": candidate_batch_id,
        "outcome_window": int(outcome_window),
        "due_date": _day(due_date),
        "status": status,
        "measured_outcome_id": measured_outcome_id,
        "created_at": created_at or utc_now_iso(),
        "schema_version": "paper_trial_due_outcome.v1",
        "research_label": RESEARCH_LABEL,
    }
    seed = content_hash(payload, exclude={"created_at", "status", "measured_outcome_id"}, sort_lists=True)
    payload["paper_trial_due_outcome_id"] = f"ptdo_{paper_trial_observation_id}_{outcome_window}_{short_hash(seed, 10)}"
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    validate_paper_trial_due_outcome(payload)
    return payload


def create_due_outcomes_for_observation(
    *,
    paper_trial_id: str,
    paper_trial_observation_id: str,
    store_root: Path | None = None,
    actor: str = "Aegis",
) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    trial = load_paper_trial(paper_trial_id, store_root=store)
    observation_path = paper_trial_dir(paper_trial_id, store_root=store) / "observations" / f"{paper_trial_observation_id}.json"
    observation = read_json(observation_path)
    due_dir = due_outcomes_dir(paper_trial_id, store_root=store)
    due_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    for window in trial["outcome_windows"]:
        due_date = compute_due_date(trial["dataset_snapshot_id"], observation["as_of_date"], int(window), store_root=store)
        status = "pending" if due_date else "unavailable"
        due = build_paper_trial_due_outcome(
            paper_trial_id=paper_trial_id,
            paper_trial_observation_id=paper_trial_observation_id,
            candidate_batch_id=observation["candidate_batch_id"],
            outcome_window=int(window),
            due_date=due_date,
            status=status,
        )
        path = due_dir / f"{due['paper_trial_due_outcome_id']}.json"
        write_json(path, due, overwrite=False)
        append_jsonl(due_outcomes_registry_path(store), due)
        rows.append(due)
    write_audit_event(
        actor=actor,
        entity_type="paper_trial",
        entity_id=paper_trial_id,
        action="paper_trial_due_outcomes_created",
        new_state_hash=content_hash({"due_outcomes": rows}, sort_lists=True),
        reason="Created due outcome records for paper trial observation.",
        metadata={"paper_trial_observation_id": paper_trial_observation_id, "due_outcome_count": len(rows)},
        store_root=store,
    )
    return rows


def _latest_due_rows(paper_trial_id: str, *, store_root: Path | None = None) -> list[dict[str, Any]]:
    rows = [row for row in read_jsonl(due_outcomes_registry_path(store_root)) if row.get("paper_trial_id") == paper_trial_id]
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        latest[str(row["paper_trial_due_outcome_id"])] = row
    return [latest[key] for key in sorted(latest)]


def list_paper_due_outcomes(paper_trial_id: str, *, as_of_date: str, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    as_of = _day(as_of_date)
    rows = []
    counts = {"pending_count": 0, "due_count": 0, "measured_count": 0, "unavailable_count": 0}
    for row in _latest_due_rows(paper_trial_id, store_root=store_root):
        item = dict(row)
        stored_status = str(item.get("status") or "pending")
        effective_status = stored_status
        if stored_status in {"pending", "due"} and _day(item.get("due_date")) and _day(item["due_date"]) <= as_of:
            effective_status = "due"
        item["effective_status"] = effective_status
        counts[f"{effective_status}_count"] += 1
        rows.append(item)
    write_audit_event(
        actor=actor,
        entity_type="paper_trial",
        entity_id=paper_trial_id,
        action="paper_trial_due_outcomes_listed",
        reason="Listed paper trial due outcomes.",
        metadata={"as_of_date": as_of, **counts},
        store_root=store_root,
    )
    return {"paper_trial_id": paper_trial_id, "as_of_date": as_of, **counts, "due_outcomes": rows}


def append_due_outcome_status(
    *,
    due_outcome: dict[str, Any],
    status: str,
    measured_outcome_id: str,
    store_root: Path | None = None,
) -> dict[str, Any]:
    updated = dict(due_outcome)
    updated["status"] = status
    updated["measured_outcome_id"] = measured_outcome_id
    updated["created_at"] = utc_now_iso()
    updated["content_hash"] = content_hash(updated, exclude={"created_at"}, sort_lists=True)
    validate_paper_trial_due_outcome(updated)
    append_jsonl(due_outcomes_registry_path(store_root), updated)
    return updated
