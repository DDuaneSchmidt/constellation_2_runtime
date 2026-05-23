from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.paper_trials.due_outcomes import (
    append_due_outcome_status,
    create_due_outcomes_for_observation,
    list_paper_due_outcomes,
)
from research_lab.paper_trials.observation import record_paper_observation
from research_lab.paper_trials.operations_report import write_paper_trial_operations_report
from research_lab.paper_trials.outcome import measure_paper_outcomes
from research_lab.paper_trials.paper_trial_registry import (
    latest_paper_trial_status,
    paper_trial_dir,
    write_paper_trial_summary,
)
from research_lab.storage.manifest_io import read_json, read_jsonl
from research_lab.storage.paths import ensure_store_layout


def _day(value: Any) -> str:
    return str(value or "").strip()[:10]


def _observation_rows(paper_trial_id: str, *, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    rows = read_jsonl(store / "registries" / "paper_trial_observations.jsonl")
    return [row for row in rows if row.get("paper_trial_id") == paper_trial_id]


def _outcome_for_observation(paper_trial_id: str, observation_id: str, *, store_root: Path | None = None) -> dict[str, Any] | None:
    store = ensure_store_layout(store_root)
    for row in reversed(read_jsonl(store / "registries" / "paper_trial_outcomes.jsonl")):
        if row.get("paper_trial_id") == paper_trial_id and row.get("paper_trial_observation_id") == observation_id:
            path = paper_trial_dir(paper_trial_id, store_root=store) / "outcomes" / f"{row['paper_trial_outcome_id']}.json"
            return read_json(path) if path.exists() else row
    return None


def record_next_paper_observation(
    *,
    paper_trial_id: str,
    as_of_date: str,
    operator_note: str,
    store_root: Path | None = None,
    actor: str = "Aegis",
    allow_json_fallback: bool = False,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    if latest_paper_trial_status(paper_trial_id, store_root=store) != "active":
        raise RuntimeError("Paper trial must be active for observation")
    normalized_as_of = _day(as_of_date)
    if any(_day(row.get("as_of_date")) == normalized_as_of for row in _observation_rows(paper_trial_id, store_root=store)):
        write_audit_event(
            actor=actor,
            entity_type="paper_trial",
            entity_id=paper_trial_id,
            action="paper_trial_duplicate_observation_blocked",
            reason="Duplicate paper trial observation for same as_of_date was blocked.",
            metadata={"as_of_date": normalized_as_of},
            store_root=store,
        )
        raise RuntimeError(f"Duplicate paper trial observation for as_of_date: {normalized_as_of}")
    observation = record_paper_observation(
        paper_trial_id=paper_trial_id,
        as_of_date=normalized_as_of,
        operator_note=operator_note,
        store_root=store,
        actor=actor,
        allow_json_fallback=allow_json_fallback,
    )
    due_outcomes = create_due_outcomes_for_observation(
        paper_trial_id=paper_trial_id,
        paper_trial_observation_id=observation["paper_trial_observation_id"],
        store_root=store,
        actor=actor,
    )
    summary = write_paper_trial_summary(paper_trial_id, store_root=store)
    report = write_paper_trial_operations_report(paper_trial_id, as_of_date=normalized_as_of, store_root=store, actor=actor)
    return {"paper_trial_observation": observation, "due_outcomes": due_outcomes, "paper_trial_summary": summary, "operations_report": report}


def measure_due_paper_outcomes(
    *,
    paper_trial_id: str,
    as_of_date: str,
    store_root: Path | None = None,
    actor: str = "Aegis",
    allow_json_fallback: bool = False,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    due_payload = list_paper_due_outcomes(paper_trial_id, as_of_date=as_of_date, store_root=store, actor=actor)
    due_rows = [row for row in due_payload["due_outcomes"] if row.get("effective_status") == "due"]
    measured_rows: list[dict[str, Any]] = []
    outcomes_by_observation: dict[str, dict[str, Any]] = {}
    for row in due_rows:
        observation_id = row["paper_trial_observation_id"]
        if observation_id not in outcomes_by_observation:
            existing = _outcome_for_observation(paper_trial_id, observation_id, store_root=store)
            outcomes_by_observation[observation_id] = existing or measure_paper_outcomes(
                paper_trial_id=paper_trial_id,
                paper_trial_observation_id=observation_id,
                store_root=store,
                actor=actor,
                allow_json_fallback=allow_json_fallback,
            )
        outcome = outcomes_by_observation[observation_id]
        status = "measured" if outcome.get("status") == "measured" else "unavailable"
        measured_rows.append(
            append_due_outcome_status(
                due_outcome=row,
                status=status,
                measured_outcome_id=str(outcome["paper_trial_outcome_id"]),
                store_root=store,
            )
        )
    write_audit_event(
        actor=actor,
        entity_type="paper_trial",
        entity_id=paper_trial_id,
        action="paper_trial_due_outcomes_measured",
        reason="Measured due paper trial outcomes.",
        metadata={"as_of_date": _day(as_of_date), "due_count": len(due_rows), "measured_due_count": len(measured_rows)},
        store_root=store,
    )
    summary = write_paper_trial_summary(paper_trial_id, store_root=store)
    report = write_paper_trial_operations_report(paper_trial_id, as_of_date=as_of_date, store_root=store, actor=actor)
    return {"paper_trial_id": paper_trial_id, "measured_due_outcomes": measured_rows, "paper_trial_summary": summary, "operations_report": report}
