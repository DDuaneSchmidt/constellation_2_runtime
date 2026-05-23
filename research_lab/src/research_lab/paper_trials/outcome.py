from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import validate_contract
from research_lab.outcomes.measurement_runner import run_candidate_outcome_measurement
from research_lab.outcomes.outcome_registry import load_attribution_report
from research_lab.paper_trials.paper_trial_registry import load_paper_trial, paper_trial_dir, write_paper_trial_summary
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, read_json, write_json
from research_lab.storage.paths import ensure_store_layout


def validate_paper_trial_outcome(outcome: dict[str, Any]) -> None:
    validate_contract("paper_trial_outcome", outcome)


def measure_paper_outcomes(
    *,
    paper_trial_id: str,
    paper_trial_observation_id: str,
    store_root: Path | None = None,
    actor: str = "Aegis",
    allow_json_fallback: bool = False,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    trial = load_paper_trial(paper_trial_id, store_root=store)
    observation = read_json(paper_trial_dir(paper_trial_id, store_root=store) / "observations" / f"{paper_trial_observation_id}.json")
    batch_id = observation["candidate_batch_id"]
    if not (store / "candidate_batches" / batch_id / "attribution_report.json").exists():
        run_candidate_outcome_measurement(
            candidate_batch_id=batch_id,
            dataset_snapshot_id=trial["dataset_snapshot_id"],
            cost_model_snapshot_id=trial["cost_model_snapshot_id"],
            benchmark_symbol="SPY",
            windows=trial["outcome_windows"],
            store_root=store,
            actor=actor,
            allow_json_fallback=allow_json_fallback,
        )
    report = load_attribution_report(batch_id, store_root=store)
    status = "measured" if int(report.get("candidate_count", 0)) == 0 or int(report.get("measured_candidate_count", 0)) > 0 else "unavailable"
    payload = {
        "paper_trial_id": paper_trial_id,
        "paper_trial_observation_id": paper_trial_observation_id,
        "candidate_batch_id": batch_id,
        "outcome_windows": trial["outcome_windows"],
        "outcome_record_count": len([]),
        "measured_candidate_count": report["measured_candidate_count"],
        "unavailable_candidate_count": report["unavailable_candidate_count"],
        "attribution_report_id": report["attribution_report_id"],
        "status": status,
        "created_at": utc_now_iso(),
        "schema_version": "paper_trial_outcome.v1",
        "research_label": "Forward observational paper trial; no broker execution; no live trading; not achieved portfolio performance.",
    }
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    payload["paper_trial_outcome_id"] = f"ptro_{paper_trial_observation_id}_{short_hash(payload['content_hash'], 10)}"
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    validate_paper_trial_outcome(payload)
    write_json(paper_trial_dir(paper_trial_id, store_root=store) / "outcomes" / f"{payload['paper_trial_outcome_id']}.json", payload, overwrite=False)
    row = {
        "paper_trial_outcome_id": payload["paper_trial_outcome_id"],
        "paper_trial_id": paper_trial_id,
        "paper_trial_observation_id": paper_trial_observation_id,
        "candidate_batch_id": batch_id,
        "measured_candidate_count": payload["measured_candidate_count"],
        "unavailable_candidate_count": payload["unavailable_candidate_count"],
        "status": payload["status"],
        "content_hash": payload["content_hash"],
        "created_at": payload["created_at"],
        "schema_version": payload["schema_version"],
    }
    append_jsonl(store / "registries" / "paper_trial_outcomes.jsonl", row)
    write_audit_event(actor=actor, entity_type="paper_trial_outcome", entity_id=payload["paper_trial_outcome_id"], action="paper_trial_outcomes_measured", new_state_hash=payload["content_hash"], reason="Measured paper trial observation outcomes.", metadata={"registry_row": row}, store_root=store)
    write_paper_trial_summary(paper_trial_id, store_root=store)
    return payload

