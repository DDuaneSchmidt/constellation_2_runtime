from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.candidates.candidate_batch import build_candidate_batch
from research_lab.candidates.candidate_generator import generate_drop_reversion_candidates
from research_lab.candidates.candidate_registry import candidate_batch_dir, load_candidates, store_candidate_batch
from research_lab.contracts.schemas import validate_contract
from research_lab.paper_trials.paper_trial_registry import (
    latest_paper_trial_status,
    load_paper_trial,
    paper_trial_dir,
    write_paper_trial_summary,
)
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout
from research_lab.storage.duckdb_query import load_dataset_snapshot_rows


def validate_paper_trial_observation(observation: dict[str, Any]) -> None:
    validate_contract("paper_trial_observation", observation)


def record_paper_observation(
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
    trial = load_paper_trial(paper_trial_id, store_root=store)
    symbols = sorted({str(row["symbol"]).upper() for row in load_dataset_snapshot_rows(trial["dataset_snapshot_id"], store_root=store)})
    batch_probe = build_candidate_batch(
        hypothesis_id=trial["hypothesis_id"],
        source_evidence_package_id=trial["source_evidence_package_ids"][-1],
        dataset_snapshot_id=trial["dataset_snapshot_id"],
        regime_snapshot_id=trial["regime_snapshot_id"],
        cost_model_snapshot_id=trial["cost_model_snapshot_id"],
        symbols=symbols,
        as_of_date=as_of_date,
        threshold=trial["threshold"],
        created_at=trial["created_at"],
        created_by=trial["created_by"],
        ranking_policy_version=trial["ranking_policy_version"],
    )
    batch_id = batch_probe["candidate_batch_id"]
    if not candidate_batch_dir(batch_id, store_root=store).exists():
        generated = generate_drop_reversion_candidates(
            source_evidence_package_id=trial["source_evidence_package_ids"][-1],
            dataset_snapshot_id=trial["dataset_snapshot_id"],
            regime_snapshot_id=trial["regime_snapshot_id"],
            cost_model_snapshot_id=trial["cost_model_snapshot_id"],
            as_of_date=as_of_date,
            threshold=trial["threshold"],
            created_by=actor,
            store_root=store,
        )
        store_candidate_batch(candidate_batch=generated["candidate_batch"], candidates=generated["candidates"], generation_summary=generated["generation_summary"], store_root=store, actor=actor, allow_json_fallback=allow_json_fallback)
        batch_id = generated["candidate_batch"]["candidate_batch_id"]
    candidates = load_candidates(batch_id, store_root=store)
    now = utc_now_iso()
    payload = {
        "paper_trial_id": paper_trial_id,
        "observation_date": now[:10],
        "as_of_date": as_of_date,
        "candidate_batch_id": batch_id,
        "candidate_count": len(candidates),
        "top_candidates": candidates[:5],
        "operator_note": operator_note,
        "status": "zero_candidates" if len(candidates) == 0 else "recorded",
        "created_at": now,
        "created_by": actor,
        "schema_version": "paper_trial_observation.v1",
        "research_label": "Forward observational paper trial; no broker execution; no live trading; not achieved portfolio performance.",
    }
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    payload["paper_trial_observation_id"] = f"ptro_{paper_trial_id}_{as_of_date.replace('-', '')}_{short_hash(payload['content_hash'], 10)}"
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    validate_paper_trial_observation(payload)
    path = paper_trial_dir(paper_trial_id, store_root=store) / "observations" / f"{payload['paper_trial_observation_id']}.json"
    write_json(path, payload, overwrite=False)
    row = {
        "paper_trial_observation_id": payload["paper_trial_observation_id"],
        "paper_trial_id": paper_trial_id,
        "observation_date": payload["observation_date"],
        "as_of_date": as_of_date,
        "candidate_batch_id": batch_id,
        "candidate_count": len(candidates),
        "status": payload["status"],
        "content_hash": payload["content_hash"],
        "created_at": payload["created_at"],
        "schema_version": payload["schema_version"],
    }
    append_jsonl(store / "registries" / "paper_trial_observations.jsonl", row)
    action = "paper_trial_zero_candidate_observation" if payload["status"] == "zero_candidates" else "paper_trial_observation_recorded"
    write_audit_event(actor=actor, entity_type="paper_trial_observation", entity_id=payload["paper_trial_observation_id"], action=action, new_state_hash=payload["content_hash"], reason="Recorded paper trial observation.", metadata={"registry_row": row}, store_root=store)
    write_paper_trial_summary(paper_trial_id, store_root=store)
    return payload
