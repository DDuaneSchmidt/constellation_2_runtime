from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.candidates.candidate_batch import validate_candidate_batch, validate_candidate
from research_lab.candidates.operator_decision import latest_decisions_by_candidate, load_operator_decisions
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.parquet_io import file_sha256, read_parquet_records, write_parquet_records
from research_lab.storage.paths import candidate_batch_uri, ensure_store_layout


def candidate_batch_dir(candidate_batch_id: str, *, store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "candidate_batches" / candidate_batch_id


def candidate_batch_manifest_path(candidate_batch_id: str, *, store_root: Path | None = None) -> Path:
    return candidate_batch_dir(candidate_batch_id, store_root=store_root) / "candidate_batch.json"


def candidates_path(candidate_batch_id: str, *, store_root: Path | None = None) -> Path:
    return candidate_batch_dir(candidate_batch_id, store_root=store_root) / "candidates.parquet"


def candidate_scores_path(candidate_batch_id: str, *, store_root: Path | None = None) -> Path:
    return candidate_batch_dir(candidate_batch_id, store_root=store_root) / "candidate_scores.parquet"


def generation_summary_path(candidate_batch_id: str, *, store_root: Path | None = None) -> Path:
    return candidate_batch_dir(candidate_batch_id, store_root=store_root) / "generation_summary.json"


def candidate_batches_registry_path(store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "registries" / "candidate_batches.jsonl"


def store_candidate_batch(
    *,
    candidate_batch: dict[str, Any],
    candidates: list[dict[str, Any]],
    generation_summary: dict[str, Any],
    store_root: Path | None = None,
    actor: str = "Aegis",
    allow_json_fallback: bool = False,
) -> dict[str, Any]:
    validate_candidate_batch(candidate_batch)
    for candidate in candidates:
        validate_candidate(candidate)
    store = ensure_store_layout(store_root)
    batch_id = candidate_batch["candidate_batch_id"]
    batch_dir = candidate_batch_dir(batch_id, store_root=store)
    batch_dir.mkdir(parents=True, exist_ok=False)
    write_json(candidate_batch_manifest_path(batch_id, store_root=store), candidate_batch, overwrite=False)
    write_json(generation_summary_path(batch_id, store_root=store), generation_summary, overwrite=False)
    write_parquet_records(candidates_path(batch_id, store_root=store), candidates, allow_json_fallback=allow_json_fallback)
    score_rows = [
        {
            "candidate_id": row["candidate_id"],
            "candidate_batch_id": row["candidate_batch_id"],
            "symbol": row["symbol"],
            "ranking_score": row["ranking_score"],
            "ranking_bucket": row["ranking_bucket"],
            "why_now": row["why_now"],
            "schema_version": "candidate_score.v1",
        }
        for row in candidates
    ]
    write_parquet_records(candidate_scores_path(batch_id, store_root=store), score_rows, allow_json_fallback=allow_json_fallback)
    registry_row = {
        "candidate_batch_id": batch_id,
        "hypothesis_id": candidate_batch["hypothesis_id"],
        "source_evidence_package_id": candidate_batch["source_evidence_package_id"],
        "dataset_snapshot_id": candidate_batch["dataset_snapshot_id"],
        "regime_snapshot_id": candidate_batch["regime_snapshot_id"],
        "cost_model_snapshot_id": candidate_batch["cost_model_snapshot_id"],
        "as_of_date": candidate_batch["as_of_date"],
        "candidate_count": len(candidates),
        "content_hash": candidate_batch["content_hash"],
        "candidates_hash": file_sha256(candidates_path(batch_id, store_root=store)),
        "candidate_scores_hash": file_sha256(candidate_scores_path(batch_id, store_root=store)),
        "storage_uri": candidate_batch_uri(batch_id),
        "created_at": candidate_batch["created_at"],
        "schema_version": candidate_batch["schema_version"],
    }
    append_jsonl(candidate_batches_registry_path(store), registry_row)
    write_audit_event(
        actor=actor,
        entity_type="candidate_batch",
        entity_id=batch_id,
        action="candidate_batch_created",
        new_state_hash=candidate_batch["content_hash"],
        reason="Created immutable advisory candidate batch.",
        metadata={"registry_row": registry_row},
        store_root=store,
    )
    write_audit_event(
        actor=actor,
        entity_type="candidate_batch",
        entity_id=batch_id,
        action="candidates_generated",
        new_state_hash=registry_row["candidates_hash"],
        reason="Generated advisory candidates from research evidence.",
        metadata={"candidate_count": len(candidates), "as_of_date": candidate_batch["as_of_date"]},
        store_root=store,
    )
    write_audit_event(
        actor=actor,
        entity_type="candidate_batch",
        entity_id=batch_id,
        action="candidate_scores_written",
        new_state_hash=registry_row["candidate_scores_hash"],
        reason="Wrote deterministic candidate scores.",
        metadata={"ranking_policy_version": candidate_batch["ranking_policy_version"]},
        store_root=store,
    )
    return registry_row


def load_candidate_batch(candidate_batch_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    return read_json(candidate_batch_manifest_path(candidate_batch_id, store_root=store_root))


def load_candidates(candidate_batch_id: str, *, store_root: Path | None = None) -> list[dict[str, Any]]:
    return read_parquet_records(candidates_path(candidate_batch_id, store_root=store_root))


def load_generation_summary(candidate_batch_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    return read_json(generation_summary_path(candidate_batch_id, store_root=store_root))


def list_candidate_batches(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    return read_jsonl(candidate_batches_registry_path(store_root))


def candidates_with_latest_status(candidate_batch_id: str, *, store_root: Path | None = None) -> list[dict[str, Any]]:
    latest = latest_decisions_by_candidate(store_root=store_root)
    rows = load_candidates(candidate_batch_id, store_root=store_root)
    decision_to_status = {"approve": "approved", "ignore": "ignored", "defer": "deferred", "reject": "rejected"}
    enriched: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        decision = latest.get(row["candidate_id"])
        if decision:
            item["derived_candidate_status"] = decision_to_status[decision["decision"]]
            item["latest_operator_decision_id"] = decision["operator_decision_id"]
            item["latest_decision_reason"] = decision["decision_reason"]
            item["latest_decided_by"] = decision["decided_by"]
        else:
            item["derived_candidate_status"] = row["candidate_status"]
        enriched.append(item)
    return enriched


def candidate_ledger_summary(candidate_batch_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    summary = load_generation_summary(candidate_batch_id, store_root=store_root)
    candidates = candidates_with_latest_status(candidate_batch_id, store_root=store_root)
    decisions = [row for row in load_operator_decisions(store_root=store_root) if row["candidate_batch_id"] == candidate_batch_id]
    status_counts: dict[str, int] = {}
    for row in candidates:
        status_counts[row["derived_candidate_status"]] = status_counts.get(row["derived_candidate_status"], 0) + 1
    return {
        "candidate_batch_id": candidate_batch_id,
        "candidate_count": len(candidates),
        "decision_count": len(decisions),
        "status_counts": status_counts,
        "top_candidates": summary.get("top_candidates", []),
        "generation_summary": summary,
        "schema_version": "candidate_ledger_summary.v1",
    }

