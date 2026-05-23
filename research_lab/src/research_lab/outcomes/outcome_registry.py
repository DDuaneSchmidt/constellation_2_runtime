from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.candidates.candidate_registry import candidate_batch_dir, candidates_with_latest_status
from research_lab.candidates.outcome_record import validate_outcome_record
from research_lab.outcomes.attribution_report import attribution_markdown, candidate_outcome_summary
from research_lab.storage.manifest_io import append_jsonl, read_json, write_json
from research_lab.storage.parquet_io import file_sha256, read_parquet_records, write_parquet_records
from research_lab.storage.paths import ensure_store_layout


def outcomes_path(candidate_batch_id: str, *, store_root: Path | None = None) -> Path:
    return candidate_batch_dir(candidate_batch_id, store_root=store_root) / "outcomes.parquet"


def attribution_report_path(candidate_batch_id: str, *, store_root: Path | None = None) -> Path:
    return candidate_batch_dir(candidate_batch_id, store_root=store_root) / "attribution_report.json"


def attribution_report_md_path(candidate_batch_id: str, *, store_root: Path | None = None) -> Path:
    return candidate_batch_dir(candidate_batch_id, store_root=store_root) / "attribution_report.md"


def outcome_registry_path(store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "registries" / "outcome_records.jsonl"


def attribution_registry_path(store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "registries" / "attribution_reports.jsonl"


def store_outcomes_and_report(
    *,
    candidate_batch_id: str,
    outcomes: list[dict[str, Any]],
    attribution_report: dict[str, Any],
    store_root: Path | None = None,
    actor: str = "Aegis",
    allow_json_fallback: bool = False,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    batch_dir = candidate_batch_dir(candidate_batch_id, store_root=store)
    if not batch_dir.exists():
        raise RuntimeError(f"Candidate batch missing: {candidate_batch_id}")
    out_path = outcomes_path(candidate_batch_id, store_root=store)
    report_path = attribution_report_path(candidate_batch_id, store_root=store)
    report_md_path = attribution_report_md_path(candidate_batch_id, store_root=store)
    if out_path.exists() or report_path.exists() or report_md_path.exists():
        raise FileExistsError(f"Refusing to overwrite immutable outcome artifacts for {candidate_batch_id}")
    for row in outcomes:
        validate_outcome_record(row)
    write_parquet_records(out_path, outcomes, allow_json_fallback=allow_json_fallback)
    write_json(report_path, attribution_report, overwrite=False)
    report_md_path.write_text(attribution_markdown(attribution_report), encoding="utf-8")
    outcomes_hash = file_sha256(out_path)
    measured = [row for row in outcomes if row.get("outcome_status") == "measured"]
    unavailable = [row for row in outcomes if row.get("outcome_status") == "unavailable"]
    for row in outcomes:
        append_jsonl(
            outcome_registry_path(store),
            {
                "outcome_record_id": row["outcome_record_id"],
                "candidate_id": row["candidate_id"],
                "candidate_batch_id": row["candidate_batch_id"],
                "outcome_window": row["outcome_window"],
                "outcome_status": row["outcome_status"],
                "content_hash": row["content_hash"],
                "created_at": row["created_at"],
                "schema_version": row["schema_version"],
            },
        )
    registry_row = {
        "attribution_report_id": attribution_report["attribution_report_id"],
        "candidate_batch_id": candidate_batch_id,
        "candidate_count": attribution_report["candidate_count"],
        "measured_candidate_count": attribution_report["measured_candidate_count"],
        "unavailable_candidate_count": attribution_report["unavailable_candidate_count"],
        "outcomes_hash": outcomes_hash,
        "content_hash": attribution_report["content_hash"],
        "created_at": attribution_report["created_at"],
        "schema_version": attribution_report["schema_version"],
    }
    append_jsonl(attribution_registry_path(store), registry_row)
    write_audit_event(
        actor=actor,
        entity_type="candidate_batch",
        entity_id=candidate_batch_id,
        action="candidate_outcomes_measured",
        new_state_hash=outcomes_hash,
        reason="Measured candidate forward outcomes where data was available.",
        metadata={"outcome_count": len(outcomes), "measured_count": len(measured)},
        store_root=store,
    )
    if unavailable:
        write_audit_event(
            actor=actor,
            entity_type="candidate_batch",
            entity_id=candidate_batch_id,
            action="candidate_outcomes_unavailable",
            reason="Some candidate outcomes were unavailable.",
            metadata={"unavailable_count": len(unavailable)},
            store_root=store,
        )
    write_audit_event(
        actor=actor,
        entity_type="candidate_batch",
        entity_id=candidate_batch_id,
        action="attribution_report_written",
        new_state_hash=attribution_report["content_hash"],
        reason="Wrote deterministic candidate attribution report.",
        metadata={"registry_row": registry_row},
        store_root=store,
    )
    return {"registry_row": registry_row, "outcomes_hash": outcomes_hash, "attribution_report_path": str(report_path)}


def load_outcomes(candidate_batch_id: str, *, store_root: Path | None = None) -> list[dict[str, Any]]:
    return read_parquet_records(outcomes_path(candidate_batch_id, store_root=store_root))


def load_attribution_report(candidate_batch_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    return read_json(attribution_report_path(candidate_batch_id, store_root=store_root))


def build_candidate_outcome_summary(candidate_batch_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    report = load_attribution_report(candidate_batch_id, store_root=store)
    return candidate_outcome_summary(report, attribution_report_path(candidate_batch_id, store_root=store))


def candidate_status_rows(candidate_batch_id: str, *, store_root: Path | None = None) -> list[dict[str, Any]]:
    return candidates_with_latest_status(candidate_batch_id, store_root=store_root)

