from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.candidates.candidate_registry import candidates_with_latest_status
from research_lab.outcomes.attribution_report import build_attribution_report, candidate_outcome_summary
from research_lab.outcomes.outcome_measurement import measure_candidate_outcomes
from research_lab.outcomes.outcome_registry import attribution_report_path, store_outcomes_and_report
from research_lab.storage.paths import ensure_store_layout


def run_candidate_outcome_measurement(
    *,
    candidate_batch_id: str,
    dataset_snapshot_id: str,
    cost_model_snapshot_id: str,
    benchmark_symbol: str,
    windows: list[int],
    store_root: Path | None = None,
    actor: str = "Aegis",
    allow_json_fallback: bool = False,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    write_audit_event(
        actor=actor,
        entity_type="candidate_batch",
        entity_id=candidate_batch_id,
        action="candidate_outcome_measurement_started",
        reason="Started candidate outcome measurement.",
        metadata={"windows": windows, "benchmark_symbol": benchmark_symbol},
        store_root=store,
    )
    try:
        measured = measure_candidate_outcomes(
            candidate_batch_id=candidate_batch_id,
            dataset_snapshot_id=dataset_snapshot_id,
            cost_model_snapshot_id=cost_model_snapshot_id,
            benchmark_symbol=benchmark_symbol,
            windows=windows,
            store_root=store,
        )
        candidates = candidates_with_latest_status(candidate_batch_id, store_root=store)
        report = build_attribution_report(
            candidate_batch=measured["candidate_batch"],
            candidates=candidates,
            outcomes=measured["outcomes"],
            dataset_snapshot_id=dataset_snapshot_id,
            cost_model_snapshot_id=cost_model_snapshot_id,
            benchmark_symbol=benchmark_symbol,
            windows=measured["windows"],
            created_at=measured["created_at"],
        )
        stored = store_outcomes_and_report(
            candidate_batch_id=candidate_batch_id,
            outcomes=measured["outcomes"],
            attribution_report=report,
            store_root=store,
            actor=actor,
            allow_json_fallback=allow_json_fallback,
        )
        summary = candidate_outcome_summary(report, attribution_report_path(candidate_batch_id, store_root=store))
        write_audit_event(
            actor=actor,
            entity_type="candidate_batch",
            entity_id=candidate_batch_id,
            action="candidate_outcome_measurement_completed",
            new_state_hash=report["content_hash"],
            reason="Completed candidate outcome measurement and attribution.",
            metadata={"candidate_count": report["candidate_count"], "measured_candidate_count": report["measured_candidate_count"]},
            store_root=store,
        )
        return {"outcomes": measured["outcomes"], "attribution_report": report, "summary": summary, **stored}
    except Exception as exc:
        write_audit_event(
            actor=actor,
            entity_type="candidate_batch",
            entity_id=candidate_batch_id,
            action="candidate_outcome_measurement_failed",
            reason=str(exc),
            metadata={"dataset_snapshot_id": dataset_snapshot_id, "cost_model_snapshot_id": cost_model_snapshot_id},
            store_root=store,
        )
        raise

