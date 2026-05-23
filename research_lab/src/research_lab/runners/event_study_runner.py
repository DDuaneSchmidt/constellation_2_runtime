from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.costs.cost_adjustments import apply_costs_to_forward_returns
from research_lab.costs.cost_model_registry import load_cost_model_snapshot
from research_lab.datasets.dataset_registry import load_dataset_snapshot
from research_lab.events.event_extractor import extract_events
from research_lab.evidence.evidence_package import build_event_study_summary, write_event_study_evidence_package
from research_lab.evidence.evidence_registry import append_evidence_registry_entry
from research_lab.research.research_plan_registry import load_research_plan
from research_lab.regimes.regime_registry import load_regime_snapshot
from research_lab.runners.forward_returns import calculate_forward_returns
from research_lab.storage.duckdb_query import canonical_parquet_path, load_dataset_snapshot_rows
from research_lab.storage.hashing import content_hash
from research_lab.storage.parquet_io import read_parquet_records
from research_lab.storage.paths import resolve_research_uri
from research_lab.storage.paths import ensure_store_layout
from research_lab.universes.universe_registry import load_universe_snapshot


def _audit(
    *,
    action: str,
    research_plan_id: str,
    reason: str,
    metadata: dict[str, Any],
    actor: str,
    store_root: Path | None,
    new_state_hash: str = "",
) -> None:
    write_audit_event(
        actor=actor,
        entity_type="event_study",
        entity_id=research_plan_id,
        action=action,
        previous_state_hash="",
        new_state_hash=new_state_hash,
        reason=reason,
        metadata=metadata,
        store_root=store_root,
    )


def run_event_study(
    *,
    research_plan_id: str,
    store_root: Path | None = None,
    actor: str = "Aegis",
    allow_json_fallback: bool = False,
    regime_snapshot_id: str | None = None,
    cost_model_snapshot_id: str | None = None,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    _audit(
        action="event_study_started",
        research_plan_id=research_plan_id,
        reason="Started deterministic event study.",
        metadata={"research_plan_id": research_plan_id},
        actor=actor,
        store_root=store,
    )
    try:
        research_plan = load_research_plan(research_plan_id, store_root=store)
        dataset_snapshot = load_dataset_snapshot(research_plan["dataset_snapshot_id"], store_root=store)
        universe = load_universe_snapshot(research_plan["universe_snapshot_id"], store_root=store)
        category_by_symbol = {
            str(row["symbol"]).upper(): str(row.get("category") or "UNKNOWN")
            for row in universe.get("symbols", [])
        }
        parquet_path = canonical_parquet_path(research_plan["dataset_snapshot_id"], store_root=store)
        if not parquet_path.exists():
            raise RuntimeError(f"Canonical dataset parquet missing: {parquet_path}")
        rows = load_dataset_snapshot_rows(research_plan["dataset_snapshot_id"], store_root=store)
        if not rows:
            raise RuntimeError("Dataset snapshot has no canonical rows")

        events = extract_events(rows, research_plan)
        if not events:
            raise RuntimeError("Event count is zero")
        _audit(
            action="events_extracted",
            research_plan_id=research_plan_id,
            reason="Extracted deterministic event table.",
            metadata={"event_count": len(events), "event_hash": content_hash({"events": events}, sort_lists=False)},
            actor=actor,
            store_root=store,
        )

        regime_snapshot = None
        if regime_snapshot_id:
            regime_snapshot = load_regime_snapshot(regime_snapshot_id, store_root=store)
            labels_path = resolve_research_uri(regime_snapshot["labels"]["labels_uri"], store_root=store)
            labels = read_parquet_records(labels_path)
            labels_by_date = {str(row["date"])[:10]: row for row in labels}
            joined_events: list[dict[str, Any]] = []
            for event in events:
                label = labels_by_date.get(str(event["event_date"])[:10], {})
                joined = dict(event)
                for field in ["trend_regime", "vol_regime", "drawdown_regime", "risk_regime"]:
                    joined[field] = label.get(field) or "unknown"
                joined["regime_snapshot_id"] = regime_snapshot_id
                joined_events.append(joined)
            events = joined_events
            _audit(
                action="event_study_regimes_joined",
                research_plan_id=research_plan_id,
                reason="Joined regime labels to event table.",
                metadata={"regime_snapshot_id": regime_snapshot_id, "event_count": len(events)},
                actor=actor,
                store_root=store,
                new_state_hash=content_hash({"events": events}, sort_lists=False),
            )

        return_column = research_plan["event_definition"].get("params", {}).get("return_column", "adj_close")
        forward_returns, unavailable_counts = calculate_forward_returns(
            events=events,
            rows=rows,
            windows=research_plan["forward_return_windows"],
            research_plan_id=research_plan_id,
            dataset_snapshot_id=research_plan["dataset_snapshot_id"],
            return_column=return_column,
        )
        if not forward_returns:
            raise RuntimeError("Forward returns cannot be calculated")
        if regime_snapshot_id:
            regime_by_event = {(event["symbol"], event["event_date"]): event for event in events}
            enriched_returns: list[dict[str, Any]] = []
            for row in forward_returns:
                event = regime_by_event.get((row["symbol"], row["event_date"]), {})
                enriched = dict(row)
                for field in ["trend_regime", "vol_regime", "drawdown_regime", "risk_regime"]:
                    enriched[field] = event.get(field) or "unknown"
                enriched["regime_snapshot_id"] = regime_snapshot_id
                enriched_returns.append(enriched)
            forward_returns = enriched_returns
        cost_model_snapshot = None
        if cost_model_snapshot_id:
            cost_model_snapshot = load_cost_model_snapshot(cost_model_snapshot_id, store_root=store)
            forward_returns = apply_costs_to_forward_returns(forward_returns, cost_model_snapshot)
            _audit(
                action="event_study_costs_applied",
                research_plan_id=research_plan_id,
                reason="Applied deterministic post-cost return adjustments.",
                metadata={"cost_model_snapshot_id": cost_model_snapshot_id, "forward_return_count": len(forward_returns)},
                actor=actor,
                store_root=store,
                new_state_hash=content_hash({"forward_returns": forward_returns}, sort_lists=False),
            )
        _audit(
            action="forward_returns_calculated",
            research_plan_id=research_plan_id,
            reason="Calculated deterministic forward returns.",
            metadata={
                "forward_return_count": len(forward_returns),
                "unavailable_counts_by_window": unavailable_counts,
                "forward_hash": content_hash({"forward_returns": forward_returns}, sort_lists=False),
            },
            actor=actor,
            store_root=store,
        )

        summary = build_event_study_summary(
            research_plan=research_plan,
            events=events,
            forward_returns=forward_returns,
            unavailable_counts_by_window=unavailable_counts,
            category_by_symbol=category_by_symbol,
            regime_snapshot_id=regime_snapshot_id,
            cost_model_snapshot_id=cost_model_snapshot_id,
        )
        manifest = write_event_study_evidence_package(
            research_plan=research_plan,
            dataset_snapshot=dataset_snapshot,
            events=events,
            forward_returns=forward_returns,
            summary=summary,
            created_by=actor,
            store_root=store,
            allow_json_fallback=allow_json_fallback,
            regime_snapshot=regime_snapshot,
            cost_model_snapshot=cost_model_snapshot,
        )
        _audit(
            action="evidence_package_written",
            research_plan_id=research_plan_id,
            reason="Wrote immutable event-study evidence package.",
            metadata={"evidence_package_id": manifest["evidence_package_id"]},
            actor=actor,
            store_root=store,
            new_state_hash=manifest["manifest_hash"],
        )
        registry_row = append_evidence_registry_entry(manifest, store_root=store)
        _audit(
            action="event_study_completed_with_regime_costs" if regime_snapshot_id or cost_model_snapshot_id else "event_study_completed",
            research_plan_id=research_plan_id,
            reason="Completed deterministic event study with regime/cost conditioning." if regime_snapshot_id or cost_model_snapshot_id else "Completed deterministic event study.",
            metadata={"evidence_package_id": manifest["evidence_package_id"], "registry_row": registry_row},
            actor=actor,
            store_root=store,
            new_state_hash=manifest["manifest_hash"],
        )
        return {"status": "completed", "evidence_manifest": manifest, "summary": summary, "registry_row": registry_row}
    except Exception as exc:
        _audit(
            action="event_study_failed",
            research_plan_id=research_plan_id,
            reason=str(exc),
            metadata={"research_plan_id": research_plan_id, "error": str(exc)},
            actor=actor,
            store_root=store,
        )
        raise
