from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.candidates.candidate_batch import CANDIDATE_GENERATOR_NAME, CANDIDATE_GENERATOR_VERSION, build_candidate_batch
from research_lab.candidates.candidate_generator import generate_drop_reversion_candidates
from research_lab.candidates.candidate_registry import (
    candidate_batch_dir,
    load_candidates,
    store_candidate_batch,
)
from research_lab.candidates.candidate_scoring import RANKING_POLICY_VERSION
from research_lab.contracts.schemas import validate_contract
from research_lab.costs.cost_model_registry import cost_model_snapshot_path
from research_lab.datasets.dataset_registry import dataset_snapshot_path
from research_lab.evidence.evidence_registry import evidence_manifest_path, load_evidence_package_manifest
from research_lab.longitudinal.batch_scheduler import scheduled_as_of_dates
from research_lab.outcomes.measurement_runner import run_candidate_outcome_measurement
from research_lab.outcomes.outcome_registry import load_attribution_report, load_outcomes
from research_lab.regimes.regime_registry import regime_snapshot_path
from research_lab.sleeves.sleeve_registry import load_sleeve_version
from research_lab.storage.duckdb_query import load_dataset_snapshot_rows
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, read_jsonl, write_json
from research_lab.storage.parquet_io import file_sha256, read_parquet_records, write_parquet_records
from research_lab.storage.paths import ensure_store_layout


def longitudinal_run_dir(longitudinal_run_id: str, *, store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "longitudinal_runs" / longitudinal_run_id


def validate_longitudinal_candidate_run(run: dict[str, Any]) -> None:
    validate_contract("longitudinal_candidate_run", run)


def build_longitudinal_candidate_run(
    *,
    sleeve_id: str,
    sleeve_version_id: str,
    hypothesis_id: str,
    source_evidence_package_id: str,
    dataset_snapshot_id: str,
    regime_snapshot_id: str,
    cost_model_snapshot_id: str,
    start_date: str,
    end_date: str,
    frequency: str = "weekly",
    threshold: float = -0.02,
    outcome_windows: list[int] | None = None,
    created_by: str = "Aegis",
    created_at: str | None = None,
) -> dict[str, Any]:
    payload = {
        "sleeve_id": sleeve_id,
        "sleeve_version_id": sleeve_version_id,
        "hypothesis_id": hypothesis_id,
        "source_evidence_package_id": source_evidence_package_id,
        "dataset_snapshot_id": dataset_snapshot_id,
        "regime_snapshot_id": regime_snapshot_id,
        "cost_model_snapshot_id": cost_model_snapshot_id,
        "start_date": start_date,
        "end_date": end_date,
        "frequency": frequency,
        "candidate_generator_name": CANDIDATE_GENERATOR_NAME,
        "candidate_generator_version": CANDIDATE_GENERATOR_VERSION,
        "ranking_policy_version": RANKING_POLICY_VERSION,
        "threshold": float(threshold),
        "outcome_windows": sorted({int(window) for window in (outcome_windows or [1, 2, 5, 10, 20])}),
        "created_at": created_at or utc_now_iso(),
        "created_by": created_by,
        "schema_version": "longitudinal_candidate_run.v1",
        "compliance_label": "Historical candidate accumulation is offline research/observation. It is not live achieved portfolio performance or investment advice.",
    }
    seed_hash = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    payload["longitudinal_run_id"] = f"lcr_{sleeve_id}_{start_date.replace('-', '')}_{end_date.replace('-', '')}_{frequency}_{short_hash(seed_hash, 10)}"
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    validate_longitudinal_candidate_run(payload)
    return payload


def _registry(name: str, store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "registries" / name


def _candidate_batch_id_for(run: dict[str, Any], as_of_date: str, symbols: list[str]) -> str:
    return build_candidate_batch(
        hypothesis_id=run["hypothesis_id"],
        source_evidence_package_id=run["source_evidence_package_id"],
        dataset_snapshot_id=run["dataset_snapshot_id"],
        regime_snapshot_id=run["regime_snapshot_id"],
        cost_model_snapshot_id=run["cost_model_snapshot_id"],
        symbols=symbols,
        as_of_date=as_of_date,
        threshold=run["threshold"],
        created_at=run["created_at"],
        created_by=run["created_by"],
        ranking_policy_version=run["ranking_policy_version"],
    )["candidate_batch_id"]


def _write_run_artifacts(
    *,
    run: dict[str, Any],
    candidate_index: list[dict[str, Any]],
    outcome_index: list[dict[str, Any]],
    store: Path,
    allow_json_fallback: bool,
) -> dict[str, Any]:
    root = longitudinal_run_dir(run["longitudinal_run_id"], store_root=store)
    root.mkdir(parents=True, exist_ok=False)
    write_json(root / "longitudinal_run.json", run, overwrite=False)
    write_parquet_records(root / "candidate_batch_index.parquet", candidate_index, allow_json_fallback=allow_json_fallback)
    write_parquet_records(root / "outcome_index.parquet", outcome_index, allow_json_fallback=allow_json_fallback)
    return {
        "candidate_batch_index_hash": file_sha256(root / "candidate_batch_index.parquet"),
        "outcome_index_hash": file_sha256(root / "outcome_index.parquet"),
    }


def _candidate_index_row(run_id: str, batch_id: str, as_of_date: str, generation_status: str, outcome_status: str, store: Path) -> dict[str, Any]:
    candidates = load_candidates(batch_id, store_root=store) if candidate_batch_dir(batch_id, store_root=store).exists() else []
    report = load_attribution_report(batch_id, store_root=store) if (candidate_batch_dir(batch_id, store_root=store) / "attribution_report.json").exists() else {}
    top = candidates[0] if candidates else {}
    return {
        "longitudinal_run_id": run_id,
        "candidate_batch_id": batch_id,
        "as_of_date": as_of_date,
        "candidate_count": len(candidates),
        "measured_candidate_count": int(report.get("measured_candidate_count", 0)),
        "unavailable_candidate_count": int(report.get("unavailable_candidate_count", 0)),
        "top_candidate_id": top.get("candidate_id", ""),
        "top_candidate_symbol": top.get("symbol", ""),
        "top_candidate_score": top.get("ranking_score"),
        "generation_status": generation_status,
        "outcome_status": outcome_status,
    }


def _outcome_index_rows(run_id: str, batch_id: str, store: Path) -> list[dict[str, Any]]:
    candidates = {row["candidate_id"]: row for row in load_candidates(batch_id, store_root=store)}
    if not (candidate_batch_dir(batch_id, store_root=store) / "outcomes.parquet").exists():
        return []
    rows: list[dict[str, Any]] = []
    for outcome in load_outcomes_safe(batch_id, store):
        candidate = candidates.get(outcome["candidate_id"], {})
        rows.append(
            {
                "longitudinal_run_id": run_id,
                "candidate_batch_id": batch_id,
                "candidate_id": outcome["candidate_id"],
                "symbol": outcome["symbol"],
                "as_of_date": outcome["as_of_date"],
                "ranking_score": candidate.get("ranking_score"),
                "ranking_bucket": candidate.get("ranking_bucket", ""),
                "risk_regime": candidate.get("risk_regime", ""),
                "decision_status": candidate.get("derived_candidate_status", candidate.get("candidate_status", "generated_no_decision")),
                "outcome_window": outcome["outcome_window"],
                "post_cost_return": outcome["post_cost_return"],
                "benchmark_return": outcome["benchmark_return"],
                "excess_return": outcome["excess_return"],
                "outcome_status": outcome["outcome_status"],
            }
        )
    return rows


def load_outcomes_safe(batch_id: str, store: Path) -> list[dict[str, Any]]:
    return read_parquet_records(candidate_batch_dir(batch_id, store_root=store) / "outcomes.parquet")


def run_longitudinal_candidate_study(
    *,
    sleeve_id: str,
    sleeve_version_id: str,
    source_evidence_package_id: str,
    dataset_snapshot_id: str,
    regime_snapshot_id: str,
    cost_model_snapshot_id: str,
    start: str,
    end: str,
    frequency: str,
    threshold: float,
    outcome_windows: list[int],
    store_root: Path | None = None,
    actor: str = "Aegis",
    allow_json_fallback: bool = False,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    try:
        version = load_sleeve_version(sleeve_id, sleeve_version_id, store_root=store)
        for path, label in [
            (evidence_manifest_path(source_evidence_package_id, store_root=store), "source evidence"),
            (dataset_snapshot_path(dataset_snapshot_id, store_root=store), "dataset snapshot"),
            (regime_snapshot_path(regime_snapshot_id, store_root=store), "regime snapshot"),
            (cost_model_snapshot_path(cost_model_snapshot_id, store_root=store), "cost model snapshot"),
        ]:
            if not path.exists():
                raise RuntimeError(f"{label} missing: {path}")
        evidence = load_evidence_package_manifest(source_evidence_package_id, store_root=store)
        rows = load_dataset_snapshot_rows(dataset_snapshot_id, store_root=store)
        symbols = sorted({str(row["symbol"]).upper() for row in rows})
        dates = scheduled_as_of_dates(rows, start=start, end=end, frequency=frequency)
        if not dates:
            raise RuntimeError("No scheduled as-of dates in dataset range")
        run = build_longitudinal_candidate_run(
            sleeve_id=sleeve_id,
            sleeve_version_id=sleeve_version_id,
            hypothesis_id=version["hypothesis_id"],
            source_evidence_package_id=source_evidence_package_id,
            dataset_snapshot_id=dataset_snapshot_id,
            regime_snapshot_id=regime_snapshot_id,
            cost_model_snapshot_id=cost_model_snapshot_id,
            start_date=start,
            end_date=end,
            frequency=frequency,
            threshold=threshold,
            outcome_windows=outcome_windows,
            created_by=actor,
        )
        run_root = longitudinal_run_dir(run["longitudinal_run_id"], store_root=store)
        if run_root.exists():
            raise FileExistsError(f"Refusing to overwrite longitudinal run: {run_root}")
        write_audit_event(actor=actor, entity_type="longitudinal_candidate_run", entity_id=run["longitudinal_run_id"], action="longitudinal_candidate_run_started", reason="Started offline longitudinal candidate study.", metadata={"scheduled_dates": len(dates)}, store_root=store)
        candidate_index: list[dict[str, Any]] = []
        outcome_index: list[dict[str, Any]] = []
        for as_of_date in dates:
            batch_id = _candidate_batch_id_for(run, as_of_date, symbols)
            generation_status = "created"
            if candidate_batch_dir(batch_id, store_root=store).exists():
                generation_status = "duplicate_skipped"
            else:
                generated = generate_drop_reversion_candidates(
                    source_evidence_package_id=source_evidence_package_id,
                    dataset_snapshot_id=dataset_snapshot_id,
                    regime_snapshot_id=regime_snapshot_id,
                    cost_model_snapshot_id=cost_model_snapshot_id,
                    as_of_date=as_of_date,
                    threshold=threshold,
                    created_by=actor,
                    store_root=store,
                )
                store_candidate_batch(candidate_batch=generated["candidate_batch"], candidates=generated["candidates"], generation_summary=generated["generation_summary"], store_root=store, actor=actor, allow_json_fallback=allow_json_fallback)
                write_audit_event(actor=actor, entity_type="longitudinal_candidate_run", entity_id=run["longitudinal_run_id"], action="longitudinal_candidate_batch_generated", reason="Generated scheduled historical candidate batch.", metadata={"candidate_batch_id": batch_id, "as_of_date": as_of_date}, store_root=store)
            outcome_status = "created"
            if (candidate_batch_dir(batch_id, store_root=store) / "outcomes.parquet").exists():
                outcome_status = "duplicate_skipped"
            else:
                run_candidate_outcome_measurement(candidate_batch_id=batch_id, dataset_snapshot_id=dataset_snapshot_id, cost_model_snapshot_id=cost_model_snapshot_id, benchmark_symbol="SPY", windows=outcome_windows, store_root=store, actor=actor, allow_json_fallback=allow_json_fallback)
                write_audit_event(actor=actor, entity_type="longitudinal_candidate_run", entity_id=run["longitudinal_run_id"], action="longitudinal_candidate_outcomes_measured", reason="Measured scheduled historical candidate outcomes.", metadata={"candidate_batch_id": batch_id, "as_of_date": as_of_date}, store_root=store)
            candidate_index.append(_candidate_index_row(run["longitudinal_run_id"], batch_id, as_of_date, generation_status, outcome_status, store))
            outcome_index.extend(_outcome_index_rows(run["longitudinal_run_id"], batch_id, store))
        hashes = _write_run_artifacts(run=run, candidate_index=candidate_index, outcome_index=outcome_index, store=store, allow_json_fallback=allow_json_fallback)
        row = {
            "longitudinal_run_id": run["longitudinal_run_id"],
            "sleeve_id": sleeve_id,
            "sleeve_version_id": sleeve_version_id,
            "start_date": start,
            "end_date": end,
            "frequency": frequency,
            "candidate_batch_count": len(candidate_index),
            "candidate_count": sum(int(row["candidate_count"]) for row in candidate_index),
            "content_hash": run["content_hash"],
            **hashes,
            "created_at": run["created_at"],
            "schema_version": run["schema_version"],
        }
        append_jsonl(_registry("longitudinal_candidate_runs.jsonl", store), row)
        from research_lab.longitudinal.ranking_quality import write_ranking_quality_report
        from research_lab.longitudinal.sleeve_learning_report import write_sleeve_learning_report

        ranking = write_ranking_quality_report(run["longitudinal_run_id"], store_root=store, actor=actor)
        learning = write_sleeve_learning_report(sleeve_id=sleeve_id, sleeve_version_id=sleeve_version_id, store_root=store, actor=actor)
        write_audit_event(actor=actor, entity_type="longitudinal_candidate_run", entity_id=run["longitudinal_run_id"], action="longitudinal_candidate_run_completed", new_state_hash=run["content_hash"], reason="Completed offline longitudinal candidate study.", metadata={"registry_row": row}, store_root=store)
        return {"longitudinal_run": run, "registry_row": row, "ranking_quality_report": ranking, "sleeve_learning_report": learning}
    except Exception as exc:
        write_audit_event(actor=actor, entity_type="longitudinal_candidate_run", entity_id=sleeve_id, action="longitudinal_candidate_run_failed", reason=str(exc), metadata={}, store_root=store)
        raise
