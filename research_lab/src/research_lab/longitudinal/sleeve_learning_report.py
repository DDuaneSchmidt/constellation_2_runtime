from __future__ import annotations

from pathlib import Path
from statistics import mean
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import validate_contract
from research_lab.longitudinal.candidate_run import longitudinal_run_dir
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.parquet_io import read_parquet_records
from research_lab.storage.paths import ensure_store_layout


def _registry(name: str, store: Path) -> Path:
    return store / "registries" / name


def _recommend(total_candidates: int, ranking_status: str) -> str:
    if total_candidates < 30:
        return "collect_more_candidates"
    if ranking_status == "useful":
        return "continue_research"
    if ranking_status == "weak":
        return "revise_ranking_policy"
    return "continue_research"


def build_sleeve_learning_report(sleeve_id: str, sleeve_version_id: str, *, store_root: Path | None = None, created_at: str | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    run_rows = [row for row in read_jsonl(_registry("longitudinal_candidate_runs.jsonl", store)) if row["sleeve_id"] == sleeve_id and row["sleeve_version_id"] == sleeve_version_id]
    ranking_rows = read_jsonl(_registry("ranking_quality_reports.jsonl", store))
    run_ids = [row["longitudinal_run_id"] for row in run_rows]
    total_batches = 0
    total_candidates = 0
    measured_candidates = 0
    zero_batches = 0
    all_outcomes: list[dict[str, Any]] = []
    latest_status = "insufficient_sample"
    for run_id in run_ids:
        root = longitudinal_run_dir(run_id, store_root=store)
        batch_index = read_parquet_records(root / "candidate_batch_index.parquet")
        outcome_index = read_parquet_records(root / "outcome_index.parquet")
        total_batches += len(batch_index)
        total_candidates += sum(int(row["candidate_count"]) for row in batch_index)
        measured_candidates += len({row["candidate_id"] for row in outcome_index if row.get("outcome_status") == "measured"})
        zero_batches += sum(1 for row in batch_index if int(row["candidate_count"]) == 0)
        all_outcomes.extend([row for row in outcome_index if row.get("outcome_status") == "measured"])
        rq = next((row for row in ranking_rows if row["longitudinal_run_id"] == run_id), None)
        if rq:
            latest_status = rq["overall_ranking_quality_status"]
    by_window: dict[str, list[float]] = {}
    by_risk: dict[str, list[float]] = {}
    for row in all_outcomes:
        by_window.setdefault(str(row["outcome_window"]), []).append(float(row["post_cost_return"]))
        by_risk.setdefault(str(row.get("risk_regime") or "unknown"), []).append(float(row["post_cost_return"]))
    window_means = {key: mean(values) for key, values in by_window.items() if values}
    risk_means = {key: mean(values) for key, values in by_risk.items() if values}
    recommended = _recommend(total_candidates, latest_status)
    report = {
        "sleeve_id": sleeve_id,
        "sleeve_version_id": sleeve_version_id,
        "longitudinal_runs": run_ids,
        "total_candidate_batches": total_batches,
        "total_candidates": total_candidates,
        "measured_candidates": measured_candidates,
        "zero_candidate_batches": zero_batches,
        "candidate_frequency": run_rows[-1]["frequency"] if run_rows else "unknown",
        "best_outcome_window": max(window_means, key=window_means.get) if window_means else None,
        "worst_outcome_window": min(window_means, key=window_means.get) if window_means else None,
        "best_risk_regime": max(risk_means, key=risk_means.get) if risk_means else None,
        "worst_risk_regime": min(risk_means, key=risk_means.get) if risk_means else None,
        "ranking_quality_status": latest_status,
        "operator_decision_coverage": 0.0,
        "current_learning_status": "needs_more_observations" if total_candidates < 30 else "learning_sample_available",
        "recommended_next_action": recommended,
        "created_at": created_at or utc_now_iso(),
        "schema_version": "sleeve_learning_report.v1",
        "compliance_label": "Sleeve learning is historical research/observation. It is not live achieved portfolio performance or investment advice.",
    }
    report["content_hash"] = content_hash(report, exclude={"created_at"}, sort_lists=True)
    report["sleeve_learning_report_id"] = f"slrn_{sleeve_id}_{short_hash(report['content_hash'], 10)}"
    report["content_hash"] = content_hash(report, exclude={"created_at"}, sort_lists=True)
    validate_contract("sleeve_learning_report", report)
    return report


def sleeve_learning_markdown(report: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Sleeve Learning Report",
            "",
            report["compliance_label"],
            "",
            f"- total_candidate_batches: {report['total_candidate_batches']}",
            f"- total_candidates: {report['total_candidates']}",
            f"- measured_candidates: {report['measured_candidates']}",
            f"- zero_candidate_batches: {report['zero_candidate_batches']}",
            f"- ranking_quality_status: {report['ranking_quality_status']}",
            f"- recommended_next_action: {report['recommended_next_action']}",
        ]
    ) + "\n"


def write_sleeve_learning_report(sleeve_id: str, sleeve_version_id: str, *, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    report = build_sleeve_learning_report(sleeve_id, sleeve_version_id, store_root=store)
    run_id = report["longitudinal_runs"][-1] if report["longitudinal_runs"] else "none"
    root = longitudinal_run_dir(run_id, store_root=store) if run_id != "none" else store / "longitudinal_runs"
    if (root / "sleeve_learning_report.json").exists():
        return read_json(root / "sleeve_learning_report.json")
    write_json(root / "sleeve_learning_report.json", report, overwrite=False)
    (root / "sleeve_learning_report.md").write_text(sleeve_learning_markdown(report), encoding="utf-8")
    row = {
        "sleeve_learning_report_id": report["sleeve_learning_report_id"],
        "sleeve_id": sleeve_id,
        "sleeve_version_id": sleeve_version_id,
        "ranking_quality_status": report["ranking_quality_status"],
        "recommended_next_action": report["recommended_next_action"],
        "content_hash": report["content_hash"],
        "created_at": report["created_at"],
        "schema_version": report["schema_version"],
    }
    append_jsonl(_registry("sleeve_learning_reports.jsonl", store), row)
    write_audit_event(actor=actor, entity_type="sleeve", entity_id=sleeve_id, action="sleeve_learning_report_written", new_state_hash=report["content_hash"], reason="Wrote deterministic sleeve learning report.", metadata={"registry_row": row}, store_root=store)
    return report
