from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.challengers.challenger_evidence import (
    _measured_rows_for_run,
    _metric_summary,
    load_challenger_evidence_batch,
)
from research_lab.challengers.challenger_track import load_challenger_research_track
from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


GENERATED_AT = "1970-01-01T00:00:00Z"
RESEARCH_LABEL = "RESEARCH_ONLY"
SCHEMA_VERSION = "challenger_comparison_report.v1"


def _registry_path(store: Path) -> Path:
    return store / "registries" / "challenger_comparison_reports.jsonl"


def _report_dir(store: Path) -> Path:
    return store / "challenger_comparison_reports"


def _report_path(store: Path, report_id: str) -> Path:
    return _report_dir(store) / f"{report_id}.json"


def _json_if_exists(path: Path) -> dict[str, Any]:
    return read_json(path) if path.exists() else {}


def _outcome_source_summary(store: Path, longitudinal_run_id: str) -> dict[str, Any]:
    rows = _measured_rows_for_run(store, longitudinal_run_id)
    return _metric_summary(rows)


def _rank_usefulness(store: Path, longitudinal_run_id: str) -> str:
    path = store / "longitudinal_runs" / longitudinal_run_id / "ranking_quality_report.json"
    payload = _json_if_exists(path)
    return str(payload.get("overall_ranking_quality_status") or "unknown")


def _incumbent_summary(store: Path, track: dict[str, Any]) -> dict[str, Any]:
    req = track.get("evidence_requirements") or {}
    longitudinal_run_id = str(req.get("longitudinal_run_id") or "")
    metrics = _outcome_source_summary(store, longitudinal_run_id)
    drift = _json_if_exists(store / "stability_reports" / "expectancy_drift" / f"{req.get('expectancy_drift_report_id')}.json")
    fragility = _json_if_exists(store / "stability_reports" / "regime_fragility" / f"{req.get('regime_fragility_report_id')}.json")
    stability = _json_if_exists(store / "stability_reports" / "sleeve_stability" / f"{req.get('sleeve_stability_report_id')}.json")
    return {
        "entity_id": track["incumbent_sleeve_id"],
        "variant_name": "incumbent",
        "event_study_evidence_id": str(req.get("event_study_evidence_package_id") or ""),
        "backtest_evidence_id": str(req.get("backtest_evidence_package_id") or ""),
        "longitudinal_run_id": longitudinal_run_id,
        "expectancy_drift_report_id": str(req.get("expectancy_drift_report_id") or ""),
        "regime_fragility_report_id": str(req.get("regime_fragility_report_id") or ""),
        "sleeve_stability_report_id": str(req.get("sleeve_stability_report_id") or ""),
        "post_cost_expectancy": metrics.get("mean_post_cost_return"),
        "win_rate": metrics.get("win_rate"),
        "median_return": metrics.get("median_post_cost_return"),
        "downside_tail_metric": metrics.get("downside_tail_metric"),
        "benchmark_relative_result": metrics.get("mean_excess_return"),
        "rank_usefulness": _rank_usefulness(store, longitudinal_run_id),
        "drift_status": str(drift.get("drift_status") or "unknown"),
        "regime_fragility_status": str(fragility.get("fragility_status") or "unknown"),
        "overall_stability_status": str(stability.get("overall_stability_status") or "unknown"),
        "evidence_quality": "historical_incumbent",
        "observation_count": metrics.get("measured_candidate_count"),
    }


def _artifact_exists(store: Path, item: dict[str, Any], key: str) -> bool:
    value = str(item.get(key) or "")
    if not value:
        return False
    paths = {
        "challenger_variant_id": store / "challenger_variants" / f"{value}.json",
        "event_study_evidence_id": store / "event_studies" / f"{value}.json",
        "backtest_evidence_id": store / "backtests" / f"{value}.json",
        "longitudinal_run_id": store / "longitudinal_runs" / value / "longitudinal_run.json",
        "expectancy_drift_report_id": store / "stability_reports" / "expectancy_drift" / f"{value}.json",
        "regime_fragility_report_id": store / "stability_reports" / "regime_fragility" / f"{value}.json",
        "sleeve_stability_report_id": store / "stability_reports" / "sleeve_stability" / f"{value}.json",
    }
    return paths[key].exists()


def _challenger_summary(store: Path, item: dict[str, Any]) -> dict[str, Any]:
    metrics = item.get("derived_metrics") or {}
    required = [
        "challenger_variant_id",
        "event_study_evidence_id",
        "backtest_evidence_id",
        "longitudinal_run_id",
        "expectancy_drift_report_id",
        "regime_fragility_report_id",
        "sleeve_stability_report_id",
    ]
    missing = [field for field in required if item.get("status") != "blocked" and not _artifact_exists(store, item, field)]
    drift = _json_if_exists(store / "stability_reports" / "expectancy_drift" / f"{item.get('expectancy_drift_report_id')}.json")
    fragility = _json_if_exists(store / "stability_reports" / "regime_fragility" / f"{item.get('regime_fragility_report_id')}.json")
    stability = _json_if_exists(store / "stability_reports" / "sleeve_stability" / f"{item.get('sleeve_stability_report_id')}.json")
    return {
        "challenger_hypothesis_id": item.get("challenger_hypothesis_id"),
        "challenger_variant_id": item.get("challenger_variant_id"),
        "variant_name": item.get("variant_name"),
        "deterministic_rule_delta": item.get("deterministic_rule_delta"),
        "status": item.get("status"),
        "evidence_quality": item.get("evidence_quality"),
        "event_study_evidence_id": item.get("event_study_evidence_id"),
        "backtest_evidence_id": item.get("backtest_evidence_id"),
        "longitudinal_run_id": item.get("longitudinal_run_id"),
        "expectancy_drift_report_id": item.get("expectancy_drift_report_id"),
        "regime_fragility_report_id": item.get("regime_fragility_report_id"),
        "sleeve_stability_report_id": item.get("sleeve_stability_report_id"),
        "post_cost_expectancy": metrics.get("mean_post_cost_return"),
        "win_rate": metrics.get("win_rate"),
        "median_return": metrics.get("median_post_cost_return"),
        "downside_tail_metric": metrics.get("downside_tail_metric"),
        "benchmark_relative_result": metrics.get("mean_excess_return"),
        "rank_usefulness": "rank_filtered" if item.get("variant_name") == "rank_threshold_variant" else "not_applicable",
        "drift_status": str(drift.get("drift_status") or "missing"),
        "regime_fragility_status": str(fragility.get("fragility_status") or "missing"),
        "stability_status": str(stability.get("overall_stability_status") or "missing"),
        "observation_count": metrics.get("measured_candidate_count"),
        "exclusion_flags": item.get("exclusion_flags") or [],
        "missing_evidence_flags": missing,
        "failure_reason": item.get("failure_reason") or "",
        "evidence_completeness": item.get("evidence_completeness") or {"complete": not missing, "missing_fields": missing},
        "artifact_lineage": item.get("artifact_lineage") or {},
    }


def _exclusion_for_challenger(summary: dict[str, Any]) -> dict[str, Any]:
    reasons: list[str] = []
    if summary["status"] == "blocked":
        reasons.append("blocked")
    if summary.get("evidence_quality") == "insufficient":
        reasons.append("insufficient observations")
    if summary.get("post_cost_expectancy") is not None and float(summary["post_cost_expectancy"]) < 0:
        reasons.append("negative post-cost expectancy")
    if summary.get("failure_reason") == "cost_sensitivity_requires_supported_cost_model_snapshot":
        reasons.append("unsupported cost model sensitivity")
    if summary.get("evidence_quality") == "blocked":
        reasons.append("missing required evidence")
    if summary.get("missing_evidence_flags"):
        reasons.append("missing required evidence")
    if summary.get("drift_status") == "degrading":
        reasons.append("degrading expectancy drift")
    if summary.get("regime_fragility_status") == "fragile":
        reasons.append("high regime fragility")
    return {
        "entity_id": summary.get("challenger_hypothesis_id"),
        "variant_name": summary.get("variant_name"),
        "excluded": bool(reasons),
        "reasons": reasons,
    }


def _comparison_row(entity_type: str, entity_id: str, variant_name: str, summary: dict[str, Any], incumbent: dict[str, Any], exclusion: dict[str, Any]) -> dict[str, Any]:
    expectancy = summary.get("post_cost_expectancy")
    win_rate = summary.get("win_rate")
    incumbent_expectancy = incumbent.get("post_cost_expectancy")
    incumbent_win = incumbent.get("win_rate")
    return {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "variant_name": variant_name,
        "evidence_quality": summary.get("evidence_quality"),
        "post_cost_expectancy": expectancy,
        "expectancy_delta_vs_incumbent": (expectancy - incumbent_expectancy) if expectancy is not None and incumbent_expectancy is not None else None,
        "win_rate": win_rate,
        "win_rate_delta_vs_incumbent": (win_rate - incumbent_win) if win_rate is not None and incumbent_win is not None else None,
        "median_return": summary.get("median_return"),
        "downside_tail_metric": summary.get("downside_tail_metric"),
        "benchmark_relative_result": summary.get("benchmark_relative_result"),
        "drift_status": summary.get("drift_status"),
        "regime_fragility_status": summary.get("regime_fragility_status"),
        "stability_status": summary.get("overall_stability_status") or summary.get("stability_status"),
        "observation_count": summary.get("observation_count"),
        "exclusion_status": "excluded" if exclusion.get("excluded") else "included_for_human_review",
        "evidence_completeness_score": (summary.get("evidence_completeness") or {}).get("completeness_score"),
        "governance_notes": "Research review only. NOT_AN_OPTIMIZER. No trading authorization.",
    }


def _rank_order(challenger_summaries: list[dict[str, Any]], exclusions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    excluded = {row["entity_id"] for row in exclusions if row["excluded"]}
    active = [
        row
        for row in challenger_summaries
        if row.get("challenger_hypothesis_id") not in excluded
        and row.get("evidence_quality") not in {"blocked", "insufficient"}
        and row.get("status") == "generated"
    ]
    fragility_rank = {"robust": 0, "watch": 1, "fragile": 3, "missing": 4, "unknown": 4}
    drift_rank = {"improving": 0, "stable": 1, "watch": 2, "degrading": 4, "missing": 5, "unknown": 5}
    ordered = sorted(
        active,
        key=lambda row: (
            drift_rank.get(str(row.get("drift_status")), 5),
            fragility_rank.get(str(row.get("regime_fragility_status")), 4),
            -(float(row.get("post_cost_expectancy") or 0.0)),
            -(float(row.get("downside_tail_metric") or -999.0)),
            -(int(row.get("observation_count") or 0)),
            str(row.get("challenger_hypothesis_id") or ""),
        ),
    )
    return [
        {
            "rank": idx + 1,
            "challenger_hypothesis_id": row["challenger_hypothesis_id"],
            "variant_name": row["variant_name"],
            "ranking_method_label": "RULE_BASED_RESEARCH_REVIEW_ORDER",
            "optimizer_statement": "NOT_AN_OPTIMIZER",
        }
        for idx, row in enumerate(ordered)
    ]


def _evidence_sufficiency(challenger_summaries: list[dict[str, Any]], missing_core: list[str]) -> str:
    usable = [row for row in challenger_summaries if row.get("status") == "generated" and row.get("evidence_quality") in {"strong", "moderate"}]
    generated = [row for row in challenger_summaries if row.get("status") == "generated"]
    if not usable:
        return "blocked" if not generated else "insufficient"
    if len(usable) >= 2 and not missing_core:
        return "comparison_ready"
    return "partially_ready"


def _recommended_action(sufficiency: str, challenger_summaries: list[dict[str, Any]]) -> str:
    if sufficiency == "comparison_ready":
        return "prepare_human_review"
    if sufficiency in {"partially_ready", "insufficient"}:
        return "collect_more_observations"
    if all(row.get("status") == "blocked" for row in challenger_summaries):
        return "maintain_watch"
    return "investigate_blockers"


def _quality_counts(challenger_summaries: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(str(row.get("evidence_quality") or "unknown") for row in challenger_summaries)
    return {key: counts.get(key, 0) for key in ["strong", "moderate", "weak", "insufficient", "blocked"]}


def build_challenger_comparison_report(
    *,
    challenger_evidence_batch_id: str,
    challenger_track_id: str | None = None,
    store_root: Path | None = None,
    generated_at: str = GENERATED_AT,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    batch = load_challenger_evidence_batch(challenger_evidence_batch_id, store_root=store)
    track_id = challenger_track_id or str(batch.get("challenger_track_id") or "")
    if track_id != batch.get("challenger_track_id"):
        raise RuntimeError("challenger track id does not match evidence batch")
    track = load_challenger_research_track(track_id, store_root=store)
    incumbent = _incumbent_summary(store, track)
    challenger_summaries = [_challenger_summary(store, item) for item in batch.get("challenger_evidence_items") or []]
    exclusions = [_exclusion_for_challenger(row) for row in challenger_summaries]
    incumbent_exclusion = {
        "entity_id": incumbent["entity_id"],
        "variant_name": "incumbent",
        "excluded": False,
        "reasons": ["degrading expectancy drift"] if incumbent["drift_status"] == "degrading" else [],
    }
    table = [_comparison_row("incumbent", incumbent["entity_id"], "incumbent", incumbent, incumbent, incumbent_exclusion)]
    for summary, exclusion in zip(challenger_summaries, exclusions, strict=True):
        entity_type = "blocked_challenger" if summary["status"] == "blocked" else "challenger"
        table.append(_comparison_row(entity_type, str(summary["challenger_hypothesis_id"]), str(summary["variant_name"]), summary, incumbent, exclusion))
    rank_order = _rank_order(challenger_summaries, exclusions)
    missing_core = [str(ref.get("artifact_type")) for ref in batch.get("audit_refs", {}).get("required_source_missing") or []]
    sufficiency = _evidence_sufficiency(challenger_summaries, missing_core)
    recommended = _recommended_action(sufficiency, challenger_summaries)
    payload = {
        "challenger_comparison_report_id": "",
        "challenger_track_id": track_id,
        "challenger_evidence_batch_id": challenger_evidence_batch_id,
        "incumbent_sleeve_id": batch["incumbent_sleeve_id"],
        "incumbent_sleeve_version_id": batch["incumbent_sleeve_version_id"],
        "generated_at": generated_at,
        "schema_version": SCHEMA_VERSION,
        "research_label": RESEARCH_LABEL,
        "source_incumbent_evidence_ids": {
            "event_study_evidence_id": incumbent["event_study_evidence_id"],
            "backtest_evidence_id": incumbent["backtest_evidence_id"],
            "longitudinal_run_id": incumbent["longitudinal_run_id"],
            "expectancy_drift_report_id": incumbent["expectancy_drift_report_id"],
            "regime_fragility_report_id": incumbent["regime_fragility_report_id"],
            "sleeve_stability_report_id": incumbent["sleeve_stability_report_id"],
        },
        "source_challenger_evidence_ids": [
            {
                "challenger_hypothesis_id": item.get("challenger_hypothesis_id"),
                "challenger_variant_id": item.get("challenger_variant_id"),
                "generated_evidence_ids": item.get("generated_evidence_ids") or [],
                "artifact_lineage": item.get("artifact_lineage") or {},
                "evidence_completeness": item.get("evidence_completeness") or {},
            }
            for item in batch.get("challenger_evidence_items") or []
        ],
        "blocked_hypotheses": batch.get("blocked_hypotheses") or [],
        "comparison_config": {
            "ranking_method_label": "RULE_BASED_RESEARCH_REVIEW_ORDER",
            "optimizer_statement": "NOT_AN_OPTIMIZER",
            "winner_policy": "no_winner_declared_human_review_only",
            "minimum_usable_challengers_for_comparison_ready": 2,
            "projection_shortcuts_allowed": False,
            "requires_materialized_challenger_evidence_chain": True,
        },
        "incumbent_summary": incumbent,
        "challenger_summaries": challenger_summaries,
        "comparison_table": table,
        "exclusion_results": [incumbent_exclusion] + exclusions,
        "evidence_sufficiency": sufficiency,
        "deterministic_rank_order": rank_order,
        "governance_summary": {
            "no_challenger_is_promoted": True,
            "incumbent_sleeve_is_not_retired": True,
            "incumbent_sleeve_is_not_modified": True,
            "paper_trial_is_not_modified": True,
            "candidate_ledger_is_not_modified": True,
            "report_is_advisory_only": True,
            "human_review_required_for_future_lifecycle_action": True,
            "no_trading_authorization_created": True,
            "not_an_optimizer": True,
            "no_capital_allocation": True,
            "comparison_uses_materialized_challenger_artifacts_only": True,
        },
        "recommended_next_action": recommended,
        "audit_refs": {
            "challenger_track_id": track_id,
            "challenger_evidence_batch_id": challenger_evidence_batch_id,
            "source_batch_content_hash": batch.get("content_hash"),
            "source_track_content_hash": track.get("content_hash"),
            "append_only_registry": "challenger_comparison_reports.jsonl",
            "audit_action": "challenger_comparison_report_created",
        },
        "content_hash": "",
    }
    seed = content_hash(payload, exclude={"challenger_comparison_report_id", "content_hash", "generated_at"}, sort_lists=True)
    payload["challenger_comparison_report_id"] = f"chcmp_{short_hash(seed, 16)}"
    payload["content_hash"] = content_hash(payload, exclude={"generated_at"}, sort_lists=True)
    validate_contract("challenger_comparison_report", payload)
    return payload


def write_challenger_comparison_report(
    *,
    challenger_evidence_batch_id: str,
    challenger_track_id: str | None = None,
    store_root: Path | None = None,
    actor: str = "Aegis",
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    report = build_challenger_comparison_report(
        challenger_evidence_batch_id=challenger_evidence_batch_id,
        challenger_track_id=challenger_track_id,
        store_root=store,
    )
    path = _report_path(store, report["challenger_comparison_report_id"])
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite immutable challenger comparison report: {path}")
    write_json(path, report, overwrite=False)
    active = [row for row in report["comparison_table"] if row["entity_type"] == "challenger" and row["exclusion_status"] == "included_for_human_review"]
    excluded = [row for row in report["comparison_table"] if row["entity_type"] != "incumbent" and row["exclusion_status"] == "excluded"]
    row = {
        "challenger_comparison_report_id": report["challenger_comparison_report_id"],
        "challenger_track_id": report["challenger_track_id"],
        "challenger_evidence_batch_id": report["challenger_evidence_batch_id"],
        "incumbent_sleeve_id": report["incumbent_sleeve_id"],
        "evidence_sufficiency": report["evidence_sufficiency"],
        "active_challenger_count": len(active),
        "excluded_challenger_count": len(excluded),
        "top_research_review_candidate_id": report["deterministic_rank_order"][0]["challenger_hypothesis_id"] if report["deterministic_rank_order"] else "",
        "recommended_next_action": report["recommended_next_action"],
        "research_label": report["research_label"],
        "content_hash": report["content_hash"],
        "generated_at": report["generated_at"],
        "schema_version": report["schema_version"],
    }
    append_jsonl(_registry_path(store), row)
    rows = read_jsonl(_registry_path(store))
    if not rows or rows[-1] != row:
        raise RuntimeError("challenger comparison report registry append failed")
    audit = write_audit_event(
        actor=actor,
        entity_type="challenger_comparison_report",
        entity_id=report["challenger_comparison_report_id"],
        action="challenger_comparison_report_created",
        new_state_hash=report["content_hash"],
        reason="Generated deterministic research-only challenger comparison report.",
        metadata={"registry_row": row, "governance_summary": report["governance_summary"]},
        store_root=store,
    )
    return {"report": report, "registry_row": row, "audit_event": audit, "json_path": str(path)}


def load_challenger_comparison_report(challenger_comparison_report_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    return read_json(_report_path(store, challenger_comparison_report_id))


def list_challenger_comparison_reports(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = ensure_store_layout(store_root)
    return read_jsonl(_registry_path(store))
