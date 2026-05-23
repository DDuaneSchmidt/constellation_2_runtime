from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.stability.stability_registry import (
    CREATED_AT,
    CREATED_BY,
    RESEARCH_LABEL,
    finalize_report,
    load_longitudinal_context,
    metric_summary,
    stable_float,
    window_num,
    write_stability_artifacts,
)


REGIME_DIMENSIONS = ["risk_regime", "trend_regime", "vol_regime", "drawdown_regime"]


def _regime_metrics(rows: list[dict[str, Any]], windows: list[str]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for dimension in sorted(REGIME_DIMENSIONS):
        values = sorted({str(row.get(dimension) or "UNKNOWN") for row in rows})
        result[dimension] = {}
        for value in values:
            result[dimension][value] = {}
            for window in windows:
                bucket = [
                    row
                    for row in rows
                    if str(row.get(dimension) or "UNKNOWN") == value and str(row.get("outcome_window")) == window
                ]
                result[dimension][value][window] = metric_summary(bucket)
    return result


def _covered_regimes(metrics: dict[str, Any]) -> list[dict[str, Any]]:
    covered: list[dict[str, Any]] = []
    for dimension in sorted(metrics):
        for regime_value in sorted(metrics[dimension]):
            for window in sorted(metrics[dimension][regime_value], key=window_num):
                row = metrics[dimension][regime_value][window]
                if int(row.get("measured_count") or 0) >= 10 and stable_float(row.get("mean_post_cost_return")) is not None:
                    covered.append(
                        {
                            "dimension": dimension,
                            "regime": regime_value,
                            "outcome_window": window,
                            "measured_count": row["measured_count"],
                            "mean_post_cost_return": row["mean_post_cost_return"],
                        }
                    )
    return covered


def _fragility_status(covered: list[dict[str, Any]], measured_count: int) -> tuple[str, list[str]]:
    if measured_count < 30:
        return "insufficient_data", [f"measured_candidate_count {measured_count} below 30"]
    if not covered:
        return "watch", ["no_regime_bucket_has_10_measured_candidates"]
    means = [float(row["mean_post_cost_return"]) for row in covered]
    best = max(means)
    worst = min(means)
    spread = best - worst
    has_positive = any(value > 0 for value in means)
    has_negative = any(value < 0 for value in means)
    thin = any(int(row["measured_count"]) < 20 for row in covered)
    reasons = [f"best_worst_mean_spread={spread:.6f}"]
    if has_positive and has_negative and spread > 0.01:
        return "fragile", reasons + ["positive_and_negative_regime_means_with_large_spread"]
    if thin:
        return "watch", reasons + ["regime_sample_sizes_are_thin"]
    if spread > 0.005:
        return "watch", reasons + ["best_worst_spread_above_watch_threshold"]
    return "robust", reasons + ["covered_regimes_have_consistent_low_spread"]


def _recommended_action(status: str) -> str:
    return {
        "insufficient_data": "collect_more_candidates",
        "robust": "continue_research",
        "watch": "review_sleeve",
        "fragile": "challenge_sleeve",
    }[status]


def build_regime_fragility_report(
    *,
    sleeve_id: str,
    sleeve_version_id: str,
    longitudinal_run_id: str,
    store_root: Path | None = None,
    created_at: str = CREATED_AT,
    created_by: str = CREATED_BY,
) -> dict[str, Any]:
    context = load_longitudinal_context(longitudinal_run_id, store_root=store_root)
    run = context["run"]
    if run.get("sleeve_id") != sleeve_id or run.get("sleeve_version_id") != sleeve_version_id:
        raise RuntimeError("longitudinal run sleeve contract mismatch")
    measured = context["measured_rows"]
    metrics = _regime_metrics(measured, context["windows"])
    covered = _covered_regimes(metrics)
    sorted_best = sorted(covered, key=lambda row: (-float(row["mean_post_cost_return"]), row["dimension"], row["regime"], row["outcome_window"]))
    sorted_worst = sorted(covered, key=lambda row: (float(row["mean_post_cost_return"]), row["dimension"], row["regime"], row["outcome_window"]))
    status, reasons = _fragility_status(covered, int(context["measured_candidate_count"]))
    coverage = {
        "total_measured_candidate_count": context["measured_candidate_count"],
        "covered_regime_window_count": len(covered),
        "thin_regime_window_count": sum(1 for row in covered if int(row["measured_count"]) < 20),
    }
    report = {
        "regime_fragility_report_id": "",
        "sleeve_id": sleeve_id,
        "sleeve_version_id": sleeve_version_id,
        "longitudinal_run_id": longitudinal_run_id,
        "created_at": created_at,
        "created_by": created_by,
        "regime_dimensions": REGIME_DIMENSIONS,
        "windows_analyzed": context["windows"],
        "fragility_status": status,
        "fragility_reasons": reasons,
        "regime_metrics": metrics,
        "best_regimes": sorted_best[:5],
        "worst_regimes": sorted_worst[:5],
        "regime_coverage": coverage,
        "recommended_action": _recommended_action(status),
        "research_label": RESEARCH_LABEL,
        "schema_version": "regime_fragility_report.v1",
        "content_hash": "",
    }
    finalize_report(report, id_field="regime_fragility_report_id", prefix="rfr")
    validate_contract("regime_fragility_report", report)
    return report


def regime_fragility_markdown(report: dict[str, Any]) -> str:
    lines = ["# Regime Fragility Report", "", report["research_label"], ""]
    lines.append(f"- sleeve_id: {report['sleeve_id']}")
    lines.append(f"- fragility_status: {report['fragility_status']}")
    lines.append(f"- recommended_action: {report['recommended_action']}")
    lines.append("")
    lines.append("## Reasons")
    lines.extend(f"- {reason}" for reason in report["fragility_reasons"])
    return "\n".join(lines) + "\n"


def write_regime_fragility_report(
    *,
    sleeve_id: str,
    sleeve_version_id: str,
    longitudinal_run_id: str,
    store_root: Path | None = None,
    actor: str = "Aegis",
) -> dict[str, Any]:
    report = build_regime_fragility_report(
        sleeve_id=sleeve_id,
        sleeve_version_id=sleeve_version_id,
        longitudinal_run_id=longitudinal_run_id,
        store_root=store_root,
        created_by=actor,
    )
    store = load_longitudinal_context(longitudinal_run_id, store_root=store_root)["store"]
    row = {
        "regime_fragility_report_id": report["regime_fragility_report_id"],
        "sleeve_id": sleeve_id,
        "sleeve_version_id": sleeve_version_id,
        "longitudinal_run_id": longitudinal_run_id,
        "fragility_status": report["fragility_status"],
        "recommended_action": report["recommended_action"],
        "content_hash": report["content_hash"],
        "created_at": report["created_at"],
        "schema_version": report["schema_version"],
    }
    return write_stability_artifacts(
        store=store,
        family="regime_fragility",
        report=report,
        id_field="regime_fragility_report_id",
        contract_name="regime_fragility_report",
        registry_name="regime_fragility_reports.jsonl",
        registry_row=row,
        markdown=regime_fragility_markdown(report),
        audit_action="regime_fragility_report_written",
        actor=actor,
    )
