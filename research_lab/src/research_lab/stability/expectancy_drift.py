from __future__ import annotations

from pathlib import Path
from statistics import mean, median
from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.stability.stability_registry import (
    CREATED_AT,
    CREATED_BY,
    RESEARCH_LABEL,
    finalize_report,
    load_longitudinal_context,
    metric_summary,
    sort_outcomes,
    stable_float,
    window_num,
    write_stability_artifacts,
)


MEAN_RETURN_EPSILON = 0.001
WIN_RATE_EPSILON = 0.03


def _rolling_metrics(rows: list[dict[str, Any]], *, rolling_window_size: int) -> list[dict[str, Any]]:
    ordered = sort_outcomes(rows)
    result: list[dict[str, Any]] = []
    for idx in range(0, max(len(ordered) - rolling_window_size + 1, 0)):
        chunk = ordered[idx : idx + rolling_window_size]
        post_cost = [stable_float(row.get("post_cost_return")) for row in chunk]
        excess = [stable_float(row.get("excess_return")) for row in chunk]
        post_cost_values = [value for value in post_cost if value is not None]
        excess_values = [value for value in excess if value is not None]
        result.append(
            {
                "rolling_index": idx + 1,
                "start_as_of_date": str(chunk[0].get("as_of_date") or ""),
                "end_as_of_date": str(chunk[-1].get("as_of_date") or ""),
                "rolling_mean_post_cost_return": mean(post_cost_values) if post_cost_values else None,
                "rolling_median_post_cost_return": median(post_cost_values) if post_cost_values else None,
                "rolling_win_rate": sum(1 for value in post_cost_values if value > 0) / len(post_cost_values) if post_cost_values else None,
                "rolling_mean_excess_return": mean(excess_values) if excess_values else None,
                "rolling_candidate_count": len(chunk),
            }
        )
    return result


def _split_metrics(rows: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any]]:
    ordered = sort_outcomes(rows)
    midpoint = len(ordered) // 2
    return metric_summary(ordered[:midpoint]), metric_summary(ordered[midpoint:])


def _window_status(baseline: dict[str, Any], latest: dict[str, Any]) -> tuple[str, list[str]]:
    baseline_mean = stable_float(baseline.get("mean_post_cost_return"))
    latest_mean = stable_float(latest.get("mean_post_cost_return"))
    baseline_win = stable_float(baseline.get("win_rate"))
    latest_win = stable_float(latest.get("win_rate"))
    baseline_excess = stable_float(baseline.get("mean_excess_return"))
    latest_excess = stable_float(latest.get("mean_excess_return"))
    if baseline_mean is None or latest_mean is None or baseline_win is None or latest_win is None:
        return "watch", ["missing_baseline_or_latest_metric"]
    mean_delta = latest_mean - baseline_mean
    win_delta = latest_win - baseline_win
    excess_delta = (latest_excess - baseline_excess) if baseline_excess is not None and latest_excess is not None else 0.0
    reasons = [
        f"mean_post_cost_delta={mean_delta:.6f}",
        f"win_rate_delta={win_delta:.6f}",
        f"mean_excess_delta={excess_delta:.6f}",
    ]
    mean_down = mean_delta < -MEAN_RETURN_EPSILON
    win_down = win_delta < -WIN_RATE_EPSILON
    mean_up = mean_delta > MEAN_RETURN_EPSILON
    win_up = win_delta > WIN_RATE_EPSILON
    if mean_down and win_down:
        return "degrading", reasons + ["latest_mean_post_cost_and_win_rate_below_baseline"]
    if mean_up and win_up:
        return "improving", reasons + ["latest_mean_post_cost_and_win_rate_above_baseline"]
    if abs(mean_delta) <= MEAN_RETURN_EPSILON and abs(win_delta) <= WIN_RATE_EPSILON:
        return "stable", reasons + ["latest_metrics_within_small_difference_thresholds"]
    if mean_down or win_down or excess_delta < -MEAN_RETURN_EPSILON:
        return "watch", reasons + ["mixed_deterioration_signal"]
    return "stable", reasons + ["mixed_without_clear_deterioration"]


def _overall_status(statuses: dict[str, str], measured_count: int) -> str:
    if measured_count < 40:
        return "insufficient_data"
    if any(status == "degrading" for status in statuses.values()):
        return "degrading"
    if any(status == "watch" for status in statuses.values()):
        return "watch"
    if statuses and all(status == "improving" for status in statuses.values()):
        return "improving"
    return "stable"


def _recommended_action(status: str) -> str:
    return {
        "insufficient_data": "collect_more_candidates",
        "stable": "continue_research",
        "improving": "continue_research",
        "watch": "review_sleeve",
        "degrading": "challenge_sleeve",
    }[status]


def build_expectancy_drift_report(
    *,
    sleeve_id: str,
    sleeve_version_id: str,
    longitudinal_run_id: str,
    rolling_window_size: int = 20,
    store_root: Path | None = None,
    created_at: str = CREATED_AT,
    created_by: str = CREATED_BY,
) -> dict[str, Any]:
    if rolling_window_size <= 0:
        raise RuntimeError(f"unsupported rolling window: {rolling_window_size}")
    context = load_longitudinal_context(longitudinal_run_id, store_root=store_root)
    run = context["run"]
    if run.get("sleeve_id") != sleeve_id or run.get("sleeve_version_id") != sleeve_version_id:
        raise RuntimeError("longitudinal run sleeve contract mismatch")
    measured = context["measured_rows"]
    by_window = {
        window: [row for row in measured if str(row.get("outcome_window")) == window]
        for window in context["windows"]
    }
    rolling_metrics: dict[str, Any] = {}
    baseline_metrics: dict[str, Any] = {}
    latest_metrics: dict[str, Any] = {}
    window_statuses: dict[str, str] = {}
    drift_reasons: list[str] = []
    for window in sorted(by_window, key=window_num):
        rows = by_window[window]
        rolling_metrics[window] = _rolling_metrics(rows, rolling_window_size=rolling_window_size)
        baseline, latest = _split_metrics(rows)
        baseline_metrics[window] = baseline
        latest_metrics[window] = latest
        status, reasons = _window_status(baseline, latest)
        window_statuses[window] = status
        drift_reasons.extend([f"{window}:{reason}" for reason in reasons])
    status = _overall_status(window_statuses, int(context["measured_candidate_count"]))
    if status == "insufficient_data":
        drift_reasons = [f"measured_candidate_count {context['measured_candidate_count']} below 40"]
    report = {
        "expectancy_drift_report_id": "",
        "sleeve_id": sleeve_id,
        "sleeve_version_id": sleeve_version_id,
        "longitudinal_run_id": longitudinal_run_id,
        "created_at": created_at,
        "created_by": created_by,
        "windows_analyzed": sorted(by_window, key=window_num),
        "rolling_window_size": rolling_window_size,
        "candidate_count": context["candidate_count"],
        "measured_candidate_count": context["measured_candidate_count"],
        "drift_status": status,
        "drift_reasons": sorted(set(drift_reasons)),
        "rolling_metrics": rolling_metrics,
        "latest_metrics": latest_metrics,
        "baseline_metrics": baseline_metrics,
        "recommended_action": _recommended_action(status),
        "research_label": RESEARCH_LABEL,
        "schema_version": "expectancy_drift_report.v1",
        "content_hash": "",
    }
    finalize_report(report, id_field="expectancy_drift_report_id", prefix="edr")
    validate_contract("expectancy_drift_report", report)
    return report


def expectancy_drift_markdown(report: dict[str, Any]) -> str:
    lines = ["# Expectancy Drift Report", "", report["research_label"], ""]
    lines.append(f"- sleeve_id: {report['sleeve_id']}")
    lines.append(f"- drift_status: {report['drift_status']}")
    lines.append(f"- measured_candidate_count: {report['measured_candidate_count']}")
    lines.append(f"- recommended_action: {report['recommended_action']}")
    lines.append("")
    lines.append("## Reasons")
    lines.extend(f"- {reason}" for reason in report["drift_reasons"])
    return "\n".join(lines) + "\n"


def write_expectancy_drift_report(
    *,
    sleeve_id: str,
    sleeve_version_id: str,
    longitudinal_run_id: str,
    rolling_window_size: int = 20,
    store_root: Path | None = None,
    actor: str = "Aegis",
) -> dict[str, Any]:
    report = build_expectancy_drift_report(
        sleeve_id=sleeve_id,
        sleeve_version_id=sleeve_version_id,
        longitudinal_run_id=longitudinal_run_id,
        rolling_window_size=rolling_window_size,
        store_root=store_root,
        created_by=actor,
    )
    store = load_longitudinal_context(longitudinal_run_id, store_root=store_root)["store"]
    row = {
        "expectancy_drift_report_id": report["expectancy_drift_report_id"],
        "sleeve_id": sleeve_id,
        "sleeve_version_id": sleeve_version_id,
        "longitudinal_run_id": longitudinal_run_id,
        "drift_status": report["drift_status"],
        "recommended_action": report["recommended_action"],
        "content_hash": report["content_hash"],
        "created_at": report["created_at"],
        "schema_version": report["schema_version"],
    }
    return write_stability_artifacts(
        store=store,
        family="expectancy_drift",
        report=report,
        id_field="expectancy_drift_report_id",
        contract_name="expectancy_drift_report",
        registry_name="expectancy_drift_reports.jsonl",
        registry_row=row,
        markdown=expectancy_drift_markdown(report),
        audit_action="expectancy_drift_report_written",
        actor=actor,
    )
