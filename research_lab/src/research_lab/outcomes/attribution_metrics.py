from __future__ import annotations

from collections import defaultdict
from statistics import mean, median
from typing import Any


def _window_number(value: Any) -> int:
    return int(str(value).rstrip("d"))


def _measured(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if row.get("outcome_status") == "measured"]


def _summary_by_window(rows: list[dict[str, Any]], field: str, fn) -> dict[str, float | int | None]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in _measured(rows):
        value = row.get(field)
        if value is not None:
            grouped[str(row["outcome_window"])].append(float(value))
    return {window: fn(values) if values else None for window, values in sorted(grouped.items(), key=lambda item: _window_number(item[0]))}


def _group_counts(rows: list[dict[str, Any]], candidates_by_id: dict[str, dict[str, Any]], group_field: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in _measured(rows):
        candidate = candidates_by_id.get(row["candidate_id"], {})
        grouped[str(candidate.get(group_field) or "unknown")].append(row)
    result: dict[str, dict[str, Any]] = {}
    for group, items in sorted(grouped.items()):
        result[group] = {
            "measured_outcome_count": len(items),
            "mean_post_cost_return_by_window": _summary_by_window(items, "post_cost_return", mean),
            "win_rate_by_window": _summary_by_window(items, "post_cost_return", lambda values: sum(1 for value in values if value > 0) / len(values)),
        }
    return result


def build_decision_quality_summary(rows: list[dict[str, Any]], candidates_by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    measured = _measured(rows)
    window_rows = [row for row in measured if row.get("outcome_window") == "5d"] or measured
    counts = {
        "approved_winner_count": 0,
        "approved_loser_count": 0,
        "ignored_winner_count": 0,
        "ignored_loser_count": 0,
        "deferred_winner_count": 0,
        "rejected_winner_count": 0,
    }
    approved_total = 0
    ignored_total = 0
    for row in window_rows:
        status = candidates_by_id.get(row["candidate_id"], {}).get("derived_candidate_status", "generated_no_decision")
        winner = float(row["post_cost_return"]) > 0
        if status == "approved":
            approved_total += 1
            counts["approved_winner_count" if winner else "approved_loser_count"] += 1
        elif status == "ignored":
            ignored_total += 1
            counts["ignored_winner_count" if winner else "ignored_loser_count"] += 1
        elif status == "deferred" and winner:
            counts["deferred_winner_count"] += 1
        elif status == "rejected" and winner:
            counts["rejected_winner_count"] += 1
    counts["operator_hit_rate"] = counts["approved_winner_count"] / approved_total if approved_total else None
    counts["missed_opportunity_rate"] = counts["ignored_winner_count"] / ignored_total if ignored_total else None
    counts["bad_approval_rate"] = counts["approved_loser_count"] / approved_total if approved_total else None
    counts["winner_definition"] = "post_cost_return > 0"
    counts["benchmark_relative_winner_definition"] = "excess_return > 0"
    counts["primary_window"] = "5d" if any(row.get("outcome_window") == "5d" for row in measured) else "all_measured_windows"
    return counts


def build_attribution_metrics(outcomes: list[dict[str, Any]], candidates: list[dict[str, Any]]) -> dict[str, Any]:
    candidates_by_id = {row["candidate_id"]: row for row in candidates}
    measured = _measured(outcomes)
    unavailable = [row for row in outcomes if row.get("outcome_status") == "unavailable"]
    metrics = {
        "candidate_count": len(candidates),
        "measured_candidate_count": len({row["candidate_id"] for row in measured}),
        "unavailable_candidate_count": len({row["candidate_id"] for row in unavailable}),
        "mean_post_cost_return_by_window": _summary_by_window(outcomes, "post_cost_return", mean),
        "median_post_cost_return_by_window": _summary_by_window(outcomes, "post_cost_return", median),
        "win_rate_by_window": _summary_by_window(outcomes, "post_cost_return", lambda values: sum(1 for value in values if value > 0) / len(values)),
        "mean_excess_return_by_window": _summary_by_window(outcomes, "excess_return", mean),
        "by_decision": _group_counts(outcomes, candidates_by_id, "derived_candidate_status"),
        "by_ranking_bucket": _group_counts(outcomes, candidates_by_id, "ranking_bucket"),
        "by_risk_regime": _group_counts(outcomes, candidates_by_id, "risk_regime"),
        "by_trend_regime": _group_counts(outcomes, candidates_by_id, "trend_regime"),
        "by_vol_regime": _group_counts(outcomes, candidates_by_id, "vol_regime"),
        "by_drawdown_regime": _group_counts(outcomes, candidates_by_id, "drawdown_regime"),
    }
    return metrics

