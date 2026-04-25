from __future__ import annotations

from typing import Any, Callable, Mapping


SELECTION_MODEL_VERSION = "constellation_2.common.product_summary_precedence_v1"


def _is_historical(candidate: Mapping[str, Any]) -> bool:
    return str(candidate.get("visibility_state") or "") in {"historical_only", "suppressed"} or str(
        candidate.get("freshness_state") or ""
    ) in {"superseded", "historical_only"}


RULES: tuple[dict[str, Any], ...] = (
    {
        "reason_id": "SUPPRESS_HISTORICAL",
        "bucket": "suppressed",
        "rank": 99,
        "predicate": _is_historical,
    },
    {
        "reason_id": "ELEVATE_CRITICAL_DEGRADED",
        "bucket": "critical_degraded",
        "rank": 0,
        "predicate": lambda candidate: str(candidate.get("item_kind") or "") == "degraded_state"
        and bool(candidate.get("critical")),
    },
    {
        "reason_id": "ELEVATE_BLOCKED_IMPORTANT",
        "bucket": "top_blocked",
        "rank": 1,
        "predicate": lambda candidate: str(candidate.get("item_kind") or "") == "opportunity"
        and str(candidate.get("opportunity_state") or "") == "blocked"
        and str(candidate.get("review_priority") or "") == "review_now",
    },
    {
        "reason_id": "ELEVATE_REVIEW_DELTA",
        "bucket": "top_review_deltas",
        "rank": 2,
        "predicate": lambda candidate: str(candidate.get("item_kind") or "") == "opportunity"
        and str(candidate.get("delta_state") or "") in {"new", "changed", "blocked_but_still_important"},
    },
    {
        "reason_id": "SELECT_ACTIONABLE_NOW",
        "bucket": "top_actionable",
        "rank": 3,
        "predicate": lambda candidate: str(candidate.get("item_kind") or "") == "opportunity"
        and str(candidate.get("actionability_state") or "") == "actionable"
        and str(candidate.get("review_priority") or "") == "review_now",
    },
    {
        "reason_id": "SELECT_ACTIONABLE_SOON",
        "bucket": "top_actionable",
        "rank": 4,
        "predicate": lambda candidate: str(candidate.get("item_kind") or "") == "opportunity"
        and str(candidate.get("actionability_state") or "") == "actionable"
        and str(candidate.get("review_priority") or "") == "review_soon",
    },
    {
        "reason_id": "SELECT_MONITOR_ONLY",
        "bucket": "background",
        "rank": 5,
        "predicate": lambda candidate: str(candidate.get("item_kind") or "") == "opportunity"
        and str(candidate.get("review_priority") or "") == "monitor_only",
    },
)


def evaluate_product_summary_candidate_v1(candidate: Mapping[str, Any]) -> dict[str, Any]:
    for rule in RULES:
        predicate = rule["predicate"]
        if bool(predicate(candidate)):
            return {
                "selection_reason_id": str(rule["reason_id"]),
                "selection_bucket": str(rule["bucket"]),
                "selection_rank": int(rule["rank"]),
                "suppressed": str(rule["bucket"]) == "suppressed",
            }
    return {
        "selection_reason_id": "NO_SELECTION",
        "selection_bucket": "background",
        "selection_rank": 90,
        "suppressed": False,
    }
