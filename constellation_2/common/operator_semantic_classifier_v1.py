from __future__ import annotations

NOT_YET_MATERIALIZED = "NOT_YET_MATERIALIZED"
BLOCKED_BY_UPSTREAM_PREREQUISITE = "BLOCKED_BY_UPSTREAM_PREREQUISITE"
PENDING_PROPAGATION = "PENDING_PROPAGATION"
MATERIALIZED_AND_FAILED = "MATERIALIZED_AND_FAILED"
FULLY_OBSERVED_AND_CONFIRMED = "FULLY_OBSERVED_AND_CONFIRMED"

OPERATOR_SEMANTIC_STATUSES_V1 = (
    NOT_YET_MATERIALIZED,
    BLOCKED_BY_UPSTREAM_PREREQUISITE,
    PENDING_PROPAGATION,
    MATERIALIZED_AND_FAILED,
    FULLY_OBSERVED_AND_CONFIRMED,
)


def classify_operator_semantic_status_v1(
    *,
    required_facts_present: bool,
    blocked_by_upstream_prerequisite: bool = False,
    pending_propagation: bool = False,
    materialized_failure: bool = False,
    fully_observed_and_confirmed: bool = False,
) -> str:
    if not required_facts_present:
        return (
            BLOCKED_BY_UPSTREAM_PREREQUISITE
            if blocked_by_upstream_prerequisite
            else NOT_YET_MATERIALIZED
        )
    if blocked_by_upstream_prerequisite:
        return BLOCKED_BY_UPSTREAM_PREREQUISITE
    if pending_propagation:
        return PENDING_PROPAGATION
    if materialized_failure:
        return MATERIALIZED_AND_FAILED
    if fully_observed_and_confirmed:
        return FULLY_OBSERVED_AND_CONFIRMED
    return MATERIALIZED_AND_FAILED


__all__ = [
    "BLOCKED_BY_UPSTREAM_PREREQUISITE",
    "FULLY_OBSERVED_AND_CONFIRMED",
    "MATERIALIZED_AND_FAILED",
    "NOT_YET_MATERIALIZED",
    "OPERATOR_SEMANTIC_STATUSES_V1",
    "PENDING_PROPAGATION",
    "classify_operator_semantic_status_v1",
]
