from __future__ import annotations

import sys
from pathlib import Path


SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.operator_semantic_classifier_v1 import (  # noqa: E402
    BLOCKED_BY_UPSTREAM_PREREQUISITE,
    FULLY_OBSERVED_AND_CONFIRMED,
    MATERIALIZED_AND_FAILED,
    NOT_YET_MATERIALIZED,
    PENDING_PROPAGATION,
    classify_operator_semantic_status_v1,
)


def test_classifier_marks_missing_required_facts_as_not_yet_materialized() -> None:
    assert (
        classify_operator_semantic_status_v1(required_facts_present=False)
        == NOT_YET_MATERIALIZED
    )


def test_classifier_prioritizes_upstream_block_over_other_non_success_states() -> None:
    assert (
        classify_operator_semantic_status_v1(
            required_facts_present=True,
            blocked_by_upstream_prerequisite=True,
            pending_propagation=True,
            materialized_failure=True,
        )
        == BLOCKED_BY_UPSTREAM_PREREQUISITE
    )


def test_classifier_marks_pending_propagation_before_materialized_failure() -> None:
    assert (
        classify_operator_semantic_status_v1(
            required_facts_present=True,
            pending_propagation=True,
            materialized_failure=True,
        )
        == PENDING_PROPAGATION
    )


def test_classifier_marks_materialized_failure_when_required_facts_exist_and_success_is_not_reached() -> None:
    assert (
        classify_operator_semantic_status_v1(
            required_facts_present=True,
            materialized_failure=True,
        )
        == MATERIALIZED_AND_FAILED
    )


def test_classifier_marks_fully_observed_and_confirmed_only_when_explicitly_complete() -> None:
    assert (
        classify_operator_semantic_status_v1(
            required_facts_present=True,
            fully_observed_and_confirmed=True,
        )
        == FULLY_OBSERVED_AND_CONFIRMED
    )
