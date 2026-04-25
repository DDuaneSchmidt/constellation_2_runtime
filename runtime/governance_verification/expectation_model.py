from __future__ import annotations

from typing import Any

from .schemas import content_hash
from .types import ExpectationRecord
from ..meta_governance.store import ArtifactStore


DEFAULT_TOLERANCES = {
    "expected_risk_delta": 0.05,
    "expected_tax_delta": 0.05,
    "expected_turnover_delta": 1.0,
    "expected_capital_usage_delta": 1.0,
    "expected_autonomy_delta": 0.0,
    "expected_execution_delta": 0.0,
}


def write_expectation_records(
    store: ArtifactStore,
    verification_id: str,
    *,
    proposal_id: str,
    snapshot_id: str,
    review_window_start: str,
    review_window_end: str,
    tolerance_overrides: dict[str, float] | None = None,
) -> tuple[str, ...]:
    impact = store.read("impact_summaries", verification_id)["record"]
    tolerances = {**DEFAULT_TOLERANCES, **(tolerance_overrides or {})}
    expectation_refs: list[str] = []
    for metric_name in (
        "expected_risk_delta",
        "expected_tax_delta",
        "expected_turnover_delta",
        "expected_capital_usage_delta",
        "expected_autonomy_delta",
        "expected_execution_delta",
    ):
        expectation_id = f"expectation-{content_hash({'snapshot_id': snapshot_id, 'proposal_id': proposal_id, 'metric_name': metric_name})[:12]}"
        record = ExpectationRecord(
            expectation_id=expectation_id,
            snapshot_id=snapshot_id,
            proposal_id=proposal_id,
            metric_name=metric_name,
            expected_value=float(impact[metric_name]),
            tolerance_band=float(tolerances[metric_name]),
            review_window_start=review_window_start,
            review_window_end=review_window_end,
            evidence_ref=verification_id,
        )
        store.write_immutable("expectation_records", expectation_id, record, artifact_type="ExpectationRecord")
        expectation_refs.append(expectation_id)
    return tuple(expectation_refs)
