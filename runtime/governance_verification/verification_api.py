from __future__ import annotations

from pathlib import Path
from typing import Any

from ..meta_governance.api import _store
from .behavioral_invariants import evaluate_behavioral_invariants
from .dataset_registry import register_dataset as _register_dataset
from .decision_diff import compute_decision_diff
from .expectation_model import write_expectation_records
from .impact_summary import build_impact_summary
from .interaction_analysis import analyze_interactions
from .realized_validation import validate_realized_expectation as _validate_realized_expectation
from .verification_artifacts import create_verification_bundle as _create_verification_bundle
from .verification_audit import emit_verification_event
from .verification_context import create_verification_context as _create_verification_context


def register_dataset(
    *,
    dataset_id: str,
    dataset_type: str,
    source_description: str,
    time_range: dict[str, Any],
    dataset: Any,
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    artifact_id = _register_dataset(
        store,
        dataset_id=dataset_id,
        dataset_type=dataset_type,
        source_description=source_description,
        time_range=time_range,
        dataset=dataset,
    )
    emit_verification_event(store, "verification_dataset_registered", "system", artifact_refs=(artifact_id,))
    return artifact_id


def create_verification_context(
    *,
    active_snapshot_id: str,
    candidate_snapshot_id: str,
    dataset_refs: tuple[str, ...],
    requested_by: str,
    scope: dict[str, Any] | None = None,
    verification_id: str | None = None,
    status: str = "created",
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    artifact_id = _create_verification_context(
        store,
        active_snapshot_id=active_snapshot_id,
        candidate_snapshot_id=candidate_snapshot_id,
        dataset_refs=dataset_refs,
        requested_by=requested_by,
        scope=scope,
        verification_id=verification_id,
        status=status,
    )
    emit_verification_event(store, "verification_context_created", requested_by, artifact_refs=(artifact_id,))
    return artifact_id


def verify_candidate_decision_diff(verification_id: str, *, store_root: str | Path | None = None) -> str:
    store = _store(store_root)
    artifact_id = compute_decision_diff(store, verification_id)
    emit_verification_event(store, "decision_diff_created", "system", artifact_refs=(artifact_id,))
    return artifact_id


def summarize_impact(verification_id: str, *, store_root: str | Path | None = None) -> str:
    store = _store(store_root)
    artifact_id = build_impact_summary(store, verification_id)
    emit_verification_event(store, "impact_summary_created", "system", artifact_refs=(artifact_id,))
    return artifact_id


def verify_behavioral_invariants(verification_id: str, *, store_root: str | Path | None = None) -> tuple[str, ...]:
    store = _store(store_root)
    invariant_refs = evaluate_behavioral_invariants(store, verification_id)
    for ref in invariant_refs:
        emit_verification_event(store, "behavioral_invariant_evaluated", "system", artifact_refs=(ref,))
    return invariant_refs


def verify_interactions(
    verification_id: str,
    *,
    invariant_refs: tuple[str, ...],
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    artifact_id = analyze_interactions(store, verification_id, invariant_refs)
    emit_verification_event(store, "interaction_analysis_created", "system", artifact_refs=(artifact_id,))
    return artifact_id


def create_expectation_records(
    verification_id: str,
    *,
    proposal_id: str,
    snapshot_id: str,
    review_window_start: str,
    review_window_end: str,
    tolerance_overrides: dict[str, float] | None = None,
    store_root: str | Path | None = None,
) -> tuple[str, ...]:
    store = _store(store_root)
    expectation_refs = write_expectation_records(
        store,
        verification_id,
        proposal_id=proposal_id,
        snapshot_id=snapshot_id,
        review_window_start=review_window_start,
        review_window_end=review_window_end,
        tolerance_overrides=tolerance_overrides,
    )
    for ref in expectation_refs:
        emit_verification_event(store, "expectation_recorded", "system", artifact_refs=(ref,))
    return expectation_refs


def validate_realized_expectation(
    expectation_id: str,
    *,
    dataset_ref: str | None = None,
    actual_value: float | None = None,
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    artifact_id = _validate_realized_expectation(
        store,
        expectation_id,
        dataset_ref=dataset_ref,
        actual_value=actual_value,
    )
    emit_verification_event(store, "realized_validation_created", "system", artifact_refs=(artifact_id, expectation_id))
    return artifact_id


def create_verification_bundle(
    verification_id: str,
    *,
    invariant_result_refs: tuple[str, ...],
    expectation_refs: tuple[str, ...] = (),
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    bundle_id = _create_verification_bundle(
        store,
        verification_id=verification_id,
        invariant_result_refs=invariant_result_refs,
        expectation_refs=expectation_refs,
    )
    emit_verification_event(store, "verification_bundle_created", "system", artifact_refs=(bundle_id, verification_id))
    return bundle_id


def run_verification(
    *,
    active_snapshot_id: str,
    candidate_snapshot_id: str,
    dataset_refs: tuple[str, ...],
    requested_by: str,
    scope: dict[str, Any] | None = None,
    proposal_id: str | None = None,
    review_window_start: str | None = None,
    review_window_end: str | None = None,
    tolerance_overrides: dict[str, float] | None = None,
    verification_id: str | None = None,
    store_root: str | Path | None = None,
) -> dict[str, Any]:
    verified_scope = dict(scope or {})
    if proposal_id:
        verified_scope.setdefault("proposal_ids", [proposal_id])
    context_id = create_verification_context(
        active_snapshot_id=active_snapshot_id,
        candidate_snapshot_id=candidate_snapshot_id,
        dataset_refs=dataset_refs,
        requested_by=requested_by,
        scope=verified_scope,
        verification_id=verification_id,
        status="completed",
        store_root=store_root,
    )
    diff_id = verify_candidate_decision_diff(context_id, store_root=store_root)
    impact_id = summarize_impact(context_id, store_root=store_root)
    invariant_refs = verify_behavioral_invariants(context_id, store_root=store_root)
    interaction_id = verify_interactions(context_id, invariant_refs=invariant_refs, store_root=store_root)
    expectation_refs: tuple[str, ...] = ()
    if proposal_id and review_window_start and review_window_end:
        expectation_refs = create_expectation_records(
            context_id,
            proposal_id=proposal_id,
            snapshot_id=candidate_snapshot_id,
            review_window_start=review_window_start,
            review_window_end=review_window_end,
            tolerance_overrides=tolerance_overrides,
            store_root=store_root,
        )
    bundle_id = create_verification_bundle(
        context_id,
        invariant_result_refs=invariant_refs,
        expectation_refs=expectation_refs,
        store_root=store_root,
    )
    return {
        "verification_id": context_id,
        "decision_diff_ref": diff_id,
        "impact_summary_ref": impact_id,
        "interaction_analysis_ref": interaction_id,
        "invariant_result_refs": invariant_refs,
        "expectation_refs": expectation_refs,
        "bundle_ref": bundle_id,
    }


def find_verification_bundles(
    *,
    proposal_id: str | None = None,
    snapshot_id: str | None = None,
    store_root: str | Path | None = None,
) -> list[dict[str, Any]]:
    store = _store(store_root)
    bundles: list[dict[str, Any]] = []
    for bundle_id in store.list_ids("verification_bundles"):
        bundle = store.read("verification_bundles", bundle_id)
        context = store.read("verification_contexts", bundle["record"]["context_ref"])
        scope = context["record"].get("scope", {})
        if proposal_id and proposal_id not in scope.get("proposal_ids", []):
            continue
        if snapshot_id and context["record"]["candidate_snapshot_id"] != snapshot_id:
            continue
        bundles.append(bundle)
    return bundles
