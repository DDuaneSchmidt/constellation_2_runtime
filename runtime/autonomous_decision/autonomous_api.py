from __future__ import annotations

from pathlib import Path
from typing import Any

from ..meta_governance.api import _store, get_active_snapshot
from .action_plan_builder import build_action_plan as _build_action_plan
from .autonomous_artifacts import write_bundle, write_records
from .autonomous_audit import emit_autonomous_event
from .authority_evaluation import evaluate_authority
from .autonomy_classification import classify_action
from .autonomy_predicates import evaluate_predicates as _evaluate_predicates
from .candidate_actions import build_candidate_actions
from .execution_eligibility import determine_eligibility
from .operator_actions import build_operator_view
from .portfolio_prioritization import prioritize
from .schemas import utc_now


AUTONOMOUS_SOURCE_KINDS = (
    "adaptation_recommendations",
    "correction_recommendations",
    "realized_validation_results",
    "reconciliation_results",
    "candidate_actions",
    "autonomy_classifications",
    "authority_evaluations",
    "prioritized_actions",
    "execution_eligibilities",
    "operator_action_views",
    "autonomous_action_plans",
)


def _created_at_for_refs(store, refs: tuple[str, ...], fallback: str | None = None) -> str:
    timestamps: list[str] = []
    for ref in refs:
        for kind in AUTONOMOUS_SOURCE_KINDS:
            if store.exists(kind, ref):
                timestamps.append(store.read(kind, ref)["created_at"])
                break
    if timestamps:
        return max(timestamps)
    return fallback or utc_now()


def generate_candidate_actions(
    *,
    adaptation_recommendation_refs: tuple[str, ...] = (),
    correction_recommendation_refs: tuple[str, ...] = (),
    realized_validation_refs: tuple[str, ...] = (),
    store_root: str | Path | None = None,
) -> tuple[str, ...]:
    store = _store(store_root)
    records = build_candidate_actions(
        store,
        adaptation_recommendation_refs=adaptation_recommendation_refs,
        correction_recommendation_refs=correction_recommendation_refs,
        realized_validation_refs=realized_validation_refs,
    )
    refs = write_records(
        store,
        kind="candidate_actions",
        records=records,
        artifact_type="CandidateAction",
        id_field="candidate_action_id",
        created_at=_created_at_for_refs(store, tuple(sorted(adaptation_recommendation_refs + correction_recommendation_refs + realized_validation_refs))),
    )
    for ref in refs:
        emit_autonomous_event(store, "candidate_action_created", "system", artifact_refs=(ref,))
    return refs


def classify_candidate_actions(
    *,
    candidate_action_refs: tuple[str, ...],
    store_root: str | Path | None = None,
) -> tuple[str, ...]:
    store = _store(store_root)
    records = tuple(classify_action(store, ref) for ref in candidate_action_refs)
    refs = write_records(
        store,
        kind="autonomy_classifications",
        records=records,
        artifact_type="AutonomyClassification",
        id_field="candidate_action_id",
        created_at=_created_at_for_refs(store, candidate_action_refs),
    )
    for ref in refs:
        emit_autonomous_event(store, "candidate_action_classified", "system", artifact_refs=(ref,))
    return refs


def assess_authority(
    *,
    candidate_action_refs: tuple[str, ...],
    expected_graph_hash: str | None = None,
    expected_interpreter_version: str | None = None,
    store_root: str | Path | None = None,
) -> tuple[str, ...]:
    store = _store(store_root)
    records = tuple(
        evaluate_authority(
            ref,
            expected_graph_hash=expected_graph_hash,
            expected_interpreter_version=expected_interpreter_version,
            store_root=store.root,
        )
        for ref in candidate_action_refs
    )
    refs = write_records(
        store,
        kind="authority_evaluations",
        records=records,
        artifact_type="AuthorityEvaluation",
        id_field="candidate_action_id",
        created_at=_created_at_for_refs(store, candidate_action_refs),
    )
    for ref in refs:
        emit_autonomous_event(store, "authority_evaluated", "system", artifact_refs=(ref,))
    return refs


def evaluate_action_predicates(
    *,
    candidate_action_refs: tuple[str, ...],
    authority_evaluation_refs: tuple[str, ...],
    classification_refs: tuple[str, ...],
    approval_refs_by_action: dict[str, tuple[str, ...]] | None = None,
    strict_portfolio_budget: bool = False,
    portfolio_risk_budget_remaining: float | None = None,
    tax_uncertainty_by_action: dict[str, float] | None = None,
    tax_uncertainty_limit: float | None = None,
    store_root: str | Path | None = None,
) -> tuple[str, ...]:
    store = _store(store_root)
    approval_map = approval_refs_by_action or {}
    tax_map = tax_uncertainty_by_action or {}
    classification_by_action = {
        store.read("autonomy_classifications", ref)["record"]["candidate_action_id"]: ref
        for ref in classification_refs
    }
    authority_by_action = {
        store.read("authority_evaluations", ref)["record"]["candidate_action_id"]: ref
        for ref in authority_evaluation_refs
    }
    created_at = _created_at_for_refs(store, tuple(sorted(candidate_action_refs + authority_evaluation_refs + classification_refs)))
    refs: list[str] = []
    for candidate_ref in candidate_action_refs:
        results = _evaluate_predicates(
            candidate_ref,
            authority_evaluation_ref=authority_by_action[candidate_ref],
            classification_ref=classification_by_action[candidate_ref],
            approval_refs=approval_map.get(candidate_ref, ()),
            strict_portfolio_budget=strict_portfolio_budget,
            portfolio_risk_budget_remaining=portfolio_risk_budget_remaining,
            tax_uncertainty=tax_map.get(candidate_ref),
            tax_uncertainty_limit=tax_uncertainty_limit,
            store_root=store.root,
        )
        for result in results:
            artifact_id = f"{result.candidate_action_id}__{result.predicate_id}"
            store.write_immutable("autonomy_predicate_results", artifact_id, result, artifact_type="AutonomyPredicateResult", created_at=created_at)
            refs.append(artifact_id)
            emit_autonomous_event(store, "autonomy_predicate_evaluated", "system", artifact_refs=(artifact_id,))
    return tuple(refs)


def prioritize_actions(
    *,
    candidate_action_refs: tuple[str, ...],
    classification_refs: tuple[str, ...],
    exposure_overrides: dict[str, dict] | None = None,
    require_explicit_exposure: bool = False,
    store_root: str | Path | None = None,
) -> tuple[str, ...]:
    store = _store(store_root)
    records = prioritize(
        store,
        candidate_action_refs=candidate_action_refs,
        classification_refs=classification_refs,
        exposure_overrides=exposure_overrides,
        require_explicit_exposure=require_explicit_exposure,
    )
    refs = write_records(
        store,
        kind="prioritized_actions",
        records=records,
        artifact_type="PrioritizedAction",
        id_field="prioritized_action_id",
        created_at=_created_at_for_refs(store, tuple(sorted(candidate_action_refs + classification_refs))),
    )
    for ref in refs:
        emit_autonomous_event(store, "candidate_action_prioritized", "system", artifact_refs=(ref,))
    return refs


def determine_execution_eligibility(
    *,
    candidate_action_refs: tuple[str, ...],
    classification_refs: tuple[str, ...],
    predicate_result_refs: tuple[str, ...],
    authority_evaluation_refs: tuple[str, ...],
    approval_refs_by_action: dict[str, tuple[str, ...]] | None = None,
    store_root: str | Path | None = None,
) -> tuple[str, ...]:
    store = _store(store_root)
    approval_map = approval_refs_by_action or {}
    classification_by_action = {
        store.read("autonomy_classifications", ref)["record"]["candidate_action_id"]: ref
        for ref in classification_refs
    }
    authority_by_action = {
        store.read("authority_evaluations", ref)["record"]["candidate_action_id"]: ref
        for ref in authority_evaluation_refs
    }
    predicate_by_action: dict[str, list[str]] = {}
    for ref in predicate_result_refs:
        record = store.read("autonomy_predicate_results", ref)["record"]
        predicate_by_action.setdefault(record["candidate_action_id"], []).append(ref)
    records = tuple(
        determine_eligibility(
            store,
            candidate_action_id=ref,
            classification_ref=classification_by_action[ref],
            predicate_result_refs=tuple(sorted(predicate_by_action.get(ref, ()))),
            authority_evaluation_ref=authority_by_action[ref],
            approval_refs=approval_map.get(ref, ()),
        )
        for ref in candidate_action_refs
    )
    refs = write_records(
        store,
        kind="execution_eligibilities",
        records=records,
        artifact_type="ExecutionEligibility",
        id_field="candidate_action_id",
        created_at=_created_at_for_refs(store, tuple(sorted(candidate_action_refs + classification_refs + authority_evaluation_refs))),
    )
    for ref in refs:
        emit_autonomous_event(store, "execution_eligibility_determined", "system", artifact_refs=(ref,))
    return refs


def build_operator_action_view(
    *,
    prioritized_action_refs: tuple[str, ...],
    execution_eligibility_refs: tuple[str, ...],
    as_of: str | None = None,
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    record = build_operator_view(
        store,
        prioritized_action_refs=prioritized_action_refs,
        execution_eligibility_refs=execution_eligibility_refs,
        as_of=as_of or utc_now(),
    )
    store.write_immutable("operator_action_views", record.operator_action_view_id, record, artifact_type="OperatorActionView", created_at=record.as_of)
    emit_autonomous_event(store, "operator_action_view_created", "system", artifact_refs=(record.operator_action_view_id,))
    return record.operator_action_view_id


def build_action_plan(
    *,
    prioritized_action_refs: tuple[str, ...],
    execution_eligibility_refs: tuple[str, ...],
    generated_at: str | None = None,
    store_root: str | Path | None = None,
) -> str:
    store = _store(store_root)
    active = get_active_snapshot(store_root=store.root)
    if active is None:
        raise ValueError("ACTIVE_SNAPSHOT_REQUIRED")
    generated = generated_at or utc_now()
    record = _build_action_plan(
        store,
        active_snapshot_id=active["record"]["snapshot_id"],
        prioritized_action_refs=prioritized_action_refs,
        execution_eligibility_refs=execution_eligibility_refs,
        generated_at=generated,
    )
    store.write_immutable("autonomous_action_plans", record.action_plan_id, record, artifact_type="AutonomousActionPlan", created_at=generated)
    emit_autonomous_event(store, "autonomous_action_plan_created", "system", artifact_refs=(record.action_plan_id,))
    return record.action_plan_id


def run_autonomous_cycle(
    *,
    adaptation_recommendation_refs: tuple[str, ...] = (),
    correction_recommendation_refs: tuple[str, ...] = (),
    realized_validation_refs: tuple[str, ...] = (),
    expected_graph_hash: str | None = None,
    expected_interpreter_version: str | None = None,
    approval_refs_by_action: dict[str, tuple[str, ...]] | None = None,
    exposure_overrides: dict[str, dict] | None = None,
    require_explicit_exposure: bool = False,
    strict_portfolio_budget: bool = False,
    portfolio_risk_budget_remaining: float | None = None,
    tax_uncertainty_by_action: dict[str, float] | None = None,
    tax_uncertainty_limit: float | None = None,
    store_root: str | Path | None = None,
) -> dict[str, Any]:
    candidate_refs = generate_candidate_actions(
        adaptation_recommendation_refs=adaptation_recommendation_refs,
        correction_recommendation_refs=correction_recommendation_refs,
        realized_validation_refs=realized_validation_refs,
        store_root=store_root,
    )
    classification_refs = classify_candidate_actions(candidate_action_refs=candidate_refs, store_root=store_root)
    authority_refs = assess_authority(
        candidate_action_refs=candidate_refs,
        expected_graph_hash=expected_graph_hash,
        expected_interpreter_version=expected_interpreter_version,
        store_root=store_root,
    )
    predicate_refs = evaluate_action_predicates(
        candidate_action_refs=candidate_refs,
        authority_evaluation_refs=authority_refs,
        classification_refs=classification_refs,
        approval_refs_by_action=approval_refs_by_action,
        strict_portfolio_budget=strict_portfolio_budget,
        portfolio_risk_budget_remaining=portfolio_risk_budget_remaining,
        tax_uncertainty_by_action=tax_uncertainty_by_action,
        tax_uncertainty_limit=tax_uncertainty_limit,
        store_root=store_root,
    )
    prioritized_refs = prioritize_actions(
        candidate_action_refs=candidate_refs,
        classification_refs=classification_refs,
        exposure_overrides=exposure_overrides,
        require_explicit_exposure=require_explicit_exposure,
        store_root=store_root,
    )
    eligibility_refs = determine_execution_eligibility(
        candidate_action_refs=candidate_refs,
        classification_refs=classification_refs,
        predicate_result_refs=predicate_refs,
        authority_evaluation_refs=authority_refs,
        approval_refs_by_action=approval_refs_by_action,
        store_root=store_root,
    )
    operator_view_ref = build_operator_action_view(
        prioritized_action_refs=prioritized_refs,
        execution_eligibility_refs=eligibility_refs,
        store_root=store_root,
    )
    action_plan_ref = build_action_plan(
        prioritized_action_refs=prioritized_refs,
        execution_eligibility_refs=eligibility_refs,
        store_root=store_root,
    )
    store = _store(store_root)
    created_at = _created_at_for_refs(store, tuple(sorted(candidate_refs + classification_refs + authority_refs + prioritized_refs + eligibility_refs + (operator_view_ref, action_plan_ref))))
    bundle_ref = write_bundle(
        store,
        candidate_action_refs=candidate_refs,
        autonomy_classification_refs=classification_refs,
        predicate_result_refs=predicate_refs,
        authority_evaluation_refs=authority_refs,
        prioritized_action_refs=prioritized_refs,
        execution_eligibility_refs=eligibility_refs,
        operator_action_view_ref=operator_view_ref,
        action_plan_ref=action_plan_ref,
        created_at=created_at,
    )
    emit_autonomous_event(store, "autonomous_decision_bundle_created", "system", artifact_refs=(bundle_ref, operator_view_ref, action_plan_ref))
    return {
        "candidate_action_refs": candidate_refs,
        "autonomy_classification_refs": classification_refs,
        "predicate_result_refs": predicate_refs,
        "authority_evaluation_refs": authority_refs,
        "prioritized_action_refs": prioritized_refs,
        "execution_eligibility_refs": eligibility_refs,
        "operator_action_view_ref": operator_view_ref,
        "action_plan_ref": action_plan_ref,
        "bundle_ref": bundle_ref,
    }


def find_autonomous_bundles(
    *,
    snapshot_id: str | None = None,
    store_root: str | Path | None = None,
) -> list[dict[str, Any]]:
    store = _store(store_root)
    if snapshot_id is None:
        return [store.read("autonomous_decision_bundles", bundle_id) for bundle_id in store.list_ids("autonomous_decision_bundles")]
    known_refs: set[str] = {snapshot_id}
    for expectation_id in store.list_ids("expectation_records"):
        if store.read("expectation_records", expectation_id)["record"]["snapshot_id"] == snapshot_id:
            known_refs.add(expectation_id)
    for validation_id in store.list_ids("realized_validation_results"):
        if store.read("realized_validation_results", validation_id)["record"]["expectation_id"] in known_refs:
            known_refs.add(validation_id)
    bundles: list[dict[str, Any]] = []
    for bundle_id in store.list_ids("autonomous_decision_bundles"):
        bundle = store.read("autonomous_decision_bundles", bundle_id)
        related = False
        for action_ref in bundle["record"]["candidate_action_refs"]:
            action = store.read("candidate_actions", action_ref)["record"]
            if set(action["source_refs"]) & known_refs or set(action["supporting_evidence_refs"]) & known_refs:
                related = True
                break
        if related:
            bundles.append(bundle)
    return bundles
