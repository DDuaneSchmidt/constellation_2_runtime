from __future__ import annotations

from typing import Any

from .schemas import content_hash
from .types import CandidateAction
from ..meta_governance.store import ArtifactStore


def _build_action(
    *,
    action_family: str,
    target_surface: str,
    source_refs: tuple[str, ...],
    proposed_effect: str,
    rationale: str,
    supporting_evidence_refs: tuple[str, ...],
    estimated_blast_radius: str,
    estimated_scope: dict[str, Any],
) -> CandidateAction:
    payload = {
        "action_family": action_family,
        "target_surface": target_surface,
        "source_refs": source_refs,
        "proposed_effect": proposed_effect,
        "rationale": rationale,
        "supporting_evidence_refs": supporting_evidence_refs,
        "estimated_blast_radius": estimated_blast_radius,
        "estimated_scope": estimated_scope,
    }
    artifact_hash = content_hash(payload)
    return CandidateAction(
        candidate_action_id=f"candidate-action-{artifact_hash[:12]}",
        action_family=action_family,
        target_surface=target_surface,
        source_refs=source_refs,
        proposed_effect=proposed_effect,
        rationale=rationale,
        supporting_evidence_refs=supporting_evidence_refs,
        estimated_blast_radius=estimated_blast_radius,
        estimated_scope=estimated_scope,
        artifact_hash=artifact_hash,
    )


def _from_adaptation_recommendation(store: ArtifactStore, ref: str) -> CandidateAction:
    recommendation = store.read("adaptation_recommendations", ref)["record"]
    recommendation_type = recommendation["recommendation_type"]
    if recommendation_type == "tighten_execution_guardrails":
        action_family = "tighten_guardrail"
        target_surface = "execution"
        proposed_effect = "tighten execution guardrail behavior through governed follow-up"
        estimated_blast_radius = "moderate"
    elif recommendation_type == "review_thresholds":
        action_family = "open_governance_proposal"
        target_surface = recommendation["target_surface"]
        proposed_effect = "open a governed review for threshold or policy changes"
        estimated_blast_radius = "critical"
    elif recommendation_type == "investigate_broker_truth":
        action_family = "require_review"
        target_surface = "broker_truth"
        proposed_effect = "require broker-truth review before further autonomous behavior"
        estimated_blast_radius = "moderate"
    elif recommendation_type == "rebuild_state_surface":
        action_family = "rebuild_state_surface"
        target_surface = "state"
        proposed_effect = "rebuild the affected internal state surface under operator review"
        estimated_blast_radius = "moderate"
    elif recommendation_type == "collect_more_evidence":
        action_family = "no_immediate_action"
        target_surface = recommendation["target_surface"]
        proposed_effect = "collect more evidence before changing runtime behavior"
        estimated_blast_radius = "safe"
    elif recommendation_type == "no_action":
        action_family = "no_immediate_action"
        target_surface = recommendation["target_surface"]
        proposed_effect = "preserve current runtime behavior and surface the issue only"
        estimated_blast_radius = "safe"
    else:
        action_family = "require_review"
        target_surface = recommendation["target_surface"]
        proposed_effect = recommendation["recommended_action"]
        estimated_blast_radius = "moderate"
    supporting = tuple(sorted(set(tuple(recommendation["source_refs"]) + (ref,))))
    return _build_action(
        action_family=action_family,
        target_surface=target_surface,
        source_refs=(ref,),
        proposed_effect=proposed_effect,
        rationale=recommendation["rationale"],
        supporting_evidence_refs=supporting,
        estimated_blast_radius=estimated_blast_radius,
        estimated_scope={
            "requires_human_review": recommendation["requires_human_review"],
            "governance_candidate_needed": recommendation["governance_candidate_needed"],
            "priority_class": recommendation["priority_class"],
            "reversible": action_family in {"freeze_execution_family", "defer_change_family", "no_immediate_action"},
        },
    )


def _from_correction_recommendation(store: ArtifactStore, ref: str) -> CandidateAction:
    recommendation = store.read("correction_recommendations", ref)["record"]
    action_type = recommendation["action_type"]
    if action_type == "freeze_governed_execution":
        action_family = "freeze_execution_family"
        target_surface = "execution"
        proposed_effect = "freeze governed execution until truth and authority conditions recover"
        estimated_blast_radius = "moderate"
    elif action_type == "import_missing_execution":
        action_family = "import_missing_truth"
        target_surface = "execution"
        proposed_effect = "import missing execution truth under governed review"
        estimated_blast_radius = "moderate"
    elif action_type == "refresh_tax_lots":
        action_family = "import_missing_truth"
        target_surface = "tax"
        proposed_effect = "refresh tax-lot truth before autonomous progression"
        estimated_blast_radius = "moderate"
    elif action_type == "rebuild_internal_state":
        action_family = "rebuild_state_surface"
        target_surface = "state"
        proposed_effect = "rebuild affected internal truth surfaces"
        estimated_blast_radius = "moderate"
    elif action_type == "broker_review_required":
        action_family = "require_review"
        target_surface = "broker_truth"
        proposed_effect = "require broker-truth review"
        estimated_blast_radius = "moderate"
    else:
        action_family = "no_immediate_action"
        target_surface = recommendation["target_surface"]
        proposed_effect = action_type
        estimated_blast_radius = "safe"
    return _build_action(
        action_family=action_family,
        target_surface=target_surface,
        source_refs=(ref,),
        proposed_effect=proposed_effect,
        rationale=recommendation["rationale"],
        supporting_evidence_refs=(ref, recommendation["reconciliation_id"]),
        estimated_blast_radius=estimated_blast_radius,
        estimated_scope={
            "action_priority": recommendation["action_priority"],
            "requires_human_review": recommendation["requires_human_review"],
            "reversible": action_family in {"freeze_execution_family", "defer_change_family", "no_immediate_action"},
        },
    )


def _from_realized_validation(store: ArtifactStore, ref: str) -> CandidateAction:
    validation = store.read("realized_validation_results", ref)["record"]
    expectation = store.read("expectation_records", validation["expectation_id"])["record"]
    recommendation = validation["recommended_action"]
    if recommendation == "freeze_similar_changes":
        action_family = "defer_change_family"
        target_surface = "governance_change_family"
        proposed_effect = "defer similar governed changes until realized drift stabilizes"
        estimated_blast_radius = "moderate"
    elif recommendation == "rollback_review":
        action_family = "require_review"
        target_surface = "governance_change_family"
        proposed_effect = "require rollback review before further autonomous expansion"
        estimated_blast_radius = "moderate"
    elif recommendation == "review_required":
        action_family = "require_review"
        target_surface = "governance_change_family"
        proposed_effect = "surface realized-drift review"
        estimated_blast_radius = "safe"
    else:
        action_family = "no_immediate_action"
        target_surface = "governance_change_family"
        proposed_effect = "maintain current behavior"
        estimated_blast_radius = "safe"
    return _build_action(
        action_family=action_family,
        target_surface=target_surface,
        source_refs=(ref,),
        proposed_effect=proposed_effect,
        rationale=f"realized validation for {expectation['metric_name']} returned {validation['drift_classification']}",
        supporting_evidence_refs=(ref, validation["expectation_id"], expectation["evidence_ref"]),
        estimated_blast_radius=estimated_blast_radius,
        estimated_scope={
            "metric_name": expectation["metric_name"],
            "drift_classification": validation["drift_classification"],
            "reversible": action_family in {"defer_change_family", "no_immediate_action"},
        },
    )


def build_candidate_actions(
    store: ArtifactStore,
    *,
    adaptation_recommendation_refs: tuple[str, ...] = (),
    correction_recommendation_refs: tuple[str, ...] = (),
    realized_validation_refs: tuple[str, ...] = (),
) -> tuple[CandidateAction, ...]:
    actions: dict[str, CandidateAction] = {}
    for ref in sorted(adaptation_recommendation_refs):
        action = _from_adaptation_recommendation(store, ref)
        actions[action.candidate_action_id] = action
    for ref in sorted(correction_recommendation_refs):
        action = _from_correction_recommendation(store, ref)
        actions[action.candidate_action_id] = action
    for ref in sorted(realized_validation_refs):
        action = _from_realized_validation(store, ref)
        actions[action.candidate_action_id] = action
    return tuple(actions[key] for key in sorted(actions))
