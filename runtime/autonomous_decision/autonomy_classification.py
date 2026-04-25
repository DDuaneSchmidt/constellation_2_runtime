from __future__ import annotations

from ..meta_governance.store import ArtifactStore
from .schemas import content_hash
from .types import AutonomyClassification


PROTECTIVE_ACTIONS = {"freeze_execution_family", "defer_change_family"}
LIVE_CAPITAL_ACTIONS = {"tighten_guardrail", "rebalance_candidate", "close_position_candidate", "reduce_exposure_candidate"}


def _source_signals(store: ArtifactStore, refs: tuple[str, ...]) -> dict[str, object]:
    insufficient = False
    broker_truth_conflict = False
    confidence = "moderate"
    completeness = "partial"
    for ref in refs:
        if store.exists("regime_signals", ref):
            regime = store.read("regime_signals", ref)["record"]
            if regime["confidence_class"] == "insufficient_evidence":
                insufficient = True
            elif regime["confidence_class"] == "high":
                confidence = "high"
        if store.exists("drift_signals", ref):
            signal = store.read("drift_signals", ref)["record"]
            if signal["drift_classification"] == "persistent_systematic":
                confidence = "high"
                completeness = "strong"
        if store.exists("correction_recommendations", ref):
            recommendation = store.read("correction_recommendations", ref)["record"]
            if recommendation["action_type"] == "broker_review_required":
                broker_truth_conflict = True
            if recommendation["action_priority"] == "p0":
                confidence = "high"
                completeness = "strong"
        if store.exists("adaptation_recommendations", ref):
            recommendation = store.read("adaptation_recommendations", ref)["record"]
            if recommendation["recommendation_type"] == "collect_more_evidence":
                insufficient = True
        if store.exists("realized_validation_results", ref):
            validation = store.read("realized_validation_results", ref)["record"]
            if validation["drift_classification"] == "severe_drift":
                confidence = "high"
                completeness = "strong"
    if insufficient:
        return {
            "confidence_class": "insufficient_evidence",
            "evidence_completeness": "incomplete",
            "broker_truth_conflict": broker_truth_conflict,
        }
    return {
        "confidence_class": confidence,
        "evidence_completeness": completeness,
        "broker_truth_conflict": broker_truth_conflict,
    }


def classify_action(store: ArtifactStore, candidate_action_id: str) -> AutonomyClassification:
    action = store.read("candidate_actions", candidate_action_id)["record"]
    source = _source_signals(store, tuple(action["supporting_evidence_refs"]))
    reasons: list[str] = []
    autonomy_class = "approval_required"
    if source["confidence_class"] == "insufficient_evidence":
        autonomy_class = "advisory_only"
        reasons.append("insufficient_evidence")
    elif source["broker_truth_conflict"]:
        autonomy_class = "approval_required"
        reasons.append("broker_truth_conflict")
    elif action["action_family"] in PROTECTIVE_ACTIONS and source["evidence_completeness"] in {"sufficient", "strong", "partial"} and source["confidence_class"] in {"moderate", "high"}:
        autonomy_class = "auto_executable"
        reasons.append("protective_action_with_strong_evidence")
    elif action["action_family"] in LIVE_CAPITAL_ACTIONS or action["target_surface"] in {"execution", "allocation", "risk", "tax"}:
        autonomy_class = "approval_required"
        reasons.append("live_capital_or_risk_surface")
    elif action["action_family"] == "no_immediate_action":
        autonomy_class = "advisory_only"
        reasons.append("informational_action")
    elif action["action_family"] == "open_governance_proposal":
        autonomy_class = "approval_required"
        reasons.append("governance_action_requires_review")
    else:
        autonomy_class = "advisory_only"
        reasons.append("non_executable_surface")
    if source["confidence_class"] == "high":
        operator_visibility = "immediate_attention"
    elif autonomy_class == "advisory_only":
        operator_visibility = "visible_only"
    else:
        operator_visibility = "review_required"
    payload = {
        "candidate_action_id": candidate_action_id,
        "autonomy_class": autonomy_class,
        "classification_reasons": tuple(reasons),
        "confidence_class": source["confidence_class"],
        "evidence_completeness": source["evidence_completeness"],
        "operator_visibility": operator_visibility,
    }
    artifact_hash = content_hash(payload)
    return AutonomyClassification(
        candidate_action_id=candidate_action_id,
        autonomy_class=autonomy_class,
        classification_reasons=tuple(reasons),
        confidence_class=str(source["confidence_class"]),
        evidence_completeness=str(source["evidence_completeness"]),
        operator_visibility=operator_visibility,
        artifact_hash=artifact_hash,
    )
