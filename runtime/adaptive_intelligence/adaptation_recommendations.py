from __future__ import annotations

from ..meta_governance.store import ArtifactStore
from .schemas import content_hash
from .types import AdaptationRecommendation


def build_recommendations(
    store: ArtifactStore,
    *,
    ranked_issue_refs: tuple[str, ...],
) -> tuple[AdaptationRecommendation, ...]:
    recommendations: list[AdaptationRecommendation] = []
    for ref in sorted(ranked_issue_refs):
        issue = store.read("ranked_issues", ref)["record"]
        source_ref = issue["source_refs"][0]
        if issue["issue_type"] in {"persistent_drift", "execution_quality_problem"} and issue["severity"] in {"material", "critical"}:
            recommendation_type = "tighten_execution_guardrails"
            target_surface = "execution"
            governance_candidate_needed = True
            requires_human_review = True
            priority_class = "high"
        elif issue["issue_type"] == "tax_truth_problem":
            recommendation_type = "investigate_broker_truth"
            target_surface = "tax"
            governance_candidate_needed = False
            requires_human_review = True
            priority_class = "high"
        elif issue["issue_type"] == "state_truth_problem":
            recommendation_type = "rebuild_state_surface"
            target_surface = "state"
            governance_candidate_needed = False
            requires_human_review = True
            priority_class = "high"
        elif issue["issue_type"] == "regime_shift" and issue["severity"] == "informational":
            recommendation_type = "collect_more_evidence"
            target_surface = "regime"
            governance_candidate_needed = False
            requires_human_review = False
            priority_class = "low"
        elif issue["issue_type"] == "regime_shift":
            recommendation_type = "review_thresholds"
            target_surface = "execution"
            governance_candidate_needed = True
            requires_human_review = True
            priority_class = "high"
        else:
            recommendation_type = "no_action"
            target_surface = "global"
            governance_candidate_needed = False
            requires_human_review = False
            priority_class = "low"
        payload = {
            "recommendation_type": recommendation_type,
            "source_refs": (ref, source_ref),
            "target_surface": target_surface,
            "recommended_action": recommendation_type,
            "priority_class": priority_class,
            "requires_human_review": requires_human_review,
            "governance_candidate_needed": governance_candidate_needed,
        }
        artifact_hash = content_hash(payload)
        recommendations.append(
            AdaptationRecommendation(
                recommendation_id=f"adaptive-recommendation-{artifact_hash[:12]}",
                recommendation_type=recommendation_type,
                source_refs=payload["source_refs"],
                target_surface=target_surface,
                rationale=issue["explanation"],
                recommended_action=recommendation_type,
                priority_class=priority_class,
                requires_human_review=requires_human_review,
                governance_candidate_needed=governance_candidate_needed,
                artifact_hash=artifact_hash,
            )
        )
    unique: dict[str, AdaptationRecommendation] = {}
    for recommendation in recommendations:
        unique[recommendation.recommendation_id] = recommendation
    return tuple(unique[key] for key in sorted(unique))
