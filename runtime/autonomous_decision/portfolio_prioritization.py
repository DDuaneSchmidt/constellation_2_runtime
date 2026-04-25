from __future__ import annotations

from ..meta_governance.store import ArtifactStore
from .types import PrioritizedAction


IMPACT_SCORE = {"low": 0, "moderate": 2, "high": 4, "critical": 6}
URGENCY_SCORE = {"informational": 0, "near_term": 1, "urgent": 3, "immediate_attention": 5}
AUTONOMY_SCORE = {"advisory_only": 0, "approval_required": 1, "auto_executable": 2}


def _impact_for_action(store: ArtifactStore, action: dict, overrides: dict[str, dict] | None, require_explicit: bool) -> str:
    overrides = overrides or {}
    if action["candidate_action_id"] in overrides:
        return overrides[action["candidate_action_id"]]["impact_class"]
    for ref in action["supporting_evidence_refs"]:
        if store.exists("ranked_issues", ref):
            return store.read("ranked_issues", ref)["record"]["impact_class"]
        if store.exists("adaptation_recommendations", ref):
            recommendation = store.read("adaptation_recommendations", ref)["record"]
            for source_ref in recommendation["source_refs"]:
                if store.exists("ranked_issues", source_ref):
                    return store.read("ranked_issues", source_ref)["record"]["impact_class"]
        if store.exists("correction_recommendations", ref):
            priority = store.read("correction_recommendations", ref)["record"]["action_priority"]
            return {"p0": "critical", "p1": "high", "p2": "moderate", "p3": "low"}[priority]
    if require_explicit:
        raise ValueError(f"PRIORITIZATION_EXPOSURE_REQUIRED:{action['candidate_action_id']}")
    return "moderate"


def _urgency_for_action(store: ArtifactStore, action: dict, autonomy_class: str) -> str:
    if action["action_family"] == "freeze_execution_family":
        return "immediate_attention"
    for ref in action["supporting_evidence_refs"]:
        if store.exists("ranked_issues", ref):
            return store.read("ranked_issues", ref)["record"]["urgency_class"]
    if autonomy_class == "auto_executable":
        return "urgent"
    if autonomy_class == "approval_required":
        return "near_term"
    return "informational"


def prioritize(
    store: ArtifactStore,
    *,
    candidate_action_refs: tuple[str, ...],
    classification_refs: tuple[str, ...],
    exposure_overrides: dict[str, dict] | None = None,
    require_explicit_exposure: bool = False,
) -> tuple[PrioritizedAction, ...]:
    classes = {
        store.read("autonomy_classifications", ref)["record"]["candidate_action_id"]: store.read("autonomy_classifications", ref)["record"]
        for ref in classification_refs
    }
    provisional: list[dict] = []
    for ref in candidate_action_refs:
        action = store.read("candidate_actions", ref)["record"]
        classification = classes[action["candidate_action_id"]]
        impact_class = _impact_for_action(store, action, exposure_overrides, require_explicit_exposure)
        urgency_class = _urgency_for_action(store, action, classification["autonomy_class"])
        reversibility_score = 1 if action["estimated_scope"].get("reversible") else 0
        evidence_quality_score = {"incomplete": 0, "partial": 1, "sufficient": 2, "strong": 3}[classification["evidence_completeness"]]
        components = {
            "impact_score": IMPACT_SCORE[impact_class],
            "urgency_score": URGENCY_SCORE[urgency_class],
            "autonomy_score": AUTONOMY_SCORE[classification["autonomy_class"]],
            "reversibility_score": reversibility_score,
            "evidence_quality_score": evidence_quality_score,
        }
        components["score_value"] = sum(components.values())
        provisional.append(
            {
                "candidate_action_ref": ref,
                "autonomy_class": classification["autonomy_class"],
                "urgency_class": urgency_class,
                "impact_class": impact_class,
                "rank_components": components,
                "explanation": action["rationale"],
            }
        )
    provisional.sort(key=lambda item: (-item["rank_components"]["score_value"], item["candidate_action_ref"]))
    prioritized: list[PrioritizedAction] = []
    for index, item in enumerate(provisional, start=1):
        prioritized.append(
            PrioritizedAction(
                prioritized_action_id=f"prioritized-action-{item['candidate_action_ref']}",
                candidate_action_ref=item["candidate_action_ref"],
                autonomy_class=item["autonomy_class"],
                urgency_class=item["urgency_class"],
                impact_class=item["impact_class"],
                rank_components=item["rank_components"],
                final_rank=index,
                explanation=item["explanation"],
            )
        )
    return tuple(prioritized)
