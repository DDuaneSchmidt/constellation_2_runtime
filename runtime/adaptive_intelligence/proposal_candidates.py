from __future__ import annotations

from ..meta_governance.store import ArtifactStore
from .schemas import content_hash
from .types import ProposalCandidate


def _bundle_like_refs(store: ArtifactStore, refs: tuple[str, ...]) -> tuple[str, ...]:
    bundle_refs: list[str] = []
    for ref in refs:
        if store.exists("verification_bundles", ref) or store.exists("reconciliation_bundles", ref):
            bundle_refs.append(ref)
    return tuple(sorted(set(bundle_refs or refs)))


def build_proposal_candidates(
    store: ArtifactStore,
    *,
    recommendation_refs: tuple[str, ...],
    adaptation_bundle_ref: str,
) -> tuple[ProposalCandidate, ...]:
    candidates: list[ProposalCandidate] = []
    for ref in sorted(recommendation_refs):
        recommendation = store.read("adaptation_recommendations", ref)["record"]
        source_refs = tuple(recommendation["source_refs"])
        if not recommendation["governance_candidate_needed"]:
            continue
        if recommendation["recommendation_type"] == "tighten_execution_guardrails":
            target_tier = "tier_3"
            target_domain = "execution"
            candidate_change_type = "parameter_adjustment_candidate"
            expected_benefit = "tighter execution guardrails against persistent execution drift"
            worst_case_downside = "reduced execution flexibility under changing conditions"
        elif recommendation["recommendation_type"] == "review_thresholds":
            target_tier = "tier_2"
            target_domain = "execution"
            candidate_change_type = "policy_module_candidate"
            expected_benefit = "re-aligned policy thresholds under changed execution regime"
            worst_case_downside = "misclassification of transient conditions as regime change"
        else:
            target_tier = "tier_1"
            target_domain = recommendation["target_surface"]
            candidate_change_type = "protocol_review_candidate"
            expected_benefit = "governed review of adaptive concern"
            worst_case_downside = "unnecessary governance churn"
        payload = {
            "source_refs": source_refs + (ref,),
            "target_tier": target_tier,
            "target_domain": target_domain,
            "candidate_change_type": candidate_change_type,
            "evidence_bundle_refs": tuple(sorted(set(_bundle_like_refs(store, source_refs) + (adaptation_bundle_ref,)))),
        }
        artifact_hash = content_hash(payload)
        candidates.append(
            ProposalCandidate(
                proposal_candidate_id=f"proposal-candidate-{artifact_hash[:12]}",
                source_refs=tuple(sorted(set(payload["source_refs"]))),
                target_tier=target_tier,
                target_domain=target_domain,
                candidate_change_type=candidate_change_type,
                justification=recommendation["rationale"],
                expected_benefit=expected_benefit,
                worst_case_downside=worst_case_downside,
                evidence_bundle_refs=payload["evidence_bundle_refs"],
                requires_human_review=True,
                artifact_hash=artifact_hash,
            )
        )
    return tuple(candidates)
