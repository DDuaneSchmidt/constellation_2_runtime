from __future__ import annotations

from constellation_2.phaseD.lib.canon_json_v1 import canonical_sha256_hex_v1
from constellation_2.common.metadata_envelope_v1 import MetadataEnvelopeV1, artifact_base_v1
from constellation_2.common.advisor_execution.decision_plan_v1 import DecisionPlanV1
from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1
from constellation_2.common.advisor_bridge.advisor_trade_intent_proposal_v1 import AdvisorTradeIntentProposalV1
from constellation_2.common.advisor_bridge.advisor_trade_translation_v1 import AdvisorTradeTranslationV1
from constellation_2.common.advisor_bridge.promotion_candidate_v1 import PromotionCandidateV1
from constellation_2.common.advisor_bridge.promotion_review_v1 import PromotionReviewV1


def build_promotion_candidate(*, planning_snapshot: PlanningSnapshotV1, decision_plan: DecisionPlanV1, translation: AdvisorTradeTranslationV1, proposal: AdvisorTradeIntentProposalV1, envelope: MetadataEnvelopeV1) -> PromotionCandidateV1:
    if proposal.translation_status != 'proposed':
        raise ValueError('PROPOSAL_NOT_PROPOSED')
    if proposal.proposal_class != 'withdrawal_candidate':
        raise ValueError('PROPOSAL_CLASS_NOT_ELIGIBLE')
    if planning_snapshot.planning_snapshot_id != proposal.planning_snapshot_id or decision_plan.plan_id != proposal.decision_plan_id:
        raise ValueError('PROPOSAL_IDENTITY_MISMATCH')
    if translation.planning_snapshot_id != planning_snapshot.planning_snapshot_id or translation.decision_plan_id != decision_plan.plan_id:
        raise ValueError('TRANSLATION_IDENTITY_MISMATCH')
    matching = [item for item in translation.translations if item.decision_action_id == proposal.decision_action_id]
    if len(matching) != 1:
        raise ValueError('TRANSLATION_PROPOSAL_LINK_MISSING')
    row = matching[0]
    if row.translation_status != 'proposed':
        raise ValueError('TRANSLATION_NOT_PROPOSED')
    candidate_id = canonical_sha256_hex_v1({'proposal_id': proposal.proposal_id, 'planning_snapshot_id': proposal.planning_snapshot_id, 'decision_plan_id': proposal.decision_plan_id, 'candidate_class': 'withdrawal_candidate'})
    obj = artifact_base_v1(schema_id='promotion_candidate', envelope=envelope)
    obj.update({'candidate_id': candidate_id, 'proposal_id': proposal.proposal_id, 'planning_snapshot_id': proposal.planning_snapshot_id, 'decision_plan_id': proposal.decision_plan_id, 'candidate_status': 'candidate', 'candidate_class': 'withdrawal_candidate', 'source_account': proposal.source_account, 'proposed_amount_cents': proposal.proposed_amount_cents, 'periodicity': proposal.periodicity, 'source_artifact_refs': list(envelope.source_artifact_refs), 'notes': ['promotion_plane_boundary_only']})
    return PromotionCandidateV1.from_dict(obj)


def build_promotion_review(*, candidate: PromotionCandidateV1, envelope: MetadataEnvelopeV1) -> PromotionReviewV1:
    if candidate.candidate_status != 'candidate':
        status = 'insufficient_basis'
        reasons = ['CANDIDATE_STATUS_NOT_CANDIDATE']
    elif candidate.candidate_class != 'withdrawal_candidate':
        status = 'rejected'
        reasons = ['CANDIDATE_CLASS_UNSUPPORTED']
    else:
        status = 'review_required'
        reasons = ['MANUAL_PROMOTION_REVIEW_REQUIRED']
    review_id = canonical_sha256_hex_v1({'candidate_id': candidate.candidate_id, 'review_status': status, 'artifact_family': 'promotion_review_v1'})
    obj = artifact_base_v1(schema_id='promotion_review', envelope=envelope)
    obj.update({'review_id': review_id, 'candidate_id': candidate.candidate_id, 'review_status': status, 'reason_codes': reasons, 'source_artifact_refs': list(envelope.source_artifact_refs), 'notes': ['never_approved_in_this_phase']})
    return PromotionReviewV1.from_dict(obj)
