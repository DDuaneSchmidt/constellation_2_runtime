from __future__ import annotations

from constellation_2.phaseD.lib.canon_json_v1 import canonical_sha256_hex_v1
from constellation_2.common.metadata_envelope_v1 import MetadataEnvelopeV1, artifact_base_v1
from constellation_2.common.advisor_execution.decision_plan_delta_v1 import DecisionPlanDeltaV1
from constellation_2.common.advisor_execution.decision_plan_v1 import DecisionPlanV1
from constellation_2.common.advisor_execution.official_recommendation_set_v1 import OfficialRecommendationSetV1
from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1
from constellation_2.common.advisor_bridge.advisor_trade_intent_proposal_v1 import AdvisorTradeIntentProposalV1
from constellation_2.common.advisor_bridge.advisor_trade_translation_v1 import AdvisorTradeTranslationV1
from constellation_2.common.advisor_bridge.promotion_candidate_v1 import PromotionCandidateV1
from constellation_2.common.advisor_bridge.promotion_gate_result_v1 import PromotionGateResultV1
from constellation_2.common.advisor_bridge.promotion_manual_review_v1 import PromotionManualReviewV1
from constellation_2.common.advisor_bridge.promotion_review_v1 import PromotionReviewV1
from constellation_2.common.decision_chain_v1 import DecisionChainV1


def build_decision_chain(*, planning_snapshot: PlanningSnapshotV1, official_recommendation_set: OfficialRecommendationSetV1, decision_plan: DecisionPlanV1, envelope: MetadataEnvelopeV1, decision_plan_delta: DecisionPlanDeltaV1 | None = None, bridge_translation: AdvisorTradeTranslationV1 | None = None, bridge_proposal: AdvisorTradeIntentProposalV1 | None = None, promotion_candidate: PromotionCandidateV1 | None = None, promotion_review: PromotionReviewV1 | None = None, promotion_manual_review: PromotionManualReviewV1 | None = None, promotion_gate_result: PromotionGateResultV1 | None = None) -> DecisionChainV1:
    if official_recommendation_set.advisory_packet_id != planning_snapshot.advisory_packet_id:
        raise ValueError('ADVISORY_PACKET_ID_MISMATCH')
    if decision_plan.planning_snapshot_id != planning_snapshot.planning_snapshot_id:
        raise ValueError('PLANNING_SNAPSHOT_ID_MISMATCH')
    if decision_plan_delta is not None and decision_plan_delta.current_plan_id != decision_plan.plan_id:
        raise ValueError('DECISION_PLAN_DELTA_MISMATCH')
    if promotion_review is not None and promotion_candidate is None and promotion_manual_review is None and promotion_gate_result is None:
        raise ValueError('PROMOTION_REVIEW_WITHOUT_CANDIDATE')
    derived_candidate_id = None if promotion_candidate is None else promotion_candidate.candidate_id
    derived_review_id = None if promotion_review is None else promotion_review.review_id
    derived_manual_review_id = None if promotion_manual_review is None else promotion_manual_review.manual_review_id
    derived_gate_result_id = None if promotion_gate_result is None else promotion_gate_result.gate_result_id
    if promotion_manual_review is not None:
        if promotion_candidate is not None and promotion_manual_review.candidate_id != promotion_candidate.candidate_id:
            raise ValueError('PROMOTION_MANUAL_REVIEW_CANDIDATE_MISMATCH')
        if promotion_review is not None and promotion_manual_review.review_id != promotion_review.review_id:
            raise ValueError('PROMOTION_MANUAL_REVIEW_REVIEW_MISMATCH')
        if derived_candidate_id is None:
            derived_candidate_id = promotion_manual_review.candidate_id
        if derived_review_id is None:
            derived_review_id = promotion_manual_review.review_id
    elif promotion_review is not None and promotion_candidate is not None and promotion_review.candidate_id != promotion_candidate.candidate_id:
        raise ValueError('PROMOTION_REVIEW_CANDIDATE_MISMATCH')
    if promotion_gate_result is not None:
        if derived_candidate_id is not None and promotion_gate_result.candidate_id != derived_candidate_id:
            raise ValueError('PROMOTION_GATE_RESULT_CANDIDATE_MISMATCH')
        if derived_review_id is not None and promotion_gate_result.review_id != derived_review_id:
            raise ValueError('PROMOTION_GATE_RESULT_REVIEW_MISMATCH')
        if derived_manual_review_id is not None and promotion_gate_result.manual_review_id != derived_manual_review_id:
            raise ValueError('PROMOTION_GATE_RESULT_MANUAL_REVIEW_MISMATCH')
        if derived_candidate_id is None:
            derived_candidate_id = promotion_gate_result.candidate_id
        if derived_review_id is None:
            derived_review_id = promotion_gate_result.review_id
        if derived_manual_review_id is None:
            derived_manual_review_id = promotion_gate_result.manual_review_id
    if promotion_gate_result is not None:
        if promotion_gate_result.gate_status == 'promotion_ready':
            chain_status = 'gate_ready'
        else:
            chain_status = 'gate_blocked'
    elif promotion_manual_review is not None:
        chain_status = 'manual_review_complete'
    elif promotion_review is not None and promotion_review.review_status in {'rejected', 'insufficient_basis'}:
        chain_status = 'promotion_blocked'
    elif promotion_candidate is not None and promotion_review is not None and promotion_review.review_status == 'review_required':
        chain_status = 'promotion_ready'
    elif bridge_translation is not None or bridge_proposal is not None:
        chain_status = 'bridge_ready'
    else:
        chain_status = 'decision_only'
    chain_id = canonical_sha256_hex_v1({
        'planning_snapshot_id': planning_snapshot.planning_snapshot_id,
        'decision_plan_id': decision_plan.plan_id,
        'decision_plan_delta_id': None if decision_plan_delta is None else decision_plan_delta.plan_delta_id,
        'bridge_translation_id': None if bridge_translation is None else bridge_translation.run_id,
        'bridge_proposal_id': None if bridge_proposal is None else bridge_proposal.proposal_id,
        'promotion_candidate_id': derived_candidate_id,
        'promotion_review_id': derived_review_id,
        'promotion_manual_review_id': derived_manual_review_id,
        'promotion_gate_result_id': derived_gate_result_id,
        'chain_status': chain_status,
    })
    obj = artifact_base_v1(schema_id='decision_chain', envelope=envelope)
    obj.update({
        'chain_id': chain_id,
        'planning_snapshot_id': planning_snapshot.planning_snapshot_id,
        'advisory_packet_id': planning_snapshot.advisory_packet_id,
        'decision_plan_id': decision_plan.plan_id,
        'decision_plan_delta_id': None if decision_plan_delta is None else decision_plan_delta.plan_delta_id,
        'bridge_translation_id': None if bridge_translation is None else bridge_translation.run_id,
        'bridge_proposal_id': None if bridge_proposal is None else bridge_proposal.proposal_id,
        'promotion_candidate_id': derived_candidate_id,
        'promotion_review_id': derived_review_id,
        'promotion_manual_review_id': derived_manual_review_id,
        'promotion_gate_result_id': derived_gate_result_id,
        'chain_status': chain_status,
        'source_artifact_refs': list(envelope.source_artifact_refs),
    })
    return DecisionChainV1.from_dict(obj)
