from __future__ import annotations

from constellation_2.phaseD.lib.canon_json_v1 import canonical_sha256_hex_v1
from constellation_2.common.metadata_envelope_v1 import MetadataEnvelopeV1, artifact_base_v1
from constellation_2.common.advisor_bridge.promotion_candidate_v1 import PromotionCandidateV1
from constellation_2.common.advisor_bridge.promotion_gate_result_v1 import PromotionGateResultV1
from constellation_2.common.advisor_bridge.promotion_manual_review_v1 import PromotionManualReviewV1
from constellation_2.common.advisor_bridge.promotion_record_v1 import PromotionRecordV1
from constellation_2.common.advisor_bridge.promotion_review_v1 import PromotionReviewV1

_APPROVAL_STATE_BY_MANUAL_STATUS = {
    'approved_for_future_promotion': 'approved',
    'rejected': 'rejected',
    'needs_more_information': 'needs_more_information',
}

_EXECUTION_READINESS_BY_GATE_STATUS = {
    'promotion_ready': 'ready_for_trading_promotion',
    'promotion_rejected': 'rejected',
    'promotion_blocked': 'blocked',
}

_VALIDITY_STATUS_BY_GATE_STATUS = {
    'promotion_ready': 'VALID',
    'promotion_rejected': 'INVALID_REJECTED',
    'promotion_blocked': 'INVALID_BLOCKED',
}


def _scope_refs(candidate: PromotionCandidateV1) -> tuple[str, ...]:
    return (
        f'source_account:{candidate.source_account}',
        f'proposed_amount_cents:{candidate.proposed_amount_cents}',
        f'periodicity:{candidate.periodicity}',
    )


def build_promotion_record(*, candidate: PromotionCandidateV1, review: PromotionReviewV1, manual_review: PromotionManualReviewV1, gate_result: PromotionGateResultV1, envelope: MetadataEnvelopeV1) -> PromotionRecordV1:
    if candidate.candidate_id != review.candidate_id:
        raise ValueError('PROMOTION_RECORD_REVIEW_CANDIDATE_MISMATCH')
    if manual_review.candidate_id != candidate.candidate_id:
        raise ValueError('PROMOTION_RECORD_MANUAL_REVIEW_CANDIDATE_MISMATCH')
    if manual_review.review_id != review.review_id:
        raise ValueError('PROMOTION_RECORD_MANUAL_REVIEW_REVIEW_MISMATCH')
    if gate_result.candidate_id != candidate.candidate_id:
        raise ValueError('PROMOTION_RECORD_GATE_RESULT_CANDIDATE_MISMATCH')
    if gate_result.review_id != review.review_id:
        raise ValueError('PROMOTION_RECORD_GATE_RESULT_REVIEW_MISMATCH')
    if gate_result.manual_review_id != manual_review.manual_review_id:
        raise ValueError('PROMOTION_RECORD_GATE_RESULT_MANUAL_REVIEW_MISMATCH')
    if envelope.selection_basis is None:
        raise ValueError('PROMOTION_RECORD_SELECTION_BASIS_REQUIRED')
    if envelope.selection_basis != gate_result.gate_status:
        raise ValueError('PROMOTION_RECORD_SELECTION_BASIS_MISMATCH')

    approval_state = _APPROVAL_STATE_BY_MANUAL_STATUS.get(manual_review.manual_review_status)
    if approval_state is None:
        raise ValueError('PROMOTION_RECORD_MANUAL_STATUS_INVALID')
    execution_readiness_state = _EXECUTION_READINESS_BY_GATE_STATUS.get(gate_result.gate_status)
    if execution_readiness_state is None:
        raise ValueError('PROMOTION_RECORD_GATE_STATUS_INVALID')
    validity_status = _VALIDITY_STATUS_BY_GATE_STATUS[gate_result.gate_status]

    scope_refs = _scope_refs(candidate)
    eligible_scope = scope_refs if gate_result.gate_status == 'promotion_ready' else ()
    blocked_scope = () if gate_result.gate_status == 'promotion_ready' else scope_refs
    reason_codes = tuple(dict.fromkeys((*review.reason_codes, *gate_result.reason_codes)))
    idempotency_key = canonical_sha256_hex_v1({
        'candidate_id': candidate.candidate_id,
        'proposal_id': candidate.proposal_id,
        'planning_snapshot_id': candidate.planning_snapshot_id,
        'decision_plan_id': candidate.decision_plan_id,
        'source_account': candidate.source_account,
        'proposed_amount_cents': candidate.proposed_amount_cents,
        'periodicity': candidate.periodicity,
        'artifact_family': 'promotion_record_v1',
    })
    promotion_id = canonical_sha256_hex_v1({
        'idempotency_key': idempotency_key,
        'approval_state': approval_state,
        'execution_readiness_state': execution_readiness_state,
        'validity_status': validity_status,
        'artifact_family': 'promotion_record_v1',
    })

    obj = artifact_base_v1(schema_id='promotion_record', envelope=envelope)
    obj.update({
        'promotion_id': promotion_id,
        'candidate_id': candidate.candidate_id,
        'review_id': review.review_id,
        'manual_review_id': manual_review.manual_review_id,
        'gate_result_id': gate_result.gate_result_id,
        'proposal_id': candidate.proposal_id,
        'planning_snapshot_id': candidate.planning_snapshot_id,
        'decision_plan_id': candidate.decision_plan_id,
        'parent_lineage_refs': [
            f'proposal_id:{candidate.proposal_id}',
            f'planning_snapshot_id:{candidate.planning_snapshot_id}',
            f'decision_plan_id:{candidate.decision_plan_id}',
            f'promotion_candidate_id:{candidate.candidate_id}',
            f'promotion_review_id:{review.review_id}',
            f'promotion_manual_review_id:{manual_review.manual_review_id}',
            f'promotion_gate_result_id:{gate_result.gate_result_id}',
        ],
        'eligible_scope': list(eligible_scope),
        'blocked_scope': list(blocked_scope),
        'reason_codes': list(reason_codes),
        'approval_state': approval_state,
        'execution_readiness_state': execution_readiness_state,
        'idempotency_key': idempotency_key,
        'timestamp_utc': envelope.produced_utc,
        'validity_status': validity_status,
        'source_artifact_refs': list(envelope.source_artifact_refs),
        'notes': ['canonical_promotion_authority', 'legacy_promotion_artifacts_are_inputs_only'],
    })
    return PromotionRecordV1.from_dict(obj)
