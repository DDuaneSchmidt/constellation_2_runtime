from __future__ import annotations

from constellation_2.phaseD.lib.canon_json_v1 import canonical_sha256_hex_v1
from constellation_2.common.metadata_envelope_v1 import MetadataEnvelopeV1, artifact_base_v1
from constellation_2.common.advisor_bridge.promotion_candidate_v1 import PromotionCandidateV1
from constellation_2.common.advisor_bridge.promotion_gate_result_v1 import PromotionGateResultV1
from constellation_2.common.advisor_bridge.promotion_manual_review_v1 import PromotionManualReviewV1
from constellation_2.common.advisor_bridge.promotion_review_v1 import PromotionReviewV1

_ALLOWED_MANUAL_STATUSES = {
    'approved_for_future_promotion',
    'rejected',
    'needs_more_information',
}


def build_promotion_gate_result(*, candidate: PromotionCandidateV1, review: PromotionReviewV1, manual_review: PromotionManualReviewV1, envelope: MetadataEnvelopeV1) -> PromotionGateResultV1:
    if review.review_status != 'review_required':
        raise ValueError('PROMOTION_REVIEW_NOT_REVIEW_REQUIRED')
    if manual_review.manual_review_status not in _ALLOWED_MANUAL_STATUSES:
        raise ValueError('MANUAL_REVIEW_STATUS_INVALID')
    if envelope.selection_basis is None:
        raise ValueError('SELECTION_BASIS_REQUIRED')
    if envelope.selection_basis != manual_review.manual_review_status:
        raise ValueError('SELECTION_BASIS_STATUS_MISMATCH')
    reason_codes: list[str] = []
    notes: list[str] = ['promotion_gate_artifact_only']
    inconsistent = False
    if candidate.candidate_id != review.candidate_id:
        inconsistent = True
        reason_codes.append('REVIEW_CANDIDATE_ID_MISMATCH')
    if manual_review.candidate_id != candidate.candidate_id:
        inconsistent = True
        reason_codes.append('MANUAL_REVIEW_CANDIDATE_ID_MISMATCH')
    if manual_review.review_id != review.review_id:
        inconsistent = True
        reason_codes.append('MANUAL_REVIEW_REVIEW_ID_MISMATCH')
    if inconsistent:
        gate_status = 'promotion_blocked'
        notes.append('source_linkage_inconsistent')
    elif manual_review.manual_review_status == 'approved_for_future_promotion' and candidate.candidate_status == 'candidate' and review.review_status == 'review_required':
        gate_status = 'promotion_ready'
        reason_codes.append('MANUAL_REVIEW_APPROVED')
    elif manual_review.manual_review_status == 'rejected':
        gate_status = 'promotion_rejected'
        reason_codes.append('MANUAL_REVIEW_REJECTED')
    elif review.review_status in {'rejected', 'insufficient_basis'}:
        gate_status = 'promotion_rejected'
        reason_codes.append('PROMOTION_REVIEW_REJECTED')
    else:
        gate_status = 'promotion_blocked'
        reason_codes.append('NEEDS_MORE_INFORMATION')
    gate_result_id = canonical_sha256_hex_v1({
        'candidate_id': candidate.candidate_id,
        'review_id': review.review_id,
        'manual_review_id': manual_review.manual_review_id,
        'gate_status': gate_status,
        'artifact_family': 'promotion_gate_result_v1',
    })
    obj = artifact_base_v1(schema_id='promotion_gate_result', envelope=envelope)
    obj.update({
        'gate_result_id': gate_result_id,
        'candidate_id': candidate.candidate_id,
        'review_id': review.review_id,
        'manual_review_id': manual_review.manual_review_id,
        'gate_status': gate_status,
        'reason_codes': reason_codes,
        'source_artifact_refs': list(envelope.source_artifact_refs),
        'selection_basis': envelope.selection_basis,
        'notes': notes,
    })
    return PromotionGateResultV1.from_dict(obj)
