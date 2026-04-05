from __future__ import annotations

from constellation_2.phaseD.lib.canon_json_v1 import canonical_sha256_hex_v1
from constellation_2.common.metadata_envelope_v1 import MetadataEnvelopeV1, artifact_base_v1
from constellation_2.common.advisor_bridge.promotion_candidate_v1 import PromotionCandidateV1
from constellation_2.common.advisor_bridge.promotion_manual_review_v1 import PromotionManualReviewV1
from constellation_2.common.advisor_bridge.promotion_review_v1 import PromotionReviewV1

_ALLOWED_STATUSES = {
    'approved_for_future_promotion',
    'rejected',
    'needs_more_information',
}


def build_promotion_manual_review(*, candidate: PromotionCandidateV1, review: PromotionReviewV1, operator_id: str, manual_review_status: str, operator_notes: str, envelope: MetadataEnvelopeV1) -> PromotionManualReviewV1:
    if review.review_status != 'review_required':
        raise ValueError('PROMOTION_REVIEW_NOT_REVIEW_REQUIRED')
    if candidate.candidate_id != review.candidate_id:
        raise ValueError('PROMOTION_REVIEW_CANDIDATE_MISMATCH')
    if candidate.candidate_status != 'candidate':
        raise ValueError('PROMOTION_CANDIDATE_NOT_CANDIDATE')
    if not operator_id:
        raise ValueError('OPERATOR_ID_REQUIRED')
    if manual_review_status not in _ALLOWED_STATUSES:
        raise ValueError('MANUAL_REVIEW_STATUS_INVALID')
    if envelope.selection_basis is None:
        raise ValueError('SELECTION_BASIS_REQUIRED')
    if envelope.selection_basis != manual_review_status:
        raise ValueError('SELECTION_BASIS_STATUS_MISMATCH')
    manual_review_id = canonical_sha256_hex_v1({
        'candidate_id': candidate.candidate_id,
        'review_id': review.review_id,
        'operator_id': operator_id,
        'manual_review_status': manual_review_status,
        'operator_notes': operator_notes,
        'artifact_family': 'promotion_manual_review_v1',
    })
    obj = artifact_base_v1(schema_id='promotion_manual_review', envelope=envelope)
    obj.update({
        'manual_review_id': manual_review_id,
        'candidate_id': candidate.candidate_id,
        'review_id': review.review_id,
        'manual_review_status': manual_review_status,
        'operator_id': operator_id,
        'operator_notes': operator_notes,
        'source_artifact_refs': list(envelope.source_artifact_refs),
        'selection_basis': envelope.selection_basis,
    })
    return PromotionManualReviewV1.from_dict(obj)
