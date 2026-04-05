from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SOURCE_ROOT = Path('/home/node/constellation_2_clean')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.advisor_bridge.promotion_gate_service import build_promotion_gate_result
from constellation_2.common.metadata_envelope_v1 import metadata_envelope_v1


def _envelope(selection_basis: str | None):
    return metadata_envelope_v1(
        produced_utc='2026-04-05T12:00:00Z',
        day_utc='2026-04-05',
        mode='paper',
        source_artifact_refs=['promotion_candidate_id:candidate1', 'promotion_review_id:review1', 'promotion_manual_review_id:manual1'],
        artifact_family='promotion_gate_result_v1',
        selection_basis=selection_basis,
    )


def test_promotion_gate_fails_closed_without_review_required() -> None:
    candidate = SimpleNamespace(candidate_id='candidate1', candidate_status='candidate')
    review = SimpleNamespace(review_id='review1', candidate_id='candidate1', review_status='rejected')
    manual_review = SimpleNamespace(manual_review_id='manual1', candidate_id='candidate1', review_id='review1', manual_review_status='rejected')

    with pytest.raises(ValueError, match='PROMOTION_REVIEW_NOT_REVIEW_REQUIRED'):
        build_promotion_gate_result(
            candidate=candidate,
            review=review,
            manual_review=manual_review,
            envelope=_envelope('rejected'),
        )


def test_promotion_gate_blocks_inconsistent_sources_and_missing_selection_basis() -> None:
    candidate = SimpleNamespace(candidate_id='candidate1', candidate_status='candidate')
    review = SimpleNamespace(review_id='review1', candidate_id='candidate1', review_status='review_required')
    inconsistent_manual_review = SimpleNamespace(manual_review_id='manual1', candidate_id='candidateX', review_id='reviewY', manual_review_status='needs_more_information')

    blocked_result = build_promotion_gate_result(
        candidate=candidate,
        review=review,
        manual_review=inconsistent_manual_review,
        envelope=_envelope('needs_more_information'),
    )
    assert blocked_result.gate_status == 'promotion_blocked'
    assert 'MANUAL_REVIEW_CANDIDATE_ID_MISMATCH' in blocked_result.reason_codes
    assert 'MANUAL_REVIEW_REVIEW_ID_MISMATCH' in blocked_result.reason_codes

    with pytest.raises(ValueError, match='SELECTION_BASIS_REQUIRED'):
        build_promotion_gate_result(
            candidate=candidate,
            review=review,
            manual_review=SimpleNamespace(manual_review_id='manual1', candidate_id='candidate1', review_id='review1', manual_review_status='needs_more_information'),
            envelope=_envelope(None),
        )
