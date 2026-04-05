from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.advisor_bridge.promotion_manual_review_service import build_promotion_manual_review
from constellation_2.common.metadata_envelope_v1 import metadata_envelope_v1


def _envelope(selection_basis: str | None = 'rejected'):
    return metadata_envelope_v1(
        produced_utc='2026-04-05T12:00:00Z',
        day_utc='2026-04-05',
        mode='paper',
        source_artifact_refs=['promotion_candidate_id:candidate1', 'promotion_review_id:review1'],
        artifact_family='promotion_manual_review_v1',
        selection_basis=selection_basis,
    )


def test_manual_review_fails_closed_without_review_required() -> None:
    candidate = SimpleNamespace(candidate_id='candidate1', candidate_status='candidate')
    review = SimpleNamespace(review_id='review1', candidate_id='candidate1', review_status='rejected')

    with pytest.raises(ValueError, match='PROMOTION_REVIEW_NOT_REVIEW_REQUIRED'):
        build_promotion_manual_review(
            candidate=candidate,
            review=review,
            operator_id='operator_alpha',
            manual_review_status='rejected',
            operator_notes='Rejected pending more evidence.',
            envelope=_envelope('rejected'),
        )


def test_manual_review_fails_closed_without_operator_or_selection_basis() -> None:
    candidate = SimpleNamespace(candidate_id='candidate1', candidate_status='candidate')
    review = SimpleNamespace(review_id='review1', candidate_id='candidate1', review_status='review_required')

    with pytest.raises(ValueError, match='OPERATOR_ID_REQUIRED'):
        build_promotion_manual_review(
            candidate=candidate,
            review=review,
            operator_id='',
            manual_review_status='needs_more_information',
            operator_notes='Need more supporting records.',
            envelope=_envelope('needs_more_information'),
        )

    with pytest.raises(ValueError, match='SELECTION_BASIS_REQUIRED'):
        build_promotion_manual_review(
            candidate=candidate,
            review=review,
            operator_id='operator_alpha',
            manual_review_status='needs_more_information',
            operator_notes='Need more supporting records.',
            envelope=_envelope(None),
        )
