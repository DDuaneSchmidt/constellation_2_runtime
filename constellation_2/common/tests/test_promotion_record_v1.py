from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.advisor_bridge.promotion_gate_service import build_promotion_gate_result
from constellation_2.common.advisor_bridge.promotion_record_service import build_promotion_record
from constellation_2.common.metadata_envelope_v1 import metadata_envelope_v1


def _envelope(artifact_family: str, refs: list[str], selection_basis: str | None):
    return metadata_envelope_v1(
        produced_utc='2026-04-05T12:00:00Z',
        day_utc='2026-04-05',
        mode='paper',
        source_artifact_refs=refs,
        artifact_family=artifact_family,
        selection_basis=selection_basis,
    )


def test_promotion_record_blocks_scope_and_sets_invalid_status() -> None:
    candidate = SimpleNamespace(
        candidate_id='candidate1',
        proposal_id='proposal1',
        planning_snapshot_id='planning1',
        decision_plan_id='decision1',
        candidate_status='candidate',
        source_account='paper-account',
        proposed_amount_cents=1000,
        periodicity='monthly',
    )
    review = SimpleNamespace(
        review_id='review1',
        candidate_id='candidate1',
        review_status='review_required',
        reason_codes=('REQUIRES_MANUAL_REVIEW',),
    )
    manual_review = SimpleNamespace(
        manual_review_id='manual1',
        candidate_id='candidate1',
        review_id='review1',
        manual_review_status='needs_more_information',
    )

    gate_result = build_promotion_gate_result(
        candidate=candidate,
        review=review,
        manual_review=manual_review,
        envelope=_envelope(
            'promotion_gate_result_v1',
            ['promotion_candidate_id:candidate1', 'promotion_review_id:review1', 'promotion_manual_review_id:manual1'],
            selection_basis='needs_more_information',
        ),
    )
    record = build_promotion_record(
        candidate=candidate,
        review=review,
        manual_review=manual_review,
        gate_result=gate_result,
        envelope=_envelope(
            'promotion_record_v1',
            [
                'promotion_candidate_id:candidate1',
                'promotion_review_id:review1',
                'promotion_manual_review_id:manual1',
                f'promotion_gate_result_id:{gate_result.gate_result_id}',
            ],
            selection_basis='promotion_blocked',
        ),
    )

    assert record.execution_readiness_state == 'blocked'
    assert record.validity_status == 'INVALID_BLOCKED'
    assert record.eligible_scope == ()
    assert record.blocked_scope == (
        'source_account:paper-account',
        'proposed_amount_cents:1000',
        'periodicity:monthly',
    )


def test_promotion_record_fails_closed_on_selection_basis_mismatch() -> None:
    candidate = SimpleNamespace(
        candidate_id='candidate1',
        proposal_id='proposal1',
        planning_snapshot_id='planning1',
        decision_plan_id='decision1',
        candidate_status='candidate',
        source_account='paper-account',
        proposed_amount_cents=1000,
        periodicity='monthly',
    )
    review = SimpleNamespace(
        review_id='review1',
        candidate_id='candidate1',
        review_status='review_required',
        reason_codes=('REQUIRES_MANUAL_REVIEW',),
    )
    manual_review = SimpleNamespace(
        manual_review_id='manual1',
        candidate_id='candidate1',
        review_id='review1',
        manual_review_status='approved_for_future_promotion',
    )
    gate_result = build_promotion_gate_result(
        candidate=candidate,
        review=review,
        manual_review=manual_review,
        envelope=_envelope(
            'promotion_gate_result_v1',
            ['promotion_candidate_id:candidate1', 'promotion_review_id:review1', 'promotion_manual_review_id:manual1'],
            selection_basis='approved_for_future_promotion',
        ),
    )

    with pytest.raises(ValueError, match='PROMOTION_RECORD_SELECTION_BASIS_MISMATCH'):
        build_promotion_record(
            candidate=candidate,
            review=review,
            manual_review=manual_review,
            gate_result=gate_result,
            envelope=_envelope(
                'promotion_record_v1',
                [
                    'promotion_candidate_id:candidate1',
                    'promotion_review_id:review1',
                    'promotion_manual_review_id:manual1',
                    f'promotion_gate_result_id:{gate_result.gate_result_id}',
                ],
                selection_basis='promotion_blocked',
            ),
        )
