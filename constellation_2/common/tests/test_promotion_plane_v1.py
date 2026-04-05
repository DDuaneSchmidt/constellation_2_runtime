from __future__ import annotations

import sys
from pathlib import Path

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from types import SimpleNamespace

import pytest

from constellation_2.common.advisor_bridge.promotion_plane_service import build_promotion_candidate, build_promotion_review
from constellation_2.common.metadata_envelope_v1 import metadata_envelope_v1


def _envelope(artifact_family: str, refs: list[str]):
    return metadata_envelope_v1(
        produced_utc='2026-04-05T12:00:00Z',
        day_utc='2026-04-05',
        mode='paper',
        source_artifact_refs=refs,
        artifact_family=artifact_family,
    )


def test_promotion_plane_emits_candidate_and_review_required() -> None:
    planning_snapshot = SimpleNamespace(planning_snapshot_id='ps1')
    decision_plan = SimpleNamespace(plan_id='dp1')
    translation = SimpleNamespace(
        planning_snapshot_id='ps1',
        decision_plan_id='dp1',
        translations=(SimpleNamespace(decision_action_id='da1', translation_status='proposed'),),
    )
    proposal = SimpleNamespace(
        proposal_id='proposal1',
        planning_snapshot_id='ps1',
        decision_plan_id='dp1',
        decision_action_id='da1',
        translation_status='proposed',
        proposal_class='withdrawal_candidate',
        source_account='taxable_account',
        proposed_amount_cents=500000,
        periodicity='annual',
    )

    candidate = build_promotion_candidate(
        planning_snapshot=planning_snapshot,
        decision_plan=decision_plan,
        translation=translation,
        proposal=proposal,
        envelope=_envelope(
            'promotion_candidate_v1',
            ['planning_snapshot_id:ps1', 'decision_plan_id:dp1', 'bridge_translation_id:tr1', 'bridge_proposal_id:proposal1'],
        ),
    )

    review = build_promotion_review(
        candidate=candidate,
        envelope=_envelope('promotion_review_v1', [f'promotion_candidate_id:{candidate.candidate_id}']),
    )

    assert candidate.candidate_status == 'candidate'
    assert candidate.candidate_class == 'withdrawal_candidate'
    assert review.review_status == 'review_required'
    assert 'approved' not in review.review_status


def test_promotion_plane_fails_closed_for_ineligible_proposal() -> None:
    planning_snapshot = SimpleNamespace(planning_snapshot_id='ps1')
    decision_plan = SimpleNamespace(plan_id='dp1')
    translation = SimpleNamespace(
        planning_snapshot_id='ps1',
        decision_plan_id='dp1',
        translations=(SimpleNamespace(decision_action_id='da1', translation_status='blocked'),),
    )
    proposal = SimpleNamespace(
        proposal_id='proposal1',
        planning_snapshot_id='ps1',
        decision_plan_id='dp1',
        decision_action_id='da1',
        translation_status='blocked',
        proposal_class='withdrawal_candidate',
        source_account='taxable_account',
        proposed_amount_cents=500000,
        periodicity='annual',
    )

    with pytest.raises(ValueError, match='PROPOSAL_NOT_PROPOSED'):
        build_promotion_candidate(
            planning_snapshot=planning_snapshot,
            decision_plan=decision_plan,
            translation=translation,
            proposal=proposal,
            envelope=_envelope(
                'promotion_candidate_v1',
                ['planning_snapshot_id:ps1', 'decision_plan_id:dp1', 'bridge_translation_id:tr1', 'bridge_proposal_id:proposal1'],
            ),
        )
