from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

SOURCE_ROOT = Path('/home/node/constellation_2_clean')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.authority_registry_v1 import build_authority_registry
from constellation_2.common.advisor_bridge.promotion_manual_review_service import build_promotion_manual_review
from constellation_2.common.decision_chain_service import build_decision_chain
from constellation_2.common.metadata_envelope_v1 import metadata_envelope_v1


def _envelope(artifact_family: str, refs: list[str], selection_basis: str | None = None):
    return metadata_envelope_v1(
        produced_utc='2026-04-05T12:00:00Z',
        day_utc='2026-04-05',
        mode='paper',
        source_artifact_refs=refs,
        artifact_family=artifact_family,
        selection_basis=selection_basis,
    )


def test_manual_review_artifact_registry_and_decision_chain() -> None:
    candidate = SimpleNamespace(candidate_id='candidate1', candidate_status='candidate')
    review = SimpleNamespace(review_id='review1', candidate_id='candidate1', review_status='review_required')

    manual_review = build_promotion_manual_review(
        candidate=candidate,
        review=review,
        operator_id='operator_alpha',
        manual_review_status='approved_for_future_promotion',
        operator_notes='Reviewed and held for future promotion gating.',
        envelope=_envelope(
            'promotion_manual_review_v1',
            ['promotion_candidate_id:candidate1', 'promotion_review_id:review1'],
            selection_basis='approved_for_future_promotion',
        ),
    )

    assert manual_review.manual_review_status == 'approved_for_future_promotion'
    assert manual_review.selection_basis == 'approved_for_future_promotion'
    assert manual_review.operator_id == 'operator_alpha'
    manual_review_again = build_promotion_manual_review(
        candidate=candidate,
        review=review,
        operator_id='operator_alpha',
        manual_review_status='approved_for_future_promotion',
        operator_notes='Reviewed and held for future promotion gating.',
        envelope=_envelope(
            'promotion_manual_review_v1',
            ['promotion_candidate_id:candidate1', 'promotion_review_id:review1'],
            selection_basis='approved_for_future_promotion',
        ),
    )

    assert manual_review.to_dict() == manual_review_again.to_dict()

    registry = build_authority_registry(
        envelope=_envelope('authority_registry_v1', []),
    ).to_dict()
    row_map = {row['artifact_family']: row for row in registry['rows']}
    assert row_map['promotion_manual_review_v1'] == {
        'artifact_family': 'promotion_manual_review_v1',
        'owner_plane': 'promotion_plane',
        'write_root': '/tmp/constellation_2_foundation/advisor_runtime/<MODE>/promotion_manual_review_v1/<DAY>/',
        'authority_class': 'recommendation_authority',
        'publication_required': True,
        'promotion_required': False,
        'replay_expected': True,
        'downstream_consumers': ['future promotion gate only'],
        'notes': ['reviewed but non-executable promotion artifact'],
    }

    planning_snapshot = SimpleNamespace(planning_snapshot_id='ps1', advisory_packet_id='packet1')
    official_recommendation_set = SimpleNamespace(advisory_packet_id='packet1')
    decision_plan = SimpleNamespace(plan_id='dp1', planning_snapshot_id='ps1')
    chain = build_decision_chain(
        planning_snapshot=planning_snapshot,
        official_recommendation_set=official_recommendation_set,
        decision_plan=decision_plan,
        promotion_manual_review=manual_review,
        envelope=_envelope(
            'decision_chain_v1',
            ['planning_snapshot_id:ps1', 'official_recommendation_set_id:packet1', 'decision_plan_id:dp1', f'promotion_manual_review_id:{manual_review.manual_review_id}'],
        ),
    )

    assert chain.chain_status == 'manual_review_complete'
    assert chain.promotion_manual_review_id == manual_review.manual_review_id
    assert chain.promotion_candidate_id == 'candidate1'
    assert chain.promotion_review_id == 'review1'
