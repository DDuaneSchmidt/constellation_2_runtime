from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.authority_registry_v1 import build_authority_registry
from constellation_2.common.advisor_bridge.promotion_gate_service import build_promotion_gate_result
from constellation_2.common.advisor_bridge.promotion_record_service import build_promotion_record
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


def test_promotion_gate_ready_is_deterministic_and_updates_chain() -> None:
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
    manual_review = SimpleNamespace(manual_review_id='manual1', candidate_id='candidate1', review_id='review1', manual_review_status='approved_for_future_promotion')

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
    gate_result_again = build_promotion_gate_result(
        candidate=candidate,
        review=review,
        manual_review=manual_review,
        envelope=_envelope(
            'promotion_gate_result_v1',
            ['promotion_candidate_id:candidate1', 'promotion_review_id:review1', 'promotion_manual_review_id:manual1'],
            selection_basis='approved_for_future_promotion',
        ),
    )

    assert gate_result.gate_status == 'promotion_ready'
    assert gate_result.to_dict() == gate_result_again.to_dict()

    promotion_record = build_promotion_record(
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
            selection_basis='promotion_ready',
        ),
    )
    promotion_record_again = build_promotion_record(
        candidate=candidate,
        review=review,
        manual_review=manual_review,
        gate_result=gate_result_again,
        envelope=_envelope(
            'promotion_record_v1',
            [
                'promotion_candidate_id:candidate1',
                'promotion_review_id:review1',
                'promotion_manual_review_id:manual1',
                f'promotion_gate_result_id:{gate_result_again.gate_result_id}',
            ],
            selection_basis='promotion_ready',
        ),
    )
    assert promotion_record.execution_readiness_state == 'ready_for_trading_promotion'
    assert promotion_record.approval_state == 'approved'
    assert promotion_record.validity_status == 'VALID'
    assert promotion_record.eligible_scope == (
        'source_account:paper-account',
        'proposed_amount_cents:1000',
        'periodicity:monthly',
    )
    assert promotion_record.blocked_scope == ()
    assert promotion_record.to_dict() == promotion_record_again.to_dict()

    registry = build_authority_registry(envelope=_envelope('authority_registry_v1', [])).to_dict()
    row_map = {row['artifact_family']: row for row in registry['rows']}
    assert row_map['promotion_record_v1'] == {
        'artifact_family': 'promotion_record_v1',
        'owner_plane': 'promotion_plane',
        'write_root': '/tmp/constellation_2_foundation/advisor_runtime/<MODE>/promotion_record_v1/<DAY>/',
        'authority_class': 'promotion_authority',
        'publication_required': True,
        'promotion_required': False,
        'replay_expected': True,
        'downstream_consumers': ['future trading-intent promotion only', 'decision_chain', 'operators'],
        'notes': ['sole_promotion_authority', 'single_writer_boundary'],
    }
    assert row_map['promotion_gate_result_v1'] == {
        'artifact_family': 'promotion_gate_result_v1',
        'owner_plane': 'promotion_plane',
        'write_root': '/tmp/constellation_2_foundation/advisor_runtime/<MODE>/promotion_gate_result_v1/<DAY>/',
        'authority_class': 'derived_only',
        'publication_required': True,
        'promotion_required': False,
        'replay_expected': True,
        'downstream_consumers': ['decision_chain', 'operators'],
        'notes': ['compatibility_output_only', 'not_authoritative_for_promotion'],
    }

    planning_snapshot = SimpleNamespace(planning_snapshot_id='ps1', advisory_packet_id='packet1')
    official_recommendation_set = SimpleNamespace(advisory_packet_id='packet1')
    decision_plan = SimpleNamespace(plan_id='dp1', planning_snapshot_id='ps1')
    chain = build_decision_chain(
        planning_snapshot=planning_snapshot,
        official_recommendation_set=official_recommendation_set,
        decision_plan=decision_plan,
        promotion_gate_result=gate_result,
        envelope=_envelope(
            'decision_chain_v1',
            ['planning_snapshot_id:ps1', 'official_recommendation_set_id:packet1', 'decision_plan_id:dp1', f'promotion_gate_result_id:{gate_result.gate_result_id}'],
        ),
    )

    assert chain.chain_status == 'gate_ready'
    assert chain.promotion_gate_result_id == gate_result.gate_result_id
    assert chain.promotion_candidate_id == 'candidate1'
    assert chain.promotion_review_id == 'review1'
    assert chain.promotion_manual_review_id == 'manual1'
