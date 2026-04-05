from __future__ import annotations

import sys
from pathlib import Path

SOURCE_ROOT = Path('/home/node/constellation_2_clean')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from types import SimpleNamespace

from constellation_2.common.decision_chain_service import build_decision_chain
from constellation_2.common.metadata_envelope_v1 import metadata_envelope_v1


def _envelope(refs: list[str]):
    return metadata_envelope_v1(
        produced_utc='2026-04-05T12:00:00Z',
        day_utc='2026-04-05',
        mode='paper',
        source_artifact_refs=refs,
        artifact_family='decision_chain_v1',
    )


def test_decision_chain_status_progression_and_determinism() -> None:
    planning_snapshot = SimpleNamespace(planning_snapshot_id='ps1', advisory_packet_id='packet1')
    official_recommendation_set = SimpleNamespace(advisory_packet_id='packet1')
    decision_plan = SimpleNamespace(plan_id='dp1', planning_snapshot_id='ps1')
    decision_plan_delta = SimpleNamespace(plan_delta_id='delta1', current_plan_id='dp1')
    bridge_translation = SimpleNamespace(run_id='tr1')
    bridge_proposal = SimpleNamespace(proposal_id='proposal1')
    promotion_candidate = SimpleNamespace(candidate_id='candidate1')
    review_required = SimpleNamespace(review_id='review1', candidate_id='candidate1', review_status='review_required')
    rejected = SimpleNamespace(review_id='review2', candidate_id='candidate1', review_status='rejected')

    decision_only = build_decision_chain(
        planning_snapshot=planning_snapshot,
        official_recommendation_set=official_recommendation_set,
        decision_plan=decision_plan,
        decision_plan_delta=decision_plan_delta,
        envelope=_envelope(['planning_snapshot_id:ps1', 'official_recommendation_set_id:packet1', 'decision_plan_id:dp1', 'decision_plan_delta_id:delta1']),
    )
    assert decision_only.chain_status == 'decision_only'

    bridge_ready = build_decision_chain(
        planning_snapshot=planning_snapshot,
        official_recommendation_set=official_recommendation_set,
        decision_plan=decision_plan,
        bridge_translation=bridge_translation,
        bridge_proposal=bridge_proposal,
        envelope=_envelope(['planning_snapshot_id:ps1', 'official_recommendation_set_id:packet1', 'decision_plan_id:dp1', 'bridge_translation_id:tr1', 'bridge_proposal_id:proposal1']),
    )
    assert bridge_ready.chain_status == 'bridge_ready'

    promotion_ready = build_decision_chain(
        planning_snapshot=planning_snapshot,
        official_recommendation_set=official_recommendation_set,
        decision_plan=decision_plan,
        bridge_translation=bridge_translation,
        bridge_proposal=bridge_proposal,
        promotion_candidate=promotion_candidate,
        promotion_review=review_required,
        envelope=_envelope(['planning_snapshot_id:ps1', 'official_recommendation_set_id:packet1', 'decision_plan_id:dp1', 'bridge_translation_id:tr1', 'bridge_proposal_id:proposal1', 'promotion_candidate_id:candidate1', 'promotion_review_id:review1']),
    )
    promotion_ready_again = build_decision_chain(
        planning_snapshot=planning_snapshot,
        official_recommendation_set=official_recommendation_set,
        decision_plan=decision_plan,
        bridge_translation=bridge_translation,
        bridge_proposal=bridge_proposal,
        promotion_candidate=promotion_candidate,
        promotion_review=review_required,
        envelope=_envelope(['planning_snapshot_id:ps1', 'official_recommendation_set_id:packet1', 'decision_plan_id:dp1', 'bridge_translation_id:tr1', 'bridge_proposal_id:proposal1', 'promotion_candidate_id:candidate1', 'promotion_review_id:review1']),
    )
    assert promotion_ready.chain_status == 'promotion_ready'
    assert promotion_ready.to_dict() == promotion_ready_again.to_dict()

    promotion_blocked = build_decision_chain(
        planning_snapshot=planning_snapshot,
        official_recommendation_set=official_recommendation_set,
        decision_plan=decision_plan,
        bridge_translation=bridge_translation,
        bridge_proposal=bridge_proposal,
        promotion_candidate=promotion_candidate,
        promotion_review=rejected,
        envelope=_envelope(['planning_snapshot_id:ps1', 'official_recommendation_set_id:packet1', 'decision_plan_id:dp1', 'bridge_translation_id:tr1', 'bridge_proposal_id:proposal1', 'promotion_candidate_id:candidate1', 'promotion_review_id:review2']),
    )
    assert promotion_blocked.chain_status == 'promotion_blocked'
