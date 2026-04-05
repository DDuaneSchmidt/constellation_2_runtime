from __future__ import annotations

import sys
from pathlib import Path

SOURCE_ROOT = Path('/home/node/constellation')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.authority_registry_v1 import build_authority_registry
from constellation_2.common.metadata_envelope_v1 import metadata_envelope_v1


def test_authority_registry_is_deterministic_and_complete() -> None:
    env = metadata_envelope_v1(
        produced_utc='2026-04-05T12:00:00Z',
        day_utc='2026-04-05',
        mode='paper',
        source_artifact_refs=[],
        artifact_family='authority_registry_v1',
    )

    registry_a = build_authority_registry(envelope=env).to_dict()
    registry_b = build_authority_registry(envelope=env).to_dict()

    assert registry_a == registry_b

    families = {row['artifact_family'] for row in registry_a['rows']}
    assert {
        'planning_snapshot_v1',
        'official_recommendation_set_v1',
        'action_policy_pack_v1',
        'action_intent_v1',
        'decision_action_v1',
        'blocked_action_v1',
        'decision_plan_v1',
        'decision_plan_delta_v1',
        'capability_schedule_v1',
        'semantic_reconciliation_report_v1',
        'publication_gate_result_v1',
        'advisor_trade_translation_v1',
        'advisor_trade_intent_proposal_v1',
        'promotion_candidate_v1',
        'promotion_review_v1',
        'decision_chain_v1',
        'runtime_trace_bundle_v1',
        'replay_manifest_v1',
    } <= families

    row_map = {row['artifact_family']: row for row in registry_a['rows']}
    assert row_map['promotion_candidate_v1']['owner_plane'] == 'promotion_plane'
    assert row_map['decision_plan_v1']['owner_plane'] == 'decision_plane'
    assert row_map['planning_snapshot_v1']['owner_plane'] == 'truth_plane'
