from __future__ import annotations

from pathlib import Path

from constellation_2.common.advisor_execution.action_policy_pack_v1 import ActionPolicyPackV1
from constellation_2.common.advisor_execution.official_recommendation_set_v1 import OfficialRecommendationSetV1
from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1


def load_planning_snapshot(path: str | Path) -> PlanningSnapshotV1:
    return PlanningSnapshotV1.load_file(path)


def load_official_recommendation_set(path: str | Path) -> OfficialRecommendationSetV1:
    return OfficialRecommendationSetV1.load_file(path)


def load_action_policy_pack(path: str | Path) -> ActionPolicyPackV1:
    return ActionPolicyPackV1.load_file(path)


def load_inputs(*, planning_snapshot_path: str | Path, official_recommendation_set_path: str | Path, action_policy_pack_path: str | Path) -> tuple[PlanningSnapshotV1, OfficialRecommendationSetV1, ActionPolicyPackV1]:
    snapshot = load_planning_snapshot(planning_snapshot_path)
    recommendation_set = load_official_recommendation_set(official_recommendation_set_path)
    policy_pack = load_action_policy_pack(action_policy_pack_path)
    if snapshot.advisory_packet_id != recommendation_set.advisory_packet_id:
        raise ValueError('ADVISORY_PACKET_ID_MISMATCH')
    return snapshot, recommendation_set, policy_pack
