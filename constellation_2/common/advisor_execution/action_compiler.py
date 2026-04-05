from __future__ import annotations

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.common.advisor_execution.action_intent_v1 import ActionIntentV1
from constellation_2.common.advisor_execution.action_policy_engine import evaluate_policy
from constellation_2.common.advisor_execution.action_policy_pack_v1 import ActionPolicyPackV1
from constellation_2.common.advisor_execution.official_recommendation_set_v1 import OfficialRecommendationSetV1
from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1


def compile_action_intent(*, planning_snapshot: PlanningSnapshotV1, official_recommendation_set: OfficialRecommendationSetV1, action_policy_pack: ActionPolicyPackV1) -> ActionIntentV1:
    evaluation = evaluate_policy(
        planning_snapshot=planning_snapshot,
        official_recommendation_set=official_recommendation_set,
        action_policy_pack=action_policy_pack,
    )
    intent_id = canonical_hash_for_c2_artifact_v1({
        'plan_id': evaluation.plan_id,
        'action_ids': [item.action_id for item in evaluation.actions],
        'blocked_action_ids': [item.action_id for item in evaluation.blocked_actions],
        'policy_pack_id': action_policy_pack.policy_pack_id,
    })
    return ActionIntentV1(
        action_intent_id=intent_id,
        planning_snapshot_id=planning_snapshot.planning_snapshot_id,
        advisory_packet_id=official_recommendation_set.advisory_packet_id,
        policy_pack_id=action_policy_pack.policy_pack_id,
        actions=evaluation.actions,
        blocked_actions=evaluation.blocked_actions,
        replan_triggers=evaluation.replan_triggers,
        version='v1',
    )
