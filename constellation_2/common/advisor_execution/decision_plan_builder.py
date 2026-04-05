from __future__ import annotations

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.common.advisor_execution.action_intent_v1 import ActionIntentV1
from constellation_2.common.advisor_execution.action_policy_pack_v1 import ActionPolicyPackV1
from constellation_2.common.advisor_execution.decision_plan_v1 import DecisionPlanV1
from constellation_2.common.advisor_execution.official_recommendation_set_v1 import OfficialRecommendationSetV1
from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1


def build_decision_plan(*, planning_snapshot: PlanningSnapshotV1, official_recommendation_set: OfficialRecommendationSetV1, action_policy_pack: ActionPolicyPackV1, action_intent: ActionIntentV1) -> DecisionPlanV1:
    if action_intent.planning_snapshot_id != planning_snapshot.planning_snapshot_id:
        raise ValueError('ACTION_INTENT_PLANNING_SNAPSHOT_MISMATCH')
    if action_intent.advisory_packet_id != official_recommendation_set.advisory_packet_id:
        raise ValueError('ACTION_INTENT_ADVISORY_PACKET_MISMATCH')
    if action_intent.policy_pack_id != action_policy_pack.policy_pack_id:
        raise ValueError('ACTION_INTENT_POLICY_PACK_MISMATCH')

    assumptions_used = tuple(sorted({
        f'planning_snapshot_id:{planning_snapshot.planning_snapshot_id}',
        f'cash_cents:{planning_snapshot.liquidity.cash_cents}',
        f'minimum_monthly_spending_cents:{planning_snapshot.spending.minimum_monthly_spending_cents}',
        f'monthly_spending_cents:{planning_snapshot.spending.monthly_spending_cents}',
        f'guaranteed_monthly_income_cents:{planning_snapshot.income.guaranteed_monthly_income_cents}',
        f'tax_profile_present:{str(planning_snapshot.tax_profile.present).lower()}',
    }))
    constraints_used = sorted({f'policy_pack_id:{action_policy_pack.policy_pack_id}'})
    for action in action_intent.actions:
        constraints_used.extend(action.constraints)
    plan_id = canonical_hash_for_c2_artifact_v1({
        'planning_snapshot_id': planning_snapshot.planning_snapshot_id,
        'advisory_packet_id': official_recommendation_set.advisory_packet_id,
        'policy_pack_id': action_policy_pack.policy_pack_id,
        'action_intent_id': action_intent.action_intent_id,
    })
    return DecisionPlanV1(
        plan_id=plan_id,
        planning_snapshot_id=planning_snapshot.planning_snapshot_id,
        advisory_packet_id=official_recommendation_set.advisory_packet_id,
        actions=action_intent.actions,
        blocked_actions=action_intent.blocked_actions,
        assumptions_used=assumptions_used,
        constraints_used=tuple(sorted(set(constraints_used))),
        replan_triggers=action_intent.replan_triggers,
        version='v1',
    )
