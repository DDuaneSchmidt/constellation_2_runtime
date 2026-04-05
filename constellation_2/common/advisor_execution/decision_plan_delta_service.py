from __future__ import annotations

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.common.advisor_execution.decision_plan_delta_v1 import DecisionPlanDeltaV1
from constellation_2.common.advisor_execution.decision_plan_v1 import DecisionPlanV1


def _all_action_dicts(plan: DecisionPlanV1) -> dict[str, dict]:
    rows = {}
    for action in plan.actions:
        rows[action.action_id] = action.to_dict()
    for action in plan.blocked_actions:
        rows[action.action_id] = action.to_dict()
    return rows


def build_decision_plan_delta(*, previous_plan: DecisionPlanV1, current_plan: DecisionPlanV1) -> DecisionPlanDeltaV1:
    prev_rows = _all_action_dicts(previous_plan)
    curr_rows = _all_action_dicts(current_plan)
    changed_ids = sorted(set(prev_rows) ^ set(curr_rows))
    changed_ids.extend(sorted(action_id for action_id in set(prev_rows) & set(curr_rows) if prev_rows[action_id] != curr_rows[action_id]))
    changed_ids = sorted(set(changed_ids))

    if previous_plan.to_dict() == current_plan.to_dict():
        delta_type = 'no_change'
    elif set(previous_plan.constraints_used) != set(current_plan.constraints_used):
        delta_type = 'constraint_change'
    elif {item.recommendation_id for item in previous_plan.actions + previous_plan.blocked_actions} != {item.recommendation_id for item in current_plan.actions + current_plan.blocked_actions}:
        delta_type = 'recommendation_change'
    elif set(previous_plan.assumptions_used) != set(current_plan.assumptions_used):
        delta_type = 'fact_change'
    else:
        delta_type = 'policy_change'

    summary = f'{delta_type}:{len(changed_ids)}'
    delta_id = canonical_hash_for_c2_artifact_v1({
        'previous_plan_id': previous_plan.plan_id,
        'current_plan_id': current_plan.plan_id,
        'delta_type': delta_type,
        'changed_action_ids': changed_ids,
    })
    return DecisionPlanDeltaV1(
        plan_delta_id=delta_id,
        previous_plan_id=previous_plan.plan_id,
        current_plan_id=current_plan.plan_id,
        delta_type=delta_type,
        changed_action_ids=tuple(changed_ids),
        summary=summary,
        version='v1',
    )
