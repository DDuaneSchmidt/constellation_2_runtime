from __future__ import annotations

from dataclasses import dataclass

from constellation_2.common.advisor_execution.decision_plan_v1 import DecisionPlanV1


@dataclass(frozen=True, slots=True)
class DecisionMonitorStateV1:
    plan_id: str
    action_count: int
    blocked_action_count: int
    trigger_ids: tuple[str, ...]
    monitor_state: str


def build_decision_monitor_state(decision_plan: DecisionPlanV1) -> DecisionMonitorStateV1:
    blocked = len(decision_plan.blocked_actions)
    return DecisionMonitorStateV1(
        plan_id=decision_plan.plan_id,
        action_count=len(decision_plan.actions),
        blocked_action_count=blocked,
        trigger_ids=tuple(item.trigger_id for item in decision_plan.replan_triggers),
        monitor_state='blocked_review_required' if blocked else 'review_ready',
    )
