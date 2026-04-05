from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.common.advisor_execution.action_policy_pack_v1 import ActionPolicyPackV1, ReplanTriggerRuleV1
from constellation_2.common.advisor_execution.blocked_action_v1 import BlockedActionV1
from constellation_2.common.advisor_execution.decision_action_v1 import DecisionActionV1
from constellation_2.common.advisor_execution.official_recommendation_set_v1 import OfficialRecommendationSetV1, OfficialRecommendationV1
from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1, AnnuityV1


@dataclass(frozen=True, slots=True)
class ActionPolicyEvaluationV1:
    plan_id: str
    actions: tuple[DecisionActionV1, ...]
    blocked_actions: tuple[BlockedActionV1, ...]
    assumptions_used: tuple[str, ...]
    constraints_used: tuple[str, ...]
    replan_triggers: tuple[ReplanTriggerRuleV1, ...]


def _plan_id(snapshot: PlanningSnapshotV1, recommendation_set: OfficialRecommendationSetV1, policy_pack: ActionPolicyPackV1) -> str:
    return canonical_hash_for_c2_artifact_v1({
        'planning_snapshot_id': snapshot.planning_snapshot_id,
        'advisory_packet_id': recommendation_set.advisory_packet_id,
        'policy_pack_id': policy_pack.policy_pack_id,
    })


def _match_recommendation(recommendation_set: OfficialRecommendationSetV1, *, domain_id: str, action_type: str) -> OfficialRecommendationV1:
    matches = [
        item for item in recommendation_set.recommendations
        if item.domain_id == domain_id and item.action_type == action_type and item.recommendation_status == 'active'
    ]
    if len(matches) != 1:
        raise ValueError(f'AMBIGUOUS_RECOMMENDATION_MAPPING:{domain_id}:{action_type}:{len(matches)}')
    return matches[0]


def _constraints(policy_pack: ActionPolicyPackV1, *, domain_id: str, action_type: str) -> tuple[str, ...]:
    rows = [item.constraint_code for item in policy_pack.action_constraint_rules if item.domain_id == domain_id and item.action_type == action_type]
    return tuple(sorted(set(rows)))


def _maturity(policy_pack: ActionPolicyPackV1, domain_id: str) -> str:
    rows = [item.maturity for item in policy_pack.domain_action_maturity if item.domain_id == domain_id]
    if len(rows) != 1:
        raise ValueError(f'AMBIGUOUS_DOMAIN_MATURITY:{domain_id}:{len(rows)}')
    return rows[0]


def _action_id(plan_id: str, recommendation_id: str, action_type: str, discriminator: str) -> str:
    return canonical_hash_for_c2_artifact_v1({
        'plan_id': plan_id,
        'recommendation_id': recommendation_id,
        'action_type': action_type,
        'discriminator': discriminator,
    })


def _build_action(*, plan_id: str, recommendation: OfficialRecommendationV1, created_at: str, source_account: str | None, destination_account: str | None, annual_amount: int, periodic_amount: int, periodicity: str, support_status: str, blockers: Iterable[str], constraints: Iterable[str], evidence_refs: Iterable[str]) -> DecisionActionV1:
    action_id = _action_id(plan_id, recommendation.recommendation_id, recommendation.action_type, recommendation.domain_id)
    return DecisionActionV1(
        action_id=action_id,
        plan_id=plan_id,
        recommendation_id=recommendation.recommendation_id,
        domain_id=recommendation.domain_id,
        action_type=recommendation.action_type,
        action_status='planned',
        support_status=support_status,
        priority=recommendation.priority,
        source_account=source_account,
        destination_account=destination_account,
        annual_amount=annual_amount,
        periodic_amount=periodic_amount,
        periodicity=periodicity,
        constraints=tuple(sorted(set(constraints))),
        blockers=tuple(sorted(set(blockers))),
        rationale_refs=recommendation.rationale_refs,
        evidence_refs=tuple(sorted(set(evidence_refs))),
        created_at=created_at,
        version='v1',
    )


def _build_blocked_action(*, plan_id: str, recommendation: OfficialRecommendationV1, created_at: str, blockers: Iterable[str], evidence_refs: Iterable[str]) -> BlockedActionV1:
    action_id = _action_id(plan_id, recommendation.recommendation_id, recommendation.action_type, 'blocked')
    return BlockedActionV1(
        action_id=action_id,
        plan_id=plan_id,
        recommendation_id=recommendation.recommendation_id,
        domain_id=recommendation.domain_id,
        action_type=recommendation.action_type,
        support_status='blocked',
        blockers=tuple(sorted(set(blockers))),
        rationale_refs=recommendation.rationale_refs,
        evidence_refs=tuple(sorted(set(evidence_refs))),
        created_at=created_at,
        version='v1',
    )


def _annuity_actions(snapshot: PlanningSnapshotV1, policy_pack: ActionPolicyPackV1, recommendation_set: OfficialRecommendationSetV1, plan_id: str) -> list[DecisionActionV1]:
    recommendation = _match_recommendation(recommendation_set, domain_id='annuity', action_type='hold_annuity')
    maturity = _maturity(policy_pack, 'annuity')
    base_constraints = set(_constraints(policy_pack, domain_id='annuity', action_type='hold_annuity'))
    base_constraints.add(f'DOMAIN_MATURITY:{maturity}')
    actions: list[DecisionActionV1] = []
    for annuity in sorted(snapshot.annuities, key=lambda item: item.annuity_id):
        if annuity.phase != 'deferred_accumulation':
            continue
        action_id = _action_id(plan_id, recommendation.recommendation_id, recommendation.action_type, annuity.annuity_id)
        actions.append(DecisionActionV1(
            action_id=action_id,
            plan_id=plan_id,
            recommendation_id=recommendation.recommendation_id,
            domain_id='annuity',
            action_type='hold_annuity',
            action_status='planned',
            support_status='fully_supported',
            priority=recommendation.priority,
            source_account=annuity.annuity_id,
            destination_account=None,
            annual_amount=0,
            periodic_amount=0,
            periodicity='none',
            constraints=tuple(sorted(base_constraints)),
            blockers=tuple(),
            rationale_refs=recommendation.rationale_refs,
            evidence_refs=tuple(sorted(set(recommendation.evidence_refs + (f'annuity:{annuity.annuity_id}', f'planning_snapshot:{snapshot.planning_snapshot_id}')))),
            created_at=snapshot.created_at,
            version='v1',
        ))
    return actions


def evaluate_policy(*, planning_snapshot: PlanningSnapshotV1, official_recommendation_set: OfficialRecommendationSetV1, action_policy_pack: ActionPolicyPackV1) -> ActionPolicyEvaluationV1:
    plan_id = _plan_id(planning_snapshot, official_recommendation_set, action_policy_pack)
    actions: list[DecisionActionV1] = []
    blocked_actions: list[BlockedActionV1] = []
    constraints_used: set[str] = {f'policy_pack_id:{action_policy_pack.policy_pack_id}'}
    assumptions_used = {
        f'planning_snapshot_id:{planning_snapshot.planning_snapshot_id}',
        f'advisory_packet_id:{planning_snapshot.advisory_packet_id}',
        f'cash_cents:{planning_snapshot.liquidity.cash_cents}',
        f'minimum_monthly_spending_cents:{planning_snapshot.spending.minimum_monthly_spending_cents}',
        f'monthly_spending_cents:{planning_snapshot.spending.monthly_spending_cents}',
        f'guaranteed_monthly_income_cents:{planning_snapshot.income.guaranteed_monthly_income_cents}',
        f'tax_profile_present:{str(planning_snapshot.tax_profile.present).lower()}',
    }

    liquidity_threshold = planning_snapshot.spending.minimum_monthly_spending_cents * 12
    if planning_snapshot.liquidity.cash_cents < liquidity_threshold:
        gap = liquidity_threshold - planning_snapshot.liquidity.cash_cents
        rec = _match_recommendation(official_recommendation_set, domain_id='liquidity', action_type='raise_cash_reserve')
        maturity = _maturity(action_policy_pack, 'liquidity')
        rules = set(_constraints(action_policy_pack, domain_id='liquidity', action_type='raise_cash_reserve'))
        rules.add(f'DOMAIN_MATURITY:{maturity}')
        constraints_used.update(rules)
        actions.append(_build_action(
            plan_id=plan_id,
            recommendation=rec,
            created_at=planning_snapshot.created_at,
            source_account=planning_snapshot.accounts.taxable_account_id,
            destination_account=planning_snapshot.accounts.cash_reserve_account_id,
            annual_amount=gap,
            periodic_amount=gap,
            periodicity='annual',
            support_status='fully_supported',
            blockers=(),
            constraints=rules,
            evidence_refs=[f'planning_snapshot:{planning_snapshot.planning_snapshot_id}'],
        ))

    required_income = planning_snapshot.spending.monthly_spending_cents - planning_snapshot.income.guaranteed_monthly_income_cents
    if required_income > 0:
        rec = _match_recommendation(official_recommendation_set, domain_id='withdrawal', action_type='withdraw_from_taxable')
        maturity = _maturity(action_policy_pack, 'withdrawal')
        rules = set(_constraints(action_policy_pack, domain_id='withdrawal', action_type='withdraw_from_taxable'))
        rules.add(f'DOMAIN_MATURITY:{maturity}')
        blockers: set[str] = set()
        support_status = 'fully_supported'
        if not planning_snapshot.tax_profile.present:
            support_status = 'provisional'
            blockers.add('TAX_PROFILE_MISSING')
        constraints_used.update(rules)
        actions.append(_build_action(
            plan_id=plan_id,
            recommendation=rec,
            created_at=planning_snapshot.created_at,
            source_account=planning_snapshot.accounts.taxable_account_id,
            destination_account=planning_snapshot.accounts.spending_account_id,
            annual_amount=required_income * 12,
            periodic_amount=required_income,
            periodicity='monthly',
            support_status=support_status,
            blockers=blockers,
            constraints=rules,
            evidence_refs=[f'planning_snapshot:{planning_snapshot.planning_snapshot_id}'],
        ))

    actions.extend(_annuity_actions(planning_snapshot, action_policy_pack, official_recommendation_set, plan_id))
    for action in actions:
        constraints_used.update(action.constraints)

    if not planning_snapshot.tax_profile.present:
        rec = _match_recommendation(official_recommendation_set, domain_id='tax', action_type='collect_missing_input')
        maturity = _maturity(action_policy_pack, 'tax')
        rules = set(_constraints(action_policy_pack, domain_id='tax', action_type='collect_missing_input'))
        rules.add(f'DOMAIN_MATURITY:{maturity}')
        constraints_used.update(rules)
        blocked_actions.append(_build_blocked_action(
            plan_id=plan_id,
            recommendation=rec,
            created_at=planning_snapshot.created_at,
            blockers=['TAX_PROFILE_MISSING'],
            evidence_refs=[f'planning_snapshot:{planning_snapshot.planning_snapshot_id}'],
        ))

    ordered_actions = tuple(sorted(actions, key=lambda item: (item.priority, item.recommendation_id, item.action_id)))
    ordered_blocked = tuple(sorted(blocked_actions, key=lambda item: (item.recommendation_id, item.action_id)))
    triggers = tuple(sorted(action_policy_pack.replan_trigger_rules, key=lambda item: (item.trigger_type, item.trigger_id)))
    return ActionPolicyEvaluationV1(
        plan_id=plan_id,
        actions=ordered_actions,
        blocked_actions=ordered_blocked,
        assumptions_used=tuple(sorted(assumptions_used)),
        constraints_used=tuple(sorted(constraints_used)),
        replan_triggers=triggers,
    )
