from __future__ import annotations

from pathlib import Path
from typing import Any

from constellation_2.common.advisory.policy_v1 import PolicyV1
from constellation_2.common.advisory.execution_intent_v1 import ExecutionIntentV1
from constellation_2.common.advisory.portfolio_intent_v1 import PortfolioIntentV1
from constellation_2.common.advisory.promotion_decision_v1 import PromotionDecisionV1
from constellation_2.common.advisor_bridge.promotion_record_v2 import PromotionRecordV2
from constellation_2.common.execution_kernel.approved_change_set_service_v1 import write_approved_change_set_v1
from constellation_2.common.execution_kernel.execution_set_intent_service_v1 import write_execution_set_intent_v1
from constellation_2.common.execution_kernel.execution_kernel_runner_v1 import run_execution_kernel_v1
from constellation_2.common.execution_kernel.multi_delta_execution_decision_service_v1 import (
    write_multi_delta_execution_decision_v1,
)
from constellation_2.common.execution_kernel.multi_delta_execution_record_service_v1 import (
    write_multi_delta_execution_record_v1,
)
from constellation_2.common.execution_kernel.multi_delta_execution_status_service_v1 import (
    summarize_execution_set_status_v1,
)


def run_multi_delta_execution_kernel_v1(
    *,
    repo_root: Path,
    truth_root: str | Path | None,
    run_id: str,
    policy: PolicyV1,
    portfolio_intent: PortfolioIntentV1,
    promotion_decision: PromotionDecisionV1,
    promotion_record: PromotionRecordV2,
    produced_utc: str,
    created_at_utc: str,
    effective_at_utc: str,
    actor_source: str,
    eval_time_utc: str,
    risk_budget_path: Path,
    ib_host: str,
    ib_port: int,
    ib_client_id: int,
    dry_run: bool,
    submissions_root_override: Path | None = None,
) -> dict[str, Any]:
    approved_change_set, approved_change_set_path = write_approved_change_set_v1(
        policy=policy,
        portfolio_intent=portfolio_intent,
        promotion_decision=promotion_decision,
        promotion_record=promotion_record,
        produced_utc=produced_utc,
        truth_root=truth_root,
    )
    decision, decision_path = write_multi_delta_execution_decision_v1(
        policy=policy,
        approved_change_set=approved_change_set,
        produced_utc=produced_utc,
        truth_root=truth_root,
    )
    if decision.outcome != 'promote':
        return {
            'approved_change_set': approved_change_set,
            'approved_change_set_path': approved_change_set_path,
            'multi_delta_execution_decision': decision,
            'multi_delta_execution_decision_path': decision_path,
            'multi_delta_execution_record': None,
            'multi_delta_execution_record_path': None,
            'execution_set_intent': None,
            'execution_set_intent_path': None,
            'member_results': [],
            'set_status': {'set_status': 'BLOCKED' if decision.outcome == 'blocked' else decision.outcome.upper()},
        }

    record, record_path, record_action = write_multi_delta_execution_record_v1(
        approved_change_set=approved_change_set,
        decision=decision,
        produced_utc=produced_utc,
        truth_root=truth_root,
    )
    if record_action != 'WROTE':
        return {
            'approved_change_set': approved_change_set,
            'approved_change_set_path': approved_change_set_path,
            'multi_delta_execution_decision': decision,
            'multi_delta_execution_decision_path': decision_path,
            'multi_delta_execution_record': record,
            'multi_delta_execution_record_path': record_path,
            'execution_set_intent': None,
            'execution_set_intent_path': None,
            'member_results': [],
            'set_status': {'set_status': 'DUPLICATE'},
        }
    execution_set_intent, execution_set_intent_path = write_execution_set_intent_v1(
        record=record,
        created_at_utc=created_at_utc,
        effective_at_utc=effective_at_utc,
        actor_source=actor_source,
        truth_root=truth_root,
    )
    member_results = []
    for member in execution_set_intent.member_intents:
        result = run_execution_kernel_v1(
            repo_root=repo_root,
            truth_root=truth_root,
            run_id=f'{run_id}.member_{int(member["member_order"]):02d}',
            execution_intent=ExecutionIntentV1.from_dict(member['execution_intent']),
            produced_utc=produced_utc,
            eval_time_utc=eval_time_utc,
            risk_budget_path=risk_budget_path,
            ib_host=ib_host,
            ib_port=ib_port,
            ib_client_id=ib_client_id,
            dry_run=dry_run,
            submissions_root_override=submissions_root_override,
        )
        member_results.append(result)
    return {
        'approved_change_set': approved_change_set,
        'approved_change_set_path': approved_change_set_path,
        'multi_delta_execution_decision': decision,
        'multi_delta_execution_decision_path': decision_path,
        'multi_delta_execution_record': record,
        'multi_delta_execution_record_path': record_path,
        'execution_set_intent': execution_set_intent,
        'execution_set_intent_path': execution_set_intent_path,
        'member_results': member_results,
        'set_status': summarize_execution_set_status_v1(
            truth_root=truth_root,
            execution_set_intent=execution_set_intent,
        ),
    }
