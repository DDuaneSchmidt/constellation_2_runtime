from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

SOURCE_ROOT = Path('/home/node/constellation_2_clean')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1, canonical_sha256_hex_v1


def compute_run_id(*, artifact_family: str, metadata: dict[str, Any]) -> str:
    payload = {
        'day_utc': metadata['day_utc'],
        'mode': metadata['mode'],
        'artifact_family': artifact_family,
        'source_artifact_refs': sorted(metadata['source_artifact_refs']),
    }
    return canonical_sha256_hex_v1(payload)


def build_base(schema_id: str, authority_class: str, metadata: dict[str, Any], artifact_family: str) -> dict[str, Any]:
    return {
        'schema_id': schema_id,
        'schema_version': 'v1',
        'authority_class': authority_class,
        'support_status': 'fully_supported',
        'produced_utc': metadata['produced_utc'],
        'run_id': compute_run_id(artifact_family=artifact_family, metadata=metadata),
    }
from constellation_2.common.advisor_execution.decision_plan_v1 import DecisionPlanV1
from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1
from constellation_2.common.advisor_kernel.decision_comparison_set_v1 import DecisionComparisonSetV1
from constellation_2.common.advisor_kernel.decision_monitor_state_v1 import DecisionMonitorStateV1
from constellation_2.common.advisor_kernel.income_floor_timeline_v1 import IncomeFloorTimelineV1
from constellation_2.common.advisor_kernel.semantic_reconciliation_report_v1 import SemanticReconciliationReportV1
from constellation_2.common.advisor_kernel.spending_policy_v1 import SpendingPolicyV1
from constellation_2.common.advisor_kernel.tax_budget_v1 import TaxBudgetV1


def build_semantic_reconciliation_report(*, planning_snapshot: PlanningSnapshotV1, spending_policy: SpendingPolicyV1 | None = None, income_floor_timeline: IncomeFloorTimelineV1 | None = None, tax_budget: TaxBudgetV1 | None = None, decision_comparison_set: DecisionComparisonSetV1 | None = None, decision_plan: DecisionPlanV1 | None = None, decision_monitor_state: DecisionMonitorStateV1 | None = None, metadata: dict[str, Any]) -> SemanticReconciliationReportV1:
    checks = []
    if decision_comparison_set is not None:
        ok = decision_comparison_set.planning_snapshot_id == planning_snapshot.planning_snapshot_id
        checks.append({'check_id': 'decision_comparison_snapshot_match', 'artifact_refs': ['decision_comparison_set_v1'], 'status': 'pass' if ok else 'fail', 'reason_codes': [] if ok else ['DECISION_COMPARISON_PLANNING_SNAPSHOT_MISMATCH']})
    if spending_policy is not None and income_floor_timeline is not None:
        ok = income_floor_timeline.required_monthly_spending_cents >= spending_policy.minimum_spending_cents
        checks.append({'check_id': 'income_floor_reconciles', 'artifact_refs': ['spending_policy_v1', 'income_floor_timeline_v1'], 'status': 'pass' if ok else 'fail', 'reason_codes': [] if ok else ['INCOME_FLOOR_SPENDING_MISMATCH']})
    if decision_plan is not None and tax_budget is not None:
        has_taxable = any(action.action_type == 'withdraw_from_taxable' for action in decision_plan.actions)
        ok = (not has_taxable) or tax_budget.taxable_withdrawal_allowed
        checks.append({'check_id': 'taxable_withdrawal_reconciles', 'artifact_refs': ['decision_plan_v1', 'tax_budget_v1'], 'status': 'pass' if ok else 'fail', 'reason_codes': [] if ok else ['TAXABLE_WITHDRAWAL_BLOCKED']})
    if decision_plan is not None and decision_monitor_state is not None:
        ok = decision_monitor_state.decision_plan_id == decision_plan.plan_id
        checks.append({'check_id': 'monitor_plan_match', 'artifact_refs': ['decision_plan_v1', 'decision_monitor_state_v1'], 'status': 'pass' if ok else 'fail', 'reason_codes': [] if ok else ['DECISION_MONITOR_PLAN_MISMATCH']})
    overall = 'pass' if all(item['status'] == 'pass' for item in checks) else 'fail'
    obj = build_base('semantic_reconciliation_report', 'policy_authority', metadata, 'semantic_reconciliation_report_v1')
    obj.update({'support_status': 'fully_supported' if overall == 'pass' else 'blocked', 'planning_snapshot_id': planning_snapshot.planning_snapshot_id, 'advisory_packet_id': planning_snapshot.advisory_packet_id, 'day_utc': metadata['day_utc'], 'checks': checks, 'overall_status': overall})
    return SemanticReconciliationReportV1.from_dict(obj)
