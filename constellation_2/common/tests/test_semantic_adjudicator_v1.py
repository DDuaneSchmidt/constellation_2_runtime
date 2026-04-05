from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if '/home/node/constellation' not in sys.path:
    sys.path.insert(0, '/home/node/constellation')

from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1
from constellation_2.common.advisor_kernel.decision_comparison_set_v1 import DecisionComparisonSetV1
from constellation_2.common.advisor_kernel.semantic_adjudicator import build_semantic_reconciliation_report


def test_semantic_adjudicator_catches_mismatch() -> None:
    snapshot = PlanningSnapshotV1.from_dict({'schema_id': 'planning_snapshot', 'schema_version': 'v1', 'planning_snapshot_id': 'ps1', 'advisory_packet_id': 'ap1', 'created_at': '2030-01-01T00:00:00Z', 'version': 'v1', 'accounts': {'taxable_account_id': 'tax', 'cash_reserve_account_id': 'cash', 'spending_account_id': 'spend'}, 'liquidity': {'cash_cents': 1}, 'spending': {'minimum_monthly_spending_cents': 1, 'monthly_spending_cents': 1}, 'income': {'guaranteed_monthly_income_cents': 0}, 'tax_profile': {'present': True}, 'annuities': []})
    comparison = DecisionComparisonSetV1.from_dict({'schema_id': 'decision_comparison_set', 'schema_version': 'v1', 'authority_class': 'recommendation_authority', 'support_status': 'fully_supported', 'produced_utc': '2030-01-20T00:00:00Z', 'run_id': 'run1', 'planning_snapshot_id': 'ps2', 'advisory_packet_id': 'ap1', 'option_ids': [], 'support_basis': 'official_recommendation_set_v1', 'reason_codes': []})
    meta = {'produced_utc': '2030-01-20T00:00:00Z', 'day_utc': '2030-01-20', 'mode': 'PAPER', 'source_artifact_refs': ['planning_snapshot_id:ps1']}
    report = build_semantic_reconciliation_report(planning_snapshot=snapshot, decision_comparison_set=comparison, metadata=meta)
    assert report.overall_status == 'fail'
