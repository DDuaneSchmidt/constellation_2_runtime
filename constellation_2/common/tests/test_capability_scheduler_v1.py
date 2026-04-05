from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if '/home/node/constellation_2_clean' not in sys.path:
    sys.path.insert(0, '/home/node/constellation_2_clean')

from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1
from constellation_2.common.advisor_kernel.capability_scheduler import build_capability_schedule


def test_capability_scheduler_deterministic() -> None:
    snapshot = PlanningSnapshotV1.from_dict({'schema_id': 'planning_snapshot', 'schema_version': 'v1', 'planning_snapshot_id': 'ps1', 'advisory_packet_id': 'ap1', 'created_at': '2030-01-01T00:00:00Z', 'version': 'v1', 'accounts': {'taxable_account_id': 'tax', 'cash_reserve_account_id': 'cash', 'spending_account_id': 'spend'}, 'liquidity': {'cash_cents': 1200000}, 'spending': {'minimum_monthly_spending_cents': 100000, 'monthly_spending_cents': 120000}, 'income': {'guaranteed_monthly_income_cents': 50000}, 'tax_profile': {'present': False}, 'annuities': []})
    meta = {'produced_utc': '2030-01-20T00:00:00Z', 'day_utc': '2030-01-20', 'mode': 'PAPER', 'source_artifact_refs': ['planning_snapshot_id:ps1']}
    one = build_capability_schedule(planning_snapshot=snapshot, module_maturity_map={'spending_policy': 'action_policy_allowed', 'readiness': 'observational_only'}, prerequisite_map={'readiness': ('spending_policy',)}, metadata=meta)
    two = build_capability_schedule(planning_snapshot=snapshot, module_maturity_map={'spending_policy': 'action_policy_allowed', 'readiness': 'observational_only'}, prerequisite_map={'readiness': ('spending_policy',)}, metadata=meta)
    assert one.to_dict() == two.to_dict()
