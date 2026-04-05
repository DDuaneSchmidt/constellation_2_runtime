from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if '/home/node/constellation_2_clean' not in sys.path:
    sys.path.insert(0, '/home/node/constellation_2_clean')

from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1
from constellation_2.common.advisor_kernel.spending_policy_service import build_spending_policy


def test_decision_lane_authority() -> None:
    snapshot = PlanningSnapshotV1.from_dict({'schema_id': 'planning_snapshot', 'schema_version': 'v1', 'planning_snapshot_id': 'ps1', 'advisory_packet_id': 'ap1', 'created_at': '2030-01-01T00:00:00Z', 'version': 'v1', 'accounts': {'taxable_account_id': 'tax', 'cash_reserve_account_id': 'cash', 'spending_account_id': 'spend'}, 'liquidity': {'cash_cents': 1}, 'spending': {'minimum_monthly_spending_cents': 2, 'monthly_spending_cents': 2}, 'income': {'guaranteed_monthly_income_cents': 0}, 'tax_profile': {'present': True}, 'annuities': []})
    meta = {'produced_utc': '2030-01-20T00:00:00Z', 'day_utc': '2030-01-20', 'mode': 'PAPER', 'source_artifact_refs': ['planning_snapshot_id:ps1']}
    artifact = build_spending_policy(planning_snapshot=snapshot, metadata=meta)
    assert artifact.authority_class == 'policy_authority'
