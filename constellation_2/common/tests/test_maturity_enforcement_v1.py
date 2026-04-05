from pathlib import Path
import sys
import pytest

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if '/home/node/constellation' not in sys.path:
    sys.path.insert(0, '/home/node/constellation')

from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1
from constellation_2.common.advisor_kernel.capability_scheduler import build_capability_schedule


def test_invalid_maturity_fails_closed() -> None:
    snapshot = PlanningSnapshotV1.from_dict({'schema_id': 'planning_snapshot', 'schema_version': 'v1', 'planning_snapshot_id': 'ps1', 'advisory_packet_id': 'ap1', 'created_at': '2030-01-01T00:00:00Z', 'version': 'v1', 'accounts': {'taxable_account_id': 'tax', 'cash_reserve_account_id': 'cash', 'spending_account_id': 'spend'}, 'liquidity': {'cash_cents': 1}, 'spending': {'minimum_monthly_spending_cents': 2, 'monthly_spending_cents': 2}, 'income': {'guaranteed_monthly_income_cents': 0}, 'tax_profile': {'present': True}, 'annuities': []})
    meta = {'produced_utc': '2030-01-20T00:00:00Z', 'day_utc': '2030-01-20', 'mode': 'PAPER', 'source_artifact_refs': ['planning_snapshot_id:ps1']}
    with pytest.raises(ValueError):
        build_capability_schedule(planning_snapshot=snapshot, module_maturity_map={'x': 'bad'}, prerequisite_map={}, metadata=meta)
