from pathlib import Path
import sys
import subprocess

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if '/home/node/constellation_2_clean' not in sys.path:
    sys.path.insert(0, '/home/node/constellation_2_clean')

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1


def test_household_timeline_cli_deterministic(tmp_path: Path) -> None:
    planning = tmp_path / 'planning_snapshot.v1.json'
    planning.write_bytes(canonical_json_bytes_v1({'schema_id': 'planning_snapshot', 'schema_version': 'v1', 'planning_snapshot_id': 'ps1', 'advisory_packet_id': 'ap1', 'created_at': '2030-01-01T00:00:00Z', 'version': 'v1', 'accounts': {'taxable_account_id': 'tax', 'cash_reserve_account_id': 'cash', 'spending_account_id': 'spend'}, 'liquidity': {'cash_cents': 1}, 'spending': {'minimum_monthly_spending_cents': 2, 'monthly_spending_cents': 2}, 'income': {'guaranteed_monthly_income_cents': 0}, 'tax_profile': {'present': True}, 'annuities': []}) + b'\n')
    out_root = Path('/tmp/constellation_2_foundation/advisor_runtime')
    cmd = [sys.executable, str(ROOT / 'ops' / 'tools' / 'run_household_timeline_v1.py'), '--planning_snapshot_json', str(planning), '--mode', 'PAPER', '--day_utc', '2030-01-20', '--produced_utc', '2030-01-20T00:00:00Z', '--output_root', str(out_root)]
    subprocess.run(cmd, check=True)
    out_path = out_root / 'PAPER' / 'household_timeline_v1' / '2030-01-20' / 'household_timeline.v1.json'
    first = out_path.read_bytes()
    subprocess.run(cmd, check=True)
    second = out_path.read_bytes()
    assert first == second
    assert not (out_root / 'PAPER' / 'decision_plan_v1').exists()
