from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
TOOL = REPO_ROOT / 'ops/tools/run_planning_snapshot_v1.py'
OUTPUT_BASE = Path('/tmp/constellation_2_foundation/advisor_runtime/PAPER/planning_snapshot_v1')


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(',', ':')) + '\n', encoding='utf-8')


def _sha_tree(root: Path) -> str:
    h = hashlib.sha256()
    for path in sorted(root.rglob('*')):
        if path.is_file():
            h.update(str(path.relative_to(root)).encode('utf-8'))
            h.update(b'\n')
            h.update(path.read_bytes())
            h.update(b'\n')
    return h.hexdigest()


def _cash_snapshot(day: str, cash_cents: int) -> dict:
    return {
        'schema_id': 'C2_CASH_LEDGER_SNAPSHOT_V1',
        'schema_version': 1,
        'produced_utc': f'{day}T00:00:00Z',
        'day_utc': day,
        'producer': {'repo': 'constellation_2_runtime', 'git_sha': '40b254d040f654dad528947650c47843a84f719d', 'module': 'test'},
        'status': 'OK',
        'reason_codes': ['TEST'],
        'input_manifest': [{'type': 'other', 'path': '/tmp/source.json', 'sha256': '0' * 64, 'day_utc': day, 'producer': 'test'}],
        'snapshot': {
            'observed_at_utc': f'{day}T00:00:00Z',
            'currency': 'USD',
            'cash_total_cents': cash_cents,
            'nlv_total_cents': cash_cents,
            'available_funds_cents': cash_cents,
            'excess_liquidity_cents': cash_cents,
            'account_id': 'DUO847203',
            'notes': ['TEST'],
        },
    }


def _household_input(*, cash_cents: int | None) -> dict:
    obj = {
        'accounts': {
            'taxable_account_id': 'acct_taxable',
            'cash_reserve_account_id': 'acct_cash_reserve',
            'spending_account_id': 'acct_spending',
        },
        'spending': {
            'minimum_monthly_spending_cents': 100000,
            'monthly_spending_cents': 180000,
        },
        'income': {
            'guaranteed_monthly_income_cents': 50000,
        },
        'tax_profile': {
            'present': False,
        },
        'annuities': [
            {'annuity_id': 'annuity_001', 'phase': 'deferred_accumulation'},
            {'annuity_id': 'annuity_002', 'phase': 'payout'},
        ],
    }
    if cash_cents is not None:
        obj['liquidity'] = {'cash_cents': cash_cents}
    return obj


def test_planning_snapshot_uses_truth_cash_and_does_not_mutate_truth(tmp_path: Path) -> None:
    day = '2030-01-15'
    out_dir = OUTPUT_BASE / day
    if out_dir.exists():
        shutil.rmtree(out_dir)
    truth_root = tmp_path / 'truth_root'
    cash_path = truth_root / 'cash_ledger_v1' / 'snapshots' / day / 'cash_ledger_snapshot.v1.json'
    _write_json(cash_path, _cash_snapshot(day, 125000))
    household_path = tmp_path / 'household.json'
    _write_json(household_path, _household_input(cash_cents=None))
    before = _sha_tree(truth_root)
    cmd = [
        sys.executable,
        str(TOOL),
        '--day_utc', day,
        '--mode', 'PAPER',
        '--advisor_household_input_json', str(household_path),
        '--truth_root', str(truth_root),
    ]
    first = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    second = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    assert first.returncode == 0, first.stderr or first.stdout
    assert second.returncode == 0, second.stderr or second.stdout
    assert 'action=EXISTS_IDENTICAL' in second.stdout
    assert _sha_tree(truth_root) == before
    output_path = out_dir / 'planning_snapshot.v1.json'
    obj = json.loads(output_path.read_text(encoding='utf-8'))
    assert obj['liquidity']['cash_cents'] == 125000
    assert obj['accounts']['taxable_account_id'] == 'acct_taxable'


def test_planning_snapshot_cash_mismatch_fails_closed(tmp_path: Path) -> None:
    day = '2030-01-16'
    out_dir = OUTPUT_BASE / day
    if out_dir.exists():
        shutil.rmtree(out_dir)
    truth_root = tmp_path / 'truth_root'
    cash_path = truth_root / 'cash_ledger_v1' / 'snapshots' / day / 'cash_ledger_snapshot.v1.json'
    _write_json(cash_path, _cash_snapshot(day, 125000))
    household_path = tmp_path / 'household.json'
    _write_json(household_path, _household_input(cash_cents=126000))
    cmd = [
        sys.executable,
        str(TOOL),
        '--day_utc', day,
        '--mode', 'PAPER',
        '--advisor_household_input_json', str(household_path),
        '--truth_root', str(truth_root),
    ]
    result = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    assert result.returncode != 0
    assert 'CASH_CENTS_MISMATCH' in (result.stderr + result.stdout)
