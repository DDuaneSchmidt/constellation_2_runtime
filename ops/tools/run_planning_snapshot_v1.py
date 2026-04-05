#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

try:
    from constellation_2.common.runtime_base_v1 import advisor_runtime_path, advisor_runtime_root, canonical_tools_root, ensure_repo_root_on_sys_path, source_root_from_file
except ModuleNotFoundError:  # pragma: no cover - direct script execution bootstrap
    import importlib.util

    _RUNTIME_BASE_PATH = Path(__file__).resolve().parents[2] / 'constellation_2' / 'common' / 'runtime_base_v1.py'
    _RUNTIME_BASE_SPEC = importlib.util.spec_from_file_location('constellation_2.common.runtime_base_v1', _RUNTIME_BASE_PATH)
    if _RUNTIME_BASE_SPEC is None or _RUNTIME_BASE_SPEC.loader is None:
        raise RuntimeError(f'RUNTIME_BASE_IMPORT_FAILED: {_RUNTIME_BASE_PATH}')
    _runtime_base_v1 = importlib.util.module_from_spec(_RUNTIME_BASE_SPEC)
    _RUNTIME_BASE_SPEC.loader.exec_module(_runtime_base_v1)
    advisor_runtime_path = _runtime_base_v1.advisor_runtime_path
    advisor_runtime_root = _runtime_base_v1.advisor_runtime_root
    canonical_tools_root = _runtime_base_v1.canonical_tools_root
    ensure_repo_root_on_sys_path = _runtime_base_v1.ensure_repo_root_on_sys_path
    source_root_from_file = _runtime_base_v1.source_root_from_file

REPO_ROOT = ensure_repo_root_on_sys_path(__file__)

from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

OUTPUT_BASE = advisor_runtime_root()
CASH_SNAPSHOT_SCHEMA = 'governance/04_DATA/SCHEMAS/C2/CASH_LEDGER/cash_ledger_snapshot.v1.schema.json'
VALID_ANNUITY_PHASES = {'deferred_accumulation', 'payout', 'closed'}


def _read_json_obj(path: Path) -> dict[str, Any]:
    obj = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(obj, dict):
        raise SystemExit(f'FAIL: TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


def _require_nonempty_str(value: Any, *, label: str) -> str:
    if value is None:
        raise SystemExit(f'FAIL: MISSING_REQUIRED_FIELD:{label}')
    out = str(value).strip()
    if not out or out == 'None':
        raise SystemExit(f'FAIL: MISSING_REQUIRED_FIELD:{label}')
    return out


def _require_nonnegative_int(value: Any, *, label: str) -> int:
    if value is None or isinstance(value, bool):
        raise SystemExit(f'FAIL: INVALID_INTEGER_FIELD:{label}')
    try:
        out = int(value)
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f'FAIL: INVALID_INTEGER_FIELD:{label}:{exc}') from exc
    if out < 0:
        raise SystemExit(f'FAIL: NEGATIVE_INTEGER_FIELD:{label}')
    return out


def _normalize_household_input(obj: dict[str, Any]) -> dict[str, Any]:
    for key in ('accounts', 'spending', 'income', 'tax_profile', 'annuities'):
        if key not in obj:
            raise SystemExit(f'FAIL: MISSING_REQUIRED_FIELD:{key}')
    accounts = obj['accounts']
    spending = obj['spending']
    income = obj['income']
    tax_profile = obj['tax_profile']
    annuities = obj['annuities']
    if not isinstance(accounts, dict) or not isinstance(spending, dict) or not isinstance(income, dict) or not isinstance(tax_profile, dict):
        raise SystemExit('FAIL: INVALID_NESTED_OBJECT')
    if not isinstance(annuities, list):
        raise SystemExit('FAIL: ANNUITIES_NOT_ARRAY')
    normalized_annuities = []
    for idx, item in enumerate(annuities):
        if not isinstance(item, dict):
            raise SystemExit(f'FAIL: ANNUITY_NOT_OBJECT:{idx}')
        annuity_id = _require_nonempty_str(item.get('annuity_id'), label=f'annuities[{idx}].annuity_id')
        phase = _require_nonempty_str(item.get('phase'), label=f'annuities[{idx}].phase')
        if phase not in VALID_ANNUITY_PHASES:
            raise SystemExit(f'FAIL: INVALID_ANNUITY_PHASE:{phase}')
        normalized_annuities.append({'annuity_id': annuity_id, 'phase': phase})
    normalized = {
        'accounts': {
            'taxable_account_id': _require_nonempty_str(accounts.get('taxable_account_id'), label='accounts.taxable_account_id'),
            'cash_reserve_account_id': _require_nonempty_str(accounts.get('cash_reserve_account_id'), label='accounts.cash_reserve_account_id'),
            'spending_account_id': _require_nonempty_str(accounts.get('spending_account_id'), label='accounts.spending_account_id'),
        },
        'spending': {
            'minimum_monthly_spending_cents': _require_nonnegative_int(spending.get('minimum_monthly_spending_cents'), label='spending.minimum_monthly_spending_cents'),
            'monthly_spending_cents': _require_nonnegative_int(spending.get('monthly_spending_cents'), label='spending.monthly_spending_cents'),
        },
        'income': {
            'guaranteed_monthly_income_cents': _require_nonnegative_int(income.get('guaranteed_monthly_income_cents'), label='income.guaranteed_monthly_income_cents'),
        },
        'tax_profile': {
            'present': tax_profile['present'] if isinstance(tax_profile.get('present'), bool) else (_ for _ in ()).throw(SystemExit('FAIL: INVALID_BOOLEAN_FIELD:tax_profile.present')),
        },
        'annuities': sorted(normalized_annuities, key=lambda item: (item['annuity_id'], item['phase'])),
    }
    liquidity_obj = obj.get('liquidity')
    if liquidity_obj is not None:
        if not isinstance(liquidity_obj, dict):
            raise SystemExit('FAIL: LIQUIDITY_NOT_OBJECT')
        normalized['liquidity'] = {
            'cash_cents': _require_nonnegative_int(liquidity_obj.get('cash_cents'), label='liquidity.cash_cents'),
        }
    return normalized


def _resolve_truth_root(raw: str) -> Path | None:
    value = str(raw).strip()
    if not value:
        return None
    path = Path(value).expanduser().resolve()
    if not path.is_absolute() or not path.exists() or not path.is_dir():
        raise SystemExit(f'FAIL: INVALID_TRUTH_ROOT:{path}')
    return path


def _resolve_cash_cents(*, day: str, truth_root: Path | None, household: dict[str, Any]) -> tuple[int, str, str | None]:
    explicit_cash = None
    if 'liquidity' in household:
        explicit_cash = int(household['liquidity']['cash_cents'])
    if truth_root is not None:
        cash_path = (truth_root / 'cash_ledger_v1' / 'snapshots' / day / 'cash_ledger_snapshot.v1.json').resolve()
        if cash_path.exists():
            cash_obj = _read_json_obj(cash_path)
            validate_against_repo_schema_v1(cash_obj, REPO_ROOT, CASH_SNAPSHOT_SCHEMA)
            truth_cash = _require_nonnegative_int(cash_obj['snapshot']['cash_total_cents'], label='cash_snapshot.snapshot.cash_total_cents')
            if explicit_cash is not None and explicit_cash != truth_cash:
                raise SystemExit(f'FAIL: CASH_CENTS_MISMATCH: explicit={explicit_cash} truth={truth_cash} path={cash_path}')
            return truth_cash, 'truth_cash_ledger_snapshot', str(cash_path)
    if explicit_cash is None:
        raise SystemExit('FAIL: LIQUIDITY_CASH_MISSING')
    return explicit_cash, 'advisor_input_liquidity', None


def _output_path(*, mode: str, day: str) -> Path:
    return (OUTPUT_BASE / mode / 'planning_snapshot_v1' / day / 'planning_snapshot.v1.json').resolve()


def main() -> int:
    parser = argparse.ArgumentParser(description='Produce governed planning_snapshot_v1 from explicit advisor inputs and optional Constellation truth cash state')
    parser.add_argument('--day_utc', required=True)
    parser.add_argument('--mode', required=True, choices=['PAPER', 'LIVE'])
    parser.add_argument('--advisor_household_input_json', required=True)
    parser.add_argument('--truth_root', default='')
    args = parser.parse_args()

    day = _require_nonempty_str(args.day_utc, label='day_utc')
    mode = _require_nonempty_str(args.mode, label='mode')
    input_path = Path(str(args.advisor_household_input_json).strip()).expanduser().resolve()
    if not input_path.exists():
        raise SystemExit(f'FAIL: ADVISOR_HOUSEHOLD_INPUT_MISSING:{input_path}')
    household = _normalize_household_input(_read_json_obj(input_path))
    truth_root = _resolve_truth_root(args.truth_root)
    cash_cents, liquidity_source, liquidity_path = _resolve_cash_cents(day=day, truth_root=truth_root, household=household)
    created_at = f'{day}T00:00:00Z'
    advisory_basis = {
        'day_utc': day,
        'mode': mode,
        'accounts': household['accounts'],
        'spending': household['spending'],
        'income': household['income'],
        'tax_profile': household['tax_profile'],
        'annuities': household['annuities'],
        'cash_cents': cash_cents,
    }
    advisory_packet_id = canonical_hash_for_c2_artifact_v1({'kind': 'advisor_packet_v1', 'basis': advisory_basis})
    snapshot_obj = {
        'schema_id': 'planning_snapshot',
        'schema_version': 'v1',
        'planning_snapshot_id': canonical_hash_for_c2_artifact_v1({'kind': 'planning_snapshot_v1', 'advisory_packet_id': advisory_packet_id, 'basis': advisory_basis}),
        'advisory_packet_id': advisory_packet_id,
        'created_at': created_at,
        'version': 'v1',
        'accounts': household['accounts'],
        'liquidity': {'cash_cents': cash_cents},
        'spending': household['spending'],
        'income': household['income'],
        'tax_profile': household['tax_profile'],
        'annuities': household['annuities'],
    }
    normalized = PlanningSnapshotV1.from_dict(snapshot_obj).to_dict()
    out_path = _output_path(mode=mode, day=day)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    new_bytes = canonical_json_bytes_v1(normalized) + b'\n'
    action = 'WROTE'
    if out_path.exists():
        if out_path.read_bytes() == new_bytes:
            action = 'EXISTS_IDENTICAL'
        else:
            action = 'REPLACED_STALE'
    out_path.write_bytes(new_bytes)
    extra = f' liquidity_path={liquidity_path}' if liquidity_path else ''
    print(f'OK: PLANNING_SNAPSHOT_V1_WRITTEN path={out_path} action={action} liquidity_source={liquidity_source}{extra}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
