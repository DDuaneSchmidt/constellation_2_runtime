from __future__ import annotations

from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_sha256_hex_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.common.advisory.advisory_domain_constants_v1 import (
    ADVISORY_VALIDITY_TIERS_V1,
    HOUSEHOLD_SNAPSHOT_BUILDER_VERSION_V1,
    HOUSEHOLD_SNAPSHOT_CONTRACT_VERSION_V1,
    VALUE_BASIS_COMPLETENESS_STATUSES_V1,
    VALUE_BASIS_FRESHNESS_STATUSES_V1,
    VALUE_BASIS_STATUSES_V1,
    VALUE_BASIS_VALUATION_MODES_V1,
)
from constellation_2.common.advisory.advisory_storage_v1 import (
    household_snapshot_path_v1,
    write_immutable_json_v1,
)
from constellation_2.common.advisory.household_snapshot_v1 import HouseholdSnapshotV1
from constellation_2.common.advisory.policy_v1 import PolicyV1

POSITIONS_SCHEMA_BY_VERSION = {
    ('C2_POSITIONS_SNAPSHOT_V2', 2): 'governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v2.schema.json',
    ('C2_POSITIONS_SNAPSHOT_V3', 3): 'governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v3.schema.json',
    ('C2_POSITIONS_SNAPSHOT_V4', 4): 'governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v4.schema.json',
}
CASH_LEDGER_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/CASH_LEDGER/cash_ledger_snapshot.v1.schema.json'
NORMALIZED_HOLDINGS_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/LEGACY_HOLDINGS/normalized_holdings_artifact.v1.schema.json'
REPO_ROOT = Path(__file__).resolve().parents[3]


def _require_nonempty_str(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f'{field.upper()}_REQUIRED')
    return value.strip()


def _normalize_string_list(value: Any, *, field: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError(f'{field.upper()}_NOT_ARRAY')
    items: list[str] = []
    for raw in value:
        if not isinstance(raw, str) or not raw.strip():
            raise ValueError(f'{field.upper()}_ITEM_INVALID')
        items.append(raw.strip())
    return tuple(sorted(set(items)))


def _normalize_target_day(timestamp: str) -> str:
    value = _require_nonempty_str(timestamp, field='effective_at')
    if len(value) < 10:
        raise ValueError('EFFECTIVE_AT_INVALID')
    return value[:10]


def _normalize_account_registry_snapshot(raw: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError('ACCOUNT_REGISTRY_SNAPSHOT_NOT_OBJECT')
    accounts = raw.get('accounts')
    if not isinstance(accounts, list) or not accounts:
        raise ValueError('ACCOUNT_REGISTRY_ACCOUNTS_REQUIRED')
    source_refs = _normalize_string_list(raw.get('source_refs'), field='account_registry_source_refs')
    if not source_refs:
        raise ValueError('ACCOUNT_REGISTRY_SOURCE_REFS_REQUIRED')
    normalized_accounts = []
    seen: set[str] = set()
    for idx, item in enumerate(accounts):
        if not isinstance(item, dict):
            raise ValueError(f'ACCOUNT_REGISTRY_ACCOUNT_NOT_OBJECT:{idx}')
        account_id = _require_nonempty_str(item.get('account_id'), field=f'account_registry_accounts[{idx}].account_id')
        if account_id in seen:
            raise ValueError(f'DUPLICATE_CORE_ACCOUNT_ID:{account_id}')
        seen.add(account_id)
        scope_role = _require_nonempty_str(item.get('scope_role'), field=f'account_registry_accounts[{idx}].scope_role')
        source_ref = _require_nonempty_str(item.get('source_ref'), field=f'account_registry_accounts[{idx}].source_ref')
        source_type = _require_nonempty_str(item.get('source_type'), field=f'account_registry_accounts[{idx}].source_type')
        verification_status = _require_nonempty_str(item.get('verification_status'), field=f'account_registry_accounts[{idx}].verification_status').upper()
        if verification_status not in {'VERIFIED', 'UNVERIFIED'}:
            raise ValueError(f'ACCOUNT_REGISTRY_VERIFICATION_STATUS_INVALID:{account_id}')
        normalized_accounts.append(
            {
                'account_id': account_id,
                'scope_role': scope_role,
                'source_ref': source_ref,
                'source_type': source_type,
                'verification_status': verification_status,
            }
        )
    return {
        'source_refs': list(source_refs),
        'accounts': sorted(normalized_accounts, key=lambda item: (item['account_id'], item['scope_role'], item['source_ref'])),
    }


def _validate_positions_snapshot(raw: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError('POSITIONS_SNAPSHOT_NOT_OBJECT')
    schema_key = (str(raw.get('schema_id') or '').strip(), int(raw.get('schema_version') or 0))
    schema_relpath = POSITIONS_SCHEMA_BY_VERSION.get(schema_key)
    if schema_relpath is None:
        raise ValueError('UNSUPPORTED_POSITIONS_SNAPSHOT_VERSION')
    validate_against_repo_schema_v1(raw, REPO_ROOT, schema_relpath)
    return raw


def _normalize_positions_inputs(raw_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> tuple[dict[str, Any], ...]:
    normalized = []
    seen_refs: set[tuple[str, str]] = set()
    for idx, item in enumerate(raw_inputs):
        if not isinstance(item, dict):
            raise ValueError(f'POSITIONS_INPUT_NOT_OBJECT:{idx}')
        account_id = _require_nonempty_str(item.get('account_id'), field=f'positions_inputs[{idx}].account_id')
        record_ref = _require_nonempty_str(item.get('record_ref'), field=f'positions_inputs[{idx}].record_ref')
        key = (account_id, record_ref)
        if key in seen_refs:
            raise ValueError(f'DUPLICATE_POSITIONS_INPUT:{account_id}:{record_ref}')
        seen_refs.add(key)
        snapshot = _validate_positions_snapshot(item.get('snapshot'))
        normalized.append({'account_id': account_id, 'record_ref': record_ref, 'snapshot': snapshot})
    return tuple(sorted(normalized, key=lambda item: (item['account_id'], item['record_ref'])))


def _normalize_cash_inputs(raw_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> tuple[dict[str, Any], ...]:
    normalized = []
    seen_refs: set[tuple[str, str]] = set()
    for idx, item in enumerate(raw_inputs):
        if not isinstance(item, dict):
            raise ValueError(f'CASH_INPUT_NOT_OBJECT:{idx}')
        account_id = _require_nonempty_str(item.get('account_id'), field=f'cash_inputs[{idx}].account_id')
        record_ref = _require_nonempty_str(item.get('record_ref'), field=f'cash_inputs[{idx}].record_ref')
        key = (account_id, record_ref)
        if key in seen_refs:
            raise ValueError(f'DUPLICATE_CASH_INPUT:{account_id}:{record_ref}')
        seen_refs.add(key)
        snapshot = item.get('snapshot')
        if not isinstance(snapshot, dict):
            raise ValueError(f'CASH_SNAPSHOT_NOT_OBJECT:{idx}')
        validate_against_repo_schema_v1(snapshot, REPO_ROOT, CASH_LEDGER_SCHEMA_RELPATH)
        normalized.append({'account_id': account_id, 'record_ref': record_ref, 'snapshot': snapshot})
    return tuple(sorted(normalized, key=lambda item: (item['account_id'], item['record_ref'])))


def _normalize_external_holdings_inputs(raw_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> tuple[dict[str, Any], ...]:
    normalized = []
    seen_refs: set[tuple[str, str]] = set()
    for idx, item in enumerate(raw_inputs):
        if not isinstance(item, dict):
            raise ValueError(f'EXTERNAL_HOLDINGS_INPUT_NOT_OBJECT:{idx}')
        account_id = _require_nonempty_str(item.get('account_id'), field=f'external_holdings_inputs[{idx}].account_id')
        record_ref = _require_nonempty_str(item.get('record_ref'), field=f'external_holdings_inputs[{idx}].record_ref')
        key = (account_id, record_ref)
        if key in seen_refs:
            raise ValueError(f'DUPLICATE_EXTERNAL_HOLDINGS_INPUT:{account_id}:{record_ref}')
        seen_refs.add(key)
        artifact = item.get('artifact')
        if not isinstance(artifact, dict):
            raise ValueError(f'EXTERNAL_HOLDINGS_ARTIFACT_NOT_OBJECT:{idx}')
        validate_against_repo_schema_v1(artifact, REPO_ROOT, NORMALIZED_HOLDINGS_SCHEMA_RELPATH)
        normalized.append({'account_id': account_id, 'record_ref': record_ref, 'artifact': artifact})
    return tuple(sorted(normalized, key=lambda item: (item['account_id'], item['record_ref'])))


def _normalize_asset_mark_inputs(raw_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> tuple[dict[str, Any], ...]:
    normalized = []
    seen_refs: set[str] = set()
    seen_symbols: set[str] = set()
    for idx, item in enumerate(raw_inputs):
        if not isinstance(item, dict):
            raise ValueError(f'ASSET_MARK_INPUT_NOT_OBJECT:{idx}')
        record_ref = _require_nonempty_str(item.get('record_ref'), field=f'asset_mark_inputs[{idx}].record_ref')
        if record_ref in seen_refs:
            raise ValueError(f'DUPLICATE_ASSET_MARK_INPUT:{record_ref}')
        seen_refs.add(record_ref)
        snapshot = item.get('snapshot')
        if not isinstance(snapshot, dict):
            raise ValueError(f'ASSET_MARK_SNAPSHOT_NOT_OBJECT:{idx}')
        asof_utc = _require_nonempty_str(snapshot.get('asof_utc'), field=f'asset_mark_inputs[{idx}].snapshot.asof_utc')
        raw_marks = snapshot.get('marks')
        if not isinstance(raw_marks, list) or not raw_marks:
            raise ValueError(f'ASSET_MARKS_REQUIRED:{idx}')
        marks = []
        for mark_idx, raw_mark in enumerate(raw_marks):
            if not isinstance(raw_mark, dict):
                raise ValueError(f'ASSET_MARK_NOT_OBJECT:{idx}:{mark_idx}')
            symbol = _require_nonempty_str(raw_mark.get('symbol'), field=f'asset_mark_inputs[{idx}].marks[{mark_idx}].symbol').upper()
            if symbol in seen_symbols:
                raise ValueError(f'DUPLICATE_ASSET_MARK_SYMBOL:{symbol}')
            seen_symbols.add(symbol)
            asset_type = _require_nonempty_str(raw_mark.get('asset_type'), field=f'asset_mark_inputs[{idx}].marks[{mark_idx}].asset_type').upper()
            currency = _require_nonempty_str(raw_mark.get('currency'), field=f'asset_mark_inputs[{idx}].marks[{mark_idx}].currency').upper()
            mark_price_cents = raw_mark.get('mark_price_cents')
            if not isinstance(mark_price_cents, int) or mark_price_cents <= 0:
                raise ValueError(f'ASSET_MARK_PRICE_CENTS_INVALID:{symbol}')
            marks.append(
                {
                    'symbol': symbol,
                    'asset_type': asset_type,
                    'currency': currency,
                    'mark_price_cents': mark_price_cents,
                    'source_record_ref': record_ref,
                    'observed_at': asof_utc,
                }
            )
        normalized.append({'record_ref': record_ref, 'asof_utc': asof_utc, 'marks': tuple(sorted(marks, key=lambda item: item['symbol']))})
    return tuple(sorted(normalized, key=lambda item: item['record_ref']))


def _positions_freshness_attestation(target_day: str, item: dict[str, Any]) -> dict[str, Any]:
    snapshot = item['snapshot']
    positions = snapshot.get('positions') if isinstance(snapshot.get('positions'), dict) else {}
    observed_at = str(positions.get('asof_utc') or '').strip() or None
    produced_at = str(snapshot.get('produced_utc') or '').strip() or None
    day_utc = str(snapshot.get('day_utc') or '').strip()
    freshness_status = 'CURRENT' if day_utc == target_day and observed_at and observed_at.startswith(target_day) else 'STALE'
    return {
        'component_type': 'verified_positions_snapshot',
        'record_ref': item['record_ref'],
        'account_id': item['account_id'],
        'freshness_rule': 'TARGET_DAY_MATCH_AND_ASOF_PRESENT',
        'freshness_status': freshness_status,
        'observed_at': observed_at,
        'produced_at': produced_at,
    }


def _cash_freshness_attestation(target_day: str, item: dict[str, Any]) -> dict[str, Any]:
    snapshot = item['snapshot']
    payload = snapshot.get('snapshot') if isinstance(snapshot.get('snapshot'), dict) else {}
    observed_at = str(payload.get('observed_at_utc') or '').strip() or None
    produced_at = str(snapshot.get('produced_utc') or '').strip() or None
    day_utc = str(snapshot.get('day_utc') or '').strip()
    freshness_status = 'CURRENT' if day_utc == target_day and observed_at and observed_at.startswith(target_day) else 'STALE'
    return {
        'component_type': 'verified_cash_snapshot',
        'record_ref': item['record_ref'],
        'account_id': item['account_id'],
        'freshness_rule': 'TARGET_DAY_MATCH_AND_OBSERVED_AT_PRESENT',
        'freshness_status': freshness_status,
        'observed_at': observed_at,
        'produced_at': produced_at,
    }


def _external_freshness_attestation(target_day: str, item: dict[str, Any]) -> dict[str, Any]:
    artifact = item['artifact']
    observed_at = str(artifact.get('as_of_date') or '').strip() or None
    produced_at = str(artifact.get('created_at') or '').strip() or None
    freshness_status = 'CURRENT' if observed_at == target_day else 'STALE'
    return {
        'component_type': 'unverified_external_holdings',
        'record_ref': item['record_ref'],
        'account_id': item['account_id'],
        'freshness_rule': 'AS_OF_DATE_EQUALS_TARGET_DAY',
        'freshness_status': freshness_status,
        'observed_at': observed_at,
        'produced_at': produced_at,
    }


def _component_entry(*, component_type: str, account_id: str, record_ref: str, source_kind: str, freshness_status: str) -> dict[str, Any]:
    return {
        'component_type': component_type,
        'account_id': account_id,
        'record_ref': record_ref,
        'source_kind': source_kind,
        'freshness_status': freshness_status,
    }


def _verified_position_entry(*, position: dict[str, Any], record_ref: str) -> dict[str, Any]:
    instrument = position['instrument']
    return {
        'position_id': str(position['position_id']),
        'engine_id': str(position['engine_id']),
        'instrument': {
            'kind': str(instrument.get('kind') or ''),
            'symbol': str(instrument.get('symbol') or instrument.get('underlying') or ''),
            'currency': str(instrument.get('currency') or ''),
            'ib_conId': instrument.get('ib_conId'),
            'ib_localSymbol': instrument.get('ib_localSymbol'),
        },
        'qty': int(position['qty']),
        'avg_cost_cents': int(position['avg_cost_cents']),
        'market_exposure_type': str(position['market_exposure_type']),
        'max_loss_cents': None if position.get('max_loss_cents') is None else int(position['max_loss_cents']),
        'opened_day_utc': str(position['opened_day_utc']),
        'status': str(position['status']),
        'source_record_ref': record_ref,
    }


def _value_basis_status(
    *,
    target_day: str,
    cash_value_cents: int,
    holdings_by_account: list[dict[str, Any]],
    asset_mark_inputs: tuple[dict[str, Any], ...],
    cash_inputs: tuple[dict[str, Any], ...],
) -> dict[str, Any]:
    cash_lineage_refs = sorted(item['record_ref'] for item in cash_inputs)
    if not asset_mark_inputs:
        scope = {
            'status': 'BLOCKED',
            'valuation_mode': 'UNAVAILABLE',
            'effective_at': None,
            'completeness_status': 'INCOMPLETE',
            'freshness_status': 'UNKNOWN',
            'source_lineage_refs': cash_lineage_refs,
            'asset_marks': [],
            'position_values': [],
            'cash_value_cents': cash_value_cents,
            'total_portfolio_value_cents': None,
            'reason_codes': ['GOVERNED_ASSET_MARKS_MISSING'],
        }
        scope['value_basis_id'] = canonical_sha256_hex_v1(scope)
        return scope

    asof_values = sorted({str(item['asof_utc']) for item in asset_mark_inputs})
    asset_marks = sorted(
        (
            {
                **dict(mark),
                'freshness_status': 'CURRENT' if str(mark['observed_at']).startswith(target_day) else 'STALE',
            }
            for item in asset_mark_inputs
            for mark in item['marks']
        ),
        key=lambda item: item['symbol'],
    )
    mark_map = {str(item['symbol']): dict(item) for item in asset_marks}
    position_values = []
    missing_mark_symbols: set[str] = set()
    unsupported_position_ids: set[str] = set()
    for account in holdings_by_account:
        for position in account['verified_positions']:
            instrument = dict(position['instrument'])
            kind = str(instrument.get('kind') or '').upper()
            symbol = str(instrument.get('symbol') or '').upper()
            if kind != 'EQUITY':
                unsupported_position_ids.add(str(position['position_id']))
                continue
            mark = mark_map.get(symbol)
            if mark is None:
                missing_mark_symbols.add(symbol)
                continue
            quantity_shares = int(position['qty'])
            position_values.append(
                {
                    'account_id': str(account['account_id']),
                    'position_id': str(position['position_id']),
                    'symbol': symbol,
                    'asset_type': 'EQUITY',
                    'currency': str(mark['currency']),
                    'quantity_shares': quantity_shares,
                    'mark_price_cents': int(mark['mark_price_cents']),
                    'market_value_cents': quantity_shares * int(mark['mark_price_cents']),
                    'source_record_ref': str(mark['source_record_ref']),
                }
            )
    position_values = sorted(position_values, key=lambda item: (item['account_id'], item['symbol'], item['position_id']))
    reason_codes: list[str] = []
    effective_at = asof_values[0] if len(asof_values) == 1 else None
    if len(asof_values) != 1:
        reason_codes.append('INCONSISTENT_MARK_EFFECTIVE_AT')
    if missing_mark_symbols:
        reason_codes.append('GOVERNED_MARKS_MISSING_FOR_OPEN_POSITIONS')
    if unsupported_position_ids:
        reason_codes.append('UNSUPPORTED_OPEN_POSITION_KIND_FOR_VALUE_BASIS')
    freshness_status = 'CURRENT' if all(str(item['observed_at']).startswith(target_day) for item in asset_marks) else 'STALE'
    if freshness_status == 'STALE':
        reason_codes.append('GOVERNED_MARKS_STALE')
    completeness_status = 'COMPLETE' if not missing_mark_symbols and not unsupported_position_ids and len(asof_values) == 1 else 'INCOMPLETE'
    status = 'VALID' if not reason_codes else 'BLOCKED'
    valuation_mode = 'GOVERNED_EQUITY_MARKS_WITH_VERIFIED_CASH_V1'
    if completeness_status not in VALUE_BASIS_COMPLETENESS_STATUSES_V1:
        raise ValueError('VALUE_BASIS_COMPLETENESS_STATUS_INVALID')
    if freshness_status not in VALUE_BASIS_FRESHNESS_STATUSES_V1:
        raise ValueError('VALUE_BASIS_FRESHNESS_STATUS_INVALID')
    if status not in VALUE_BASIS_STATUSES_V1:
        raise ValueError('VALUE_BASIS_STATUS_INVALID')
    if valuation_mode not in VALUE_BASIS_VALUATION_MODES_V1:
        raise ValueError('VALUE_BASIS_VALUATION_MODE_INVALID')
    if not reason_codes:
        reason_codes.append('VALUE_BASIS_VALID')
    total_position_value_cents = sum(int(item['market_value_cents']) for item in position_values)
    scope = {
        'status': status,
        'valuation_mode': valuation_mode,
        'effective_at': effective_at,
        'completeness_status': completeness_status,
        'freshness_status': freshness_status,
        'source_lineage_refs': sorted({*cash_lineage_refs, *[str(item['record_ref']) for item in asset_mark_inputs]}),
        'asset_marks': asset_marks,
        'position_values': position_values,
        'cash_value_cents': cash_value_cents,
        'total_portfolio_value_cents': None if status != 'VALID' else cash_value_cents + total_position_value_cents,
        'reason_codes': sorted(set(reason_codes)),
    }
    scope['value_basis_id'] = canonical_sha256_hex_v1(scope)
    return scope


def build_household_snapshot_candidate_v1(
    *,
    policy: PolicyV1,
    created_at: str,
    effective_at: str,
    actor_source: str,
    account_registry_snapshot: dict[str, Any],
    verified_positions_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    verified_cash_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    governed_asset_mark_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
    external_holdings_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
    classification_refs: list[str] | tuple[str, ...] = (),
    builder_version: str = HOUSEHOLD_SNAPSHOT_BUILDER_VERSION_V1,
) -> HouseholdSnapshotV1:
    if policy.policy_completeness_status != 'VALID':
        raise ValueError('PARENT_POLICY_NOT_VALID')
    if policy.validity_tier not in {'VALID_ADVISORY_ONLY', 'VALID_EXECUTION_ELIGIBLE'}:
        raise ValueError('PARENT_POLICY_INVALID_TIER')

    created_at_clean = _require_nonempty_str(created_at, field='created_at')
    effective_at_clean = _require_nonempty_str(effective_at, field='effective_at')
    actor_source_clean = _require_nonempty_str(actor_source, field='actor_source')
    builder_version_clean = _require_nonempty_str(builder_version, field='builder_version')
    if builder_version_clean != HOUSEHOLD_SNAPSHOT_BUILDER_VERSION_V1:
        raise ValueError('UNSUPPORTED_HOUSEHOLD_SNAPSHOT_BUILDER_VERSION')
    target_day = _normalize_target_day(effective_at_clean)

    account_scope = _normalize_account_registry_snapshot(account_registry_snapshot)
    positions_inputs = _normalize_positions_inputs(list(verified_positions_inputs))
    cash_inputs = _normalize_cash_inputs(list(verified_cash_inputs))
    asset_mark_inputs = _normalize_asset_mark_inputs(list(governed_asset_mark_inputs))
    external_inputs = _normalize_external_holdings_inputs(list(external_holdings_inputs))
    classification_refs_norm = _normalize_string_list(list(classification_refs), field='classification_refs')

    scope_account_ids = {item['account_id'] for item in account_scope['accounts']}
    if not scope_account_ids:
        raise ValueError('MISSING_REQUIRED_CORE_ACCOUNT_SCOPE')
    for item in positions_inputs + cash_inputs + external_inputs:
        if item['account_id'] not in scope_account_ids:
            raise ValueError(f'INPUT_ACCOUNT_OUTSIDE_SCOPE:{item["account_id"]}')

    seen_position_ids: dict[str, str] = {}
    for item in positions_inputs:
        positions = item['snapshot']['positions']['items']
        for position in positions:
            position_id = str(position.get('position_id') or '').strip()
            prior = seen_position_ids.get(position_id)
            if prior is not None and prior != item['account_id']:
                raise ValueError(f'DUPLICATE_POSITION_IDENTITY_COLLISION:{position_id}')
            seen_position_ids[position_id] = item['account_id']

    positions_by_account: dict[str, dict[str, Any]] = {account_id: {'open_ids': [], 'defined_risk': 0, 'undefined_risk': 0} for account_id in scope_account_ids}
    for item in positions_inputs:
        for position in item['snapshot']['positions']['items']:
            if str(position.get('status') or '').strip().upper() != 'OPEN':
                continue
            account_bucket = positions_by_account[item['account_id']]
            position_id = str(position.get('position_id') or '').strip()
            account_bucket['open_ids'].append(position_id)
            exposure_type = str(position.get('market_exposure_type') or '').strip().upper()
            if exposure_type == 'DEFINED_RISK':
                account_bucket['defined_risk'] += 1
            else:
                account_bucket['undefined_risk'] += 1
    for bucket in positions_by_account.values():
        bucket['open_ids'] = sorted(set(bucket['open_ids']))

    cash_by_account: dict[str, dict[str, Any]] = {}
    for item in cash_inputs:
        snap = item['snapshot']['snapshot']
        cash_by_account[item['account_id']] = {
            'cash_total_cents': int(snap['cash_total_cents']),
            'available_funds_cents': None if snap['available_funds_cents'] is None else int(snap['available_funds_cents']),
            'excess_liquidity_cents': None if snap['excess_liquidity_cents'] is None else int(snap['excess_liquidity_cents']),
        }

    external_by_account: dict[str, dict[str, Any]] = {account_id: {'holding_ids': []} for account_id in scope_account_ids}
    for item in external_inputs:
        artifact = item['artifact']
        external_by_account[item['account_id']]['holding_ids'].extend(
            sorted(str(holding.get('holding_id') or '').strip() for holding in artifact['holdings'] if str(holding.get('holding_id') or '').strip())
        )
    for bucket in external_by_account.values():
        bucket['holding_ids'] = sorted(set(bucket['holding_ids']))

    freshness_attestations = sorted(
        [_positions_freshness_attestation(target_day, item) for item in positions_inputs]
        + [_cash_freshness_attestation(target_day, item) for item in cash_inputs]
        + [_external_freshness_attestation(target_day, item) for item in external_inputs],
        key=lambda item: (item['component_type'], item['account_id'], item['record_ref']),
    )

    verified_components = sorted(
        [
            _component_entry(
                component_type='verified_positions_snapshot',
                account_id=item['account_id'],
                record_ref=item['record_ref'],
                source_kind='positions_snapshot',
                freshness_status=next(att['freshness_status'] for att in freshness_attestations if att['record_ref'] == item['record_ref']),
            )
            for item in positions_inputs
        ]
        + [
            _component_entry(
                component_type='verified_cash_snapshot',
                account_id=item['account_id'],
                record_ref=item['record_ref'],
                source_kind='cash_ledger_snapshot',
                freshness_status=next(att['freshness_status'] for att in freshness_attestations if att['record_ref'] == item['record_ref']),
            )
            for item in cash_inputs
        ],
        key=lambda item: (item['component_type'], item['account_id'], item['record_ref']),
    )
    unverified_components = sorted(
        [
            _component_entry(
                component_type='unverified_external_holdings',
                account_id=item['account_id'],
                record_ref=item['record_ref'],
                source_kind='normalized_holdings_artifact',
                freshness_status=next(att['freshness_status'] for att in freshness_attestations if att['record_ref'] == item['record_ref']),
            )
            for item in external_inputs
        ],
        key=lambda item: (item['component_type'], item['account_id'], item['record_ref']),
    )

    missing_components = []
    for account_id in sorted(scope_account_ids):
        if account_id not in cash_by_account:
            missing_components.append({'component_type': 'verified_cash_snapshot', 'account_id': account_id, 'reason_code': 'MISSING_VERIFIED_CASH'})
        if not positions_by_account[account_id]['open_ids'] and account_id not in {item['account_id'] for item in positions_inputs}:
            missing_components.append({'component_type': 'verified_positions_snapshot', 'account_id': account_id, 'reason_code': 'MISSING_VERIFIED_POSITIONS'})
    missing_components = sorted(missing_components, key=lambda item: (item['component_type'], item['account_id'], item['reason_code']))

    holdings_by_account = []
    for account in account_scope['accounts']:
        account_id = account['account_id']
        verified_cash = cash_by_account.get(account_id)
        verified_positions = []
        for item in positions_inputs:
            if item['account_id'] != account_id:
                continue
            for position in item['snapshot']['positions']['items']:
                if str(position.get('status') or '').strip().upper() != 'OPEN':
                    continue
                verified_positions.append(_verified_position_entry(position=position, record_ref=item['record_ref']))
        verified_positions = sorted(verified_positions, key=lambda item: (item['position_id'], item['instrument']['symbol'], item['source_record_ref']))
        holdings_by_account.append(
            {
                'account_id': account_id,
                'scope_role': account['scope_role'],
                'verified_position_ids': positions_by_account[account_id]['open_ids'],
                'verified_positions': verified_positions,
                'verified_open_position_count': len(positions_by_account[account_id]['open_ids']),
                'unverified_external_holding_ids': external_by_account[account_id]['holding_ids'],
                'unverified_external_holding_count': len(external_by_account[account_id]['holding_ids']),
                'verified_cash_total_cents': None if verified_cash is None else verified_cash['cash_total_cents'],
                'source_refs': sorted(
                    {
                        account['source_ref'],
                        *[item['record_ref'] for item in positions_inputs if item['account_id'] == account_id],
                        *[item['record_ref'] for item in cash_inputs if item['account_id'] == account_id],
                        *[item['record_ref'] for item in external_inputs if item['account_id'] == account_id],
                    }
                ),
            }
        )
    holdings_by_account = sorted(holdings_by_account, key=lambda item: item['account_id'])

    all_available_funds = [item['available_funds_cents'] for item in cash_by_account.values()]
    all_excess_liquidity = [item['excess_liquidity_cents'] for item in cash_by_account.values()]
    cash_liquidity_summary = {
        'verified_cash_total_cents': sum(item['cash_total_cents'] for item in cash_by_account.values()),
        'verified_available_funds_total_cents': None if any(item is None for item in all_available_funds) else sum(int(item) for item in all_available_funds),
        'verified_excess_liquidity_total_cents': None if any(item is None for item in all_excess_liquidity) else sum(int(item) for item in all_excess_liquidity),
        'verified_cash_account_count': len(cash_by_account),
    }
    investable_asset_summary = {
        'verified_open_position_count': sum(len(item['open_ids']) for item in positions_by_account.values()),
        'verified_defined_risk_position_count': sum(int(item['defined_risk']) for item in positions_by_account.values()),
        'verified_undefined_risk_position_count': sum(int(item['undefined_risk']) for item in positions_by_account.values()),
        'unverified_external_holding_count': sum(len(item['holding_ids']) for item in external_by_account.values()),
    }
    value_basis = _value_basis_status(
        target_day=target_day,
        cash_value_cents=int(cash_liquidity_summary['verified_cash_total_cents']),
        holdings_by_account=holdings_by_account,
        asset_mark_inputs=asset_mark_inputs,
        cash_inputs=cash_inputs,
    )

    stale_verified = any(item['freshness_status'] != 'CURRENT' for item in verified_components)
    has_unverified = bool(unverified_components) or any(account['verification_status'] == 'UNVERIFIED' for account in account_scope['accounts'])
    no_verified_core = not verified_components
    partial_coverage = bool(missing_components)

    reason_codes = []
    if partial_coverage:
        reason_codes.append('PARTIAL_HOUSEHOLD_COVERAGE')
    if stale_verified:
        reason_codes.append('STALE_VERIFIED_COMPONENTS_PRESENT')
    if has_unverified:
        reason_codes.append('UNVERIFIED_COMPONENTS_PRESENT')
    if no_verified_core:
        reason_codes.append('NO_VERIFIED_CORE_COMPONENTS')
    if not reason_codes:
        reason_codes.append('HOUSEHOLD_SNAPSHOT_COMPLETE')

    if no_verified_core or partial_coverage:
        validity_tier = 'INVALID_REMEDIABLE'
    elif stale_verified or has_unverified:
        validity_tier = 'VALID_ADVISORY_ONLY'
    else:
        validity_tier = 'VALID_EXECUTION_ELIGIBLE'
    if validity_tier not in ADVISORY_VALIDITY_TIERS_V1:
        raise ValueError('INVALID_VALIDITY_TIER_CONFIGURATION')

    input_record_refs = sorted(
        {
            f'policy_id:{policy.policy_id}',
            *account_scope['source_refs'],
            *[item['record_ref'] for item in positions_inputs],
            *[item['record_ref'] for item in cash_inputs],
            *[item['record_ref'] for item in asset_mark_inputs],
            *[item['record_ref'] for item in external_inputs],
        }
    )

    coverage_scope = {
        'included_account_ids': sorted(scope_account_ids),
        'explicit_account_coverage_count': len(scope_account_ids),
        'explicit_positions_account_count': len({item['account_id'] for item in positions_inputs}),
        'explicit_cash_account_count': len({item['account_id'] for item in cash_inputs}),
        'external_component_account_count': len({item['account_id'] for item in external_inputs}),
        'partial_coverage': partial_coverage,
    }
    validation_status = 'VALIDATED'
    completeness_status = 'PARTIAL' if partial_coverage else 'COMPLETE'
    freshness_status = 'STALE' if stale_verified else 'CURRENT'
    reconciliation_status = 'NOT_PROVIDED'

    snapshot_scope = {
        'household_id': policy.household_id,
        'parent_policy_id': policy.policy_id,
        'builder_version': builder_version_clean,
        'validation_status': validation_status,
        'completeness_status': completeness_status,
        'freshness_status': freshness_status,
        'reconciliation_status': reconciliation_status,
        'coverage_scope': coverage_scope,
        'account_registry_snapshot': account_scope,
        'investable_asset_summary': investable_asset_summary,
        'cash_liquidity_summary': cash_liquidity_summary,
        'holdings_by_account': holdings_by_account,
        'holdings_by_asset_class': [],
        'holdings_by_tax_treatment': [],
        'verified_components': verified_components,
        'unverified_components': unverified_components,
        'missing_components': missing_components,
        'input_record_refs': input_record_refs,
        'classification_refs': list(classification_refs_norm),
        'freshness_attestations': freshness_attestations,
        'value_basis': value_basis,
        'validity_tier': validity_tier,
        'reason_codes': sorted(set(reason_codes)),
        'contract_version': HOUSEHOLD_SNAPSHOT_CONTRACT_VERSION_V1,
        'snapshot_version': 'v1',
        'effective_day_utc': target_day,
    }
    household_snapshot_id = canonical_sha256_hex_v1(snapshot_scope)

    obj = {
        'schema_id': 'household_snapshot',
        'schema_version': 'v1',
        'record_id': household_snapshot_id,
        'household_snapshot_id': household_snapshot_id,
        'household_id': policy.household_id,
        'parent_policy_id': policy.policy_id,
        'snapshot_version': 'v1',
        'created_at': created_at_clean,
        'effective_at': effective_at_clean,
        'actor_source': actor_source_clean,
        'timestamp_basis': 'effective_at_day_and_input_observed_timestamps',
        'contract_version': HOUSEHOLD_SNAPSHOT_CONTRACT_VERSION_V1,
        'builder_version': builder_version_clean,
        'validation_status': validation_status,
        'completeness_status': completeness_status,
        'freshness_status': freshness_status,
        'reconciliation_status': reconciliation_status,
        'parent_lineage_refs': [f'policy_id:{policy.policy_id}'],
        'coverage_scope': coverage_scope,
        'account_registry_snapshot': account_scope,
        'investable_asset_summary': investable_asset_summary,
        'cash_liquidity_summary': cash_liquidity_summary,
        'holdings_by_account': holdings_by_account,
        'holdings_by_asset_class': [],
        'holdings_by_tax_treatment': [],
        'verified_components': verified_components,
        'unverified_components': unverified_components,
        'missing_components': missing_components,
        'input_record_refs': input_record_refs,
        'classification_refs': list(classification_refs_norm),
        'freshness_attestations': freshness_attestations,
        'value_basis': value_basis,
        'validity_tier': validity_tier,
        'reason_codes': sorted(set(reason_codes)),
    }
    return HouseholdSnapshotV1.from_dict(obj)


def evaluate_household_snapshot_candidate_v1(candidate: HouseholdSnapshotV1) -> dict[str, Any]:
    blocked_reason_codes = []
    if candidate.validation_status != 'VALIDATED':
        blocked_reason_codes.append('SNAPSHOT_VALIDATION_STATUS_INVALID')
    if candidate.completeness_status != 'COMPLETE' or bool(candidate.coverage_scope.get('partial_coverage')):
        blocked_reason_codes.append('SNAPSHOT_INCOMPLETE_ACCOUNT_SCOPE')
    if candidate.freshness_status != 'CURRENT':
        blocked_reason_codes.append('SNAPSHOT_STALE_VERIFIED_CORE')
    if candidate.validity_tier in {'INVALID_REMEDIABLE', 'INVALID_HARD_STOP'}:
        blocked_reason_codes.extend(candidate.reason_codes)

    if blocked_reason_codes:
        return {
            'outcome': 'blocked',
            'validity_tier': 'INVALID_REMEDIABLE',
            'reason_codes': sorted(set(str(item) for item in blocked_reason_codes)),
        }
    return {
        'outcome': 'valid',
        'validity_tier': candidate.validity_tier,
        'reason_codes': sorted(set(str(item) for item in candidate.reason_codes)),
    }


def build_household_snapshot_v1(
    *,
    policy: PolicyV1,
    created_at: str,
    effective_at: str,
    actor_source: str,
    account_registry_snapshot: dict[str, Any],
    verified_positions_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    verified_cash_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    governed_asset_mark_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
    external_holdings_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
    classification_refs: list[str] | tuple[str, ...] = (),
    builder_version: str = HOUSEHOLD_SNAPSHOT_BUILDER_VERSION_V1,
) -> HouseholdSnapshotV1:
    candidate = build_household_snapshot_candidate_v1(
        policy=policy,
        created_at=created_at,
        effective_at=effective_at,
        actor_source=actor_source,
        account_registry_snapshot=account_registry_snapshot,
        verified_positions_inputs=verified_positions_inputs,
        verified_cash_inputs=verified_cash_inputs,
        governed_asset_mark_inputs=governed_asset_mark_inputs,
        external_holdings_inputs=external_holdings_inputs,
        classification_refs=classification_refs,
        builder_version=builder_version,
    )
    evaluation = evaluate_household_snapshot_candidate_v1(candidate)
    if evaluation['outcome'] != 'valid':
        raise ValueError(f'HOUSEHOLD_SNAPSHOT_VALIDATION_BLOCKED:{evaluation["reason_codes"][0]}')
    return candidate


def write_household_snapshot_v1(
    *,
    policy: PolicyV1,
    created_at: str,
    effective_at: str,
    actor_source: str,
    account_registry_snapshot: dict[str, Any],
    verified_positions_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    verified_cash_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    governed_asset_mark_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
    external_holdings_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
    classification_refs: list[str] | tuple[str, ...] = (),
    builder_version: str = HOUSEHOLD_SNAPSHOT_BUILDER_VERSION_V1,
    output_root: str = '',
) -> tuple[HouseholdSnapshotV1, str]:
    snapshot = build_household_snapshot_v1(
        policy=policy,
        created_at=created_at,
        effective_at=effective_at,
        actor_source=actor_source,
        account_registry_snapshot=account_registry_snapshot,
        verified_positions_inputs=verified_positions_inputs,
        verified_cash_inputs=verified_cash_inputs,
        governed_asset_mark_inputs=governed_asset_mark_inputs,
        external_holdings_inputs=external_holdings_inputs,
        classification_refs=classification_refs,
        builder_version=builder_version,
    )
    path = household_snapshot_path_v1(output_root, snapshot.household_id, snapshot.household_snapshot_id)
    written = write_immutable_json_v1(path, snapshot.to_dict())
    return snapshot, str(written)
