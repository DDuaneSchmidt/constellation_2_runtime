#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.domain_source_builders_v1 import build_domain_source_artifact_v1, setup_requirements_v1
from ops.aegis.domain_source_registry_v1 import domain_source_contract_v1, render_domain_source_path_v1


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def _valid_status(payload: dict[str, Any]) -> bool:
    text = ' '.join(str(payload.get(key) or '') for key in ('status', 'validation_status', 'certification_status')).upper()
    return any(token in text for token in ('VALID', 'CERTIFIED', 'READY', 'PASS', 'CURRENT'))


def _symbol_set(payload: dict[str, Any]) -> set[str]:
    values: set[str] = set()
    for key in ('tracked_symbols', 'symbols', 'covered_symbols', 'requested_symbols', 'fetched_symbols'):
        for item in payload.get(key, []) if isinstance(payload.get(key), list) else []:
            if str(item).strip():
                values.add(str(item).strip().upper())
    for key in ('events', 'actions', 'calendar_rows'):
        for row in payload.get(key, []) if isinstance(payload.get(key), list) else []:
            if isinstance(row, dict) and str(row.get('symbol') or '').strip():
                values.add(str(row.get('symbol')).strip().upper())
    return values


def validate_domain_source_v1(*, truth_root: Path, day_utc: str, domain_id: str) -> dict[str, Any]:
    contract = domain_source_contract_v1(domain_id)
    path = render_domain_source_path_v1(truth_root=truth_root, day_utc=day_utc, contract=contract) if contract else Path('')
    build = build_domain_source_artifact_v1(truth_root=truth_root, day_utc=day_utc, domain_id=domain_id)
    payload = _read_json(path) if path and path.exists() else {}
    source_exists = bool(path and path.exists() and path.is_file())
    errors: list[str] = []
    warnings: list[str] = []
    if not contract:
        errors.append('DOMAIN_SOURCE_CONTRACT_MISSING')
    if not source_exists:
        errors.append('DOMAIN_SOURCE_ARTIFACT_MISSING')
    if source_exists and not _valid_status(payload):
        errors.append('DOMAIN_SOURCE_STATUS_NOT_VALID')
    if str(build.get('result_status') or '').upper() in {'INVALID_SOURCE', 'FAILED'}:
        errors.extend(str(item) for item in build.get('errors', []) if str(item))
        if build.get('failure_reason'):
            errors.append(str(build.get('failure_reason')))
    if str(build.get('result_status') or '').upper() == 'SOURCE_SETUP_REQUIRED' and not source_exists:
        errors.append('SOURCE_SETUP_REQUIRED')
    missing_symbols: list[str] = []
    if domain_id in {'EARNINGS_EVENTS', 'CORPORATE_ACTIONS'} and source_exists:
        tracked = _symbol_set(payload)
        if not tracked:
            warnings.append('SYMBOL_COVERAGE_NOT_REPORTED')
    requirements = setup_requirements_v1(truth_root=truth_root, day_utc=day_utc, domain_id=domain_id)
    result = {
        'schema_id': 'domain_source_validation',
        'schema_version': 'v1',
        'domain_id': domain_id,
        'day_utc': day_utc,
        'source_contract': contract,
        'source_path': str(path),
        'source_exists': source_exists,
        'source_status': str(payload.get('validation_status') or payload.get('status') or build.get('result_status') or ''),
        'validation_status': 'VALID' if not errors else 'MISSING' if 'DOMAIN_SOURCE_ARTIFACT_MISSING' in errors or 'SOURCE_SETUP_REQUIRED' in errors else 'INVALID',
        'errors': sorted(set(errors)),
        'warnings': warnings,
        'missing_symbols': missing_symbols,
        'build_result': build,
        'setup_requirements': requirements,
        'broker_submit_transmit_allowed': False,
        'broker_execution_allowed': False,
        'autonomous_execution_allowed': False,
        'trade_advice_allowed': False,
    }
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='validate_domain_source_v1')
    parser.add_argument('--truth-root', '--truth_root', dest='truth_root', default='/home/node/constellation_runtime_data/truth')
    parser.add_argument('--day-utc', '--day', dest='day_utc', required=True)
    parser.add_argument('--domain-id', '--domain_id', dest='domain_id', required=True)
    args = parser.parse_args(argv)
    result = validate_domain_source_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), domain_id=str(args.domain_id).upper())
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get('validation_status') == 'VALID' else 2


if __name__ == '__main__':
    raise SystemExit(main())
