#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_nav.v1.schema.json'
MODULE = 'ops/tools/bridge_accounting_nav_v2_to_compat_v1.py'


def _require_truth_root(raw: str) -> Path:
    p = Path(str(raw).strip()).expanduser().resolve()
    if not p.is_absolute() or (not p.exists()) or (not p.is_dir()):
        raise SystemExit(f'FAIL: invalid --truth_root: {p}')
    return p


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _stable_bytes(obj: Dict[str, Any]) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False) + '\n').encode('utf-8')


def _read_json_obj(p: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(p.read_text(encoding='utf-8'))
    except Exception as e:
        raise SystemExit(f'FAIL: JSON_PARSE_FAILED: {p}: {e!r}') from e
    if not isinstance(obj, dict):
        raise SystemExit(f'FAIL: JSON_NOT_OBJECT: {p}')
    return obj


def _git_sha() -> str:
    try:
        return subprocess.check_output(['/usr/bin/git', 'rev-parse', 'HEAD'], cwd=str(REPO_ROOT)).decode().strip()
    except Exception:
        return 'UNKNOWN'


def _parse_number(value: Any, *, field: str) -> int | float:
    if isinstance(value, bool):
        raise SystemExit(f'FAIL: INVALID_NUMERIC_FIELD: {field}')
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            raise SystemExit(f'FAIL: EMPTY_NUMERIC_FIELD: {field}')
        try:
            d = Decimal(s)
        except InvalidOperation as e:
            raise SystemExit(f'FAIL: BAD_NUMERIC_FIELD: {field} value={value!r}') from e
        if d == d.to_integral_value():
            return int(d)
        return float(s)
    raise SystemExit(f'FAIL: UNSUPPORTED_NUMERIC_FIELD: {field} type={type(value).__name__}')


def _derive_history(nav_v2: Dict[str, Any]) -> Dict[str, Any]:
    nav = nav_v2.get('nav') if isinstance(nav_v2.get('nav'), dict) else {}
    nav_total = nav.get('nav_total')
    if not isinstance(nav_total, int):
        raise SystemExit('FAIL: ACCOUNTING_V2_NAV_TOTAL_NOT_INT')
    hist = nav_v2.get('history')
    if not isinstance(hist, dict) or not hist:
        return {'peak_nav': int(nav_total), 'drawdown_abs': 0, 'drawdown_pct': '0.000000'}
    peak_nav = hist.get('peak_nav')
    drawdown_abs = hist.get('drawdown_abs')
    drawdown_pct = hist.get('drawdown_pct')
    if not isinstance(peak_nav, int):
        raise SystemExit('FAIL: ACCOUNTING_V2_PEAK_NAV_NOT_INT')
    if not isinstance(drawdown_abs, int):
        raise SystemExit('FAIL: ACCOUNTING_V2_DRAWDOWN_ABS_NOT_INT')
    if not isinstance(drawdown_pct, str) or not drawdown_pct.strip():
        raise SystemExit('FAIL: ACCOUNTING_V2_DRAWDOWN_PCT_MISSING_OR_NOT_STRING')
    dd = Decimal(drawdown_pct).quantize(Decimal('0.000001'))
    return {'peak_nav': int(peak_nav), 'drawdown_abs': int(drawdown_abs), 'drawdown_pct': f'{dd:.6f}'}


def _derive_status(source_status: str) -> tuple[str, list[str]]:
    s = source_status.strip().upper()
    if s in {'ACTIVE', 'OK', ''}:
        return 'OK', []
    if s == 'BOOTSTRAP':
        return 'OK', ['SOURCE_ACCOUNTING_V2_BOOTSTRAP']
    raise SystemExit(f'FAIL: UNSUPPORTED_SOURCE_STATUS_FOR_COMPAT_BRIDGE: {source_status!r}')


def _build_output(day: str, src_path: Path, src: Dict[str, Any]) -> Dict[str, Any]:
    nav = src.get('nav') if isinstance(src.get('nav'), dict) else None
    if nav is None:
        raise SystemExit('FAIL: ACCOUNTING_V2_NAV_OBJECT_MISSING')
    producer_src = src.get('producer') if isinstance(src.get('producer'), dict) else {}
    produced_utc = str(src.get('produced_utc') or '').strip()
    if not produced_utc:
        raise SystemExit('FAIL: ACCOUNTING_V2_PRODUCED_UTC_MISSING')
    out_status, extra_reason_codes = _derive_status(str(src.get('status') or ''))
    components = []
    raw_components = nav.get('components')
    if not isinstance(raw_components, list):
        raise SystemExit('FAIL: ACCOUNTING_V2_COMPONENTS_INVALID')
    for i, comp in enumerate(raw_components):
        if not isinstance(comp, dict):
            raise SystemExit(f'FAIL: ACCOUNTING_V2_COMPONENT_NOT_OBJECT idx={i}')
        mark = comp.get('mark') if isinstance(comp.get('mark'), dict) else None
        if mark is None:
            raise SystemExit(f'FAIL: ACCOUNTING_V2_MARK_MISSING idx={i}')
        components.append({
            'kind': str(comp.get('kind') or ''),
            'symbol': str(comp.get('symbol') or ''),
            'qty': _parse_number(comp.get('qty'), field=f'components[{i}].qty'),
            'mv': _parse_number(comp.get('mv'), field=f'components[{i}].mv'),
            'mark': {
                'bid': mark.get('bid'),
                'ask': mark.get('ask'),
                'last': mark.get('last'),
                'source': str(mark.get('source') or ''),
                'asof_utc': str(mark.get('asof_utc') or ''),
            },
        })
    reason_codes = ['BRIDGED_FROM_ACCOUNTING_NAV_V2']
    for rc in src.get('reason_codes') or []:
        if isinstance(rc, str) and rc.strip():
            reason_codes.append(f'SOURCE_{rc.strip()}')
    reason_codes.extend(extra_reason_codes)
    return {
        'schema_id': 'C2_ACCOUNTING_NAV_V1',
        'schema_version': 1,
        'produced_utc': produced_utc,
        'day_utc': day,
        'producer': {
            'repo': 'constellation_2_runtime',
            'git_sha': _git_sha(),
            'module': MODULE,
        },
        'status': out_status,
        'reason_codes': reason_codes,
        'input_manifest': [{
            'type': 'other',
            'path': str(src_path),
            'sha256': _sha256_file(src_path),
            'day_utc': day,
            'producer': 'accounting_nav_v2',
        }],
        'nav': {
            'currency': str(nav.get('currency') or ''),
            'nav_total': int(nav.get('nav_total')),
            'cash_total': int(nav.get('cash_total')),
            'gross_positions_value': int(nav.get('gross_positions_value')),
            'realized_pnl_to_date': int(nav.get('realized_pnl_to_date')),
            'unrealized_pnl': int(nav.get('unrealized_pnl')),
            'components': components,
            'notes': [str(x) for x in (nav.get('notes') or [])],
        },
        'history': _derive_history(src),
    }


def _validate_existing_or_reason(path: Path) -> tuple[bool, str]:
    try:
        obj = _read_json_obj(path)
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        return True, 'VALID'
    except BaseException as e:
        return False, str(e)


def _write_with_integrity(path: Path, obj: Dict[str, Any]) -> str:
    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
    data = _stable_bytes(obj)
    cand_sha = _sha256_bytes(data)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        valid, reason = _validate_existing_or_reason(path)
        if valid:
            existing = path.read_bytes()
            exist_sha = _sha256_bytes(existing)
            if exist_sha == cand_sha:
                return f'EXISTS_IDENTICAL sha256={cand_sha}'
            tmp = path.with_suffix(path.suffix + '.tmp')
            tmp.write_bytes(data)
            os.replace(tmp, path)
            return f'REPLACED_STALE prior_sha256={exist_sha} sha256={cand_sha}'
        path.unlink()
        tmp = path.with_suffix(path.suffix + '.tmp')
        tmp.write_bytes(data)
        os.replace(tmp, path)
        return f'REPLACED_INVALID prior_reason={reason} sha256={cand_sha}'
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_bytes(data)
    os.replace(tmp, path)
    return f'WROTE sha256={cand_sha}'


def main() -> int:
    ap = argparse.ArgumentParser(prog='bridge_accounting_nav_v2_to_compat_v1')
    ap.add_argument('--day_utc', required=True)
    ap.add_argument('--truth_root', required=True)
    args = ap.parse_args()
    day = str(args.day_utc).strip()
    truth_root = _require_truth_root(args.truth_root)
    src_path = (truth_root / 'accounting_v2' / 'nav' / day / 'nav.v2.json').resolve()
    out_path = (truth_root / 'accounting_compat_v1' / 'nav' / day / 'nav_snapshot.v1.json').resolve()
    if not src_path.exists() or not src_path.is_file():
        raise SystemExit(f'FAIL: SOURCE_NAV_V2_MISSING: {src_path}')
    src = _read_json_obj(src_path)
    out = _build_output(day, src_path, src)
    action = _write_with_integrity(out_path, out)
    print(f'OK: ACCOUNTING_NAV_COMPAT_BRIDGED day_utc={day} path={out_path} action={action}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
