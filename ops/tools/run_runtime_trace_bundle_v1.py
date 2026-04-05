from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

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

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

SCHEMA = 'governance/04_DATA/SCHEMAS/C2/REPORTS/runtime_trace_bundle.v1.schema.json'


def _require_truth_root(raw: str) -> Path:
    p = Path(str(raw).strip()).expanduser().resolve()
    if not p.is_absolute() or not p.exists() or not p.is_dir():
        raise SystemExit(f'FAIL: invalid --truth_root: {p}')
    return p


def _read_json(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(obj, dict):
        raise SystemExit(f'FAIL: TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _append_ref(rows: List[Dict[str, Any]], *, stage: str, artifact_type: str, path: Path, producer: str | None = None) -> None:
    rows.append({
        'stage': stage,
        'artifact_type': artifact_type,
        'path': str(path),
        'sha256': _sha(path),
        'producer': producer,
    })


def _is_sha256_hex(value: str) -> bool:
    return len(value) == 64 and all(ch in '0123456789abcdef' for ch in value)


def _validate_artifact_rows(artifacts: List[Dict[str, Any]]) -> None:
    if not artifacts:
        raise SystemExit('FAIL: EMPTY_TRACE_BUNDLE')
    for item in artifacts:
        path = str(item.get('path') or '').strip()
        sha256 = str(item.get('sha256') or '').strip()
        if not path:
            raise SystemExit('FAIL: TRACE_ARTIFACT_PATH_MISSING')
        if not _is_sha256_hex(sha256):
            raise SystemExit(f'FAIL: TRACE_ARTIFACT_SHA256_INVALID: {path}')


def _validate_stage_coverage(artifacts: List[Dict[str, Any]], *, submission_dir: Path) -> None:
    stages = {str(item['stage']): [] for item in artifacts}
    for item in artifacts:
        stages.setdefault(str(item['stage']), []).append(item)
    for required_stage in ('allocation', 'authorization', 'submission', 'execution_stream', 'fill_ledger'):
        if not stages.get(required_stage):
            raise SystemExit(f'FAIL: TRACE_STAGE_MISSING:{required_stage}')
    submission_names = {Path(str(item['path'])).name for item in stages['submission']}
    if 'broker_submission_record.v2.json' not in submission_names:
        raise SystemExit('FAIL: TRACE_SUBMISSION_LINEAGE_MISSING:broker_submission_record.v2.json')
    if 'equity_order_plan.v1.json' not in submission_names and 'order_plan.v1.json' not in submission_names:
        raise SystemExit('FAIL: TRACE_SUBMISSION_LINEAGE_MISSING:supported_plan')


def main() -> int:
    ap = argparse.ArgumentParser(prog='run_runtime_trace_bundle_v1')
    ap.add_argument('--day_utc', required=True)
    ap.add_argument('--truth_root', required=True)
    ap.add_argument('--submission_id', required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    truth_root = _require_truth_root(args.truth_root)
    submission_id = str(args.submission_id).strip()
    produced_utc = f'{day}T00:00:00Z'

    subdir = (truth_root / 'execution_evidence_v1' / 'submissions' / day / submission_id).resolve()
    if not subdir.exists() or not subdir.is_dir():
        raise SystemExit(f'FAIL: MISSING_SUBMISSION_DIR: {subdir}')

    artifacts: List[Dict[str, Any]] = []

    allocation_p = (truth_root / 'allocation_v1' / 'capital_authority_allocation_v1' / day / 'capital_authority_allocation.v1.json').resolve()
    if not allocation_p.exists():
        raise SystemExit(f'FAIL: MISSING_ALLOCATION: {allocation_p}')
    alloc = _read_json(allocation_p)
    _append_ref(artifacts, stage='allocation', artifact_type='capital_authority_allocation', path=allocation_p, producer=str((alloc.get('producer') or {}).get('module') or 'allocation_v1'))
    for item in list(alloc.get('input_manifest') or []):
        if isinstance(item, dict) and str(item.get('path') or '').strip() and str(item.get('sha256') or '').strip():
            artifacts.append({
                'stage': 'allocation',
                'artifact_type': str(item.get('type') or 'input_manifest'),
                'path': str(item.get('path')),
                'sha256': str(item.get('sha256')),
                'producer': str(item.get('producer')) if item.get('producer') is not None else None,
            })

    broker_submission_p = (subdir / 'broker_submission_record.v2.json').resolve()
    if not broker_submission_p.exists():
        raise SystemExit(f'FAIL: MISSING_BROKER_SUBMISSION_RECORD: {broker_submission_p}')

    plan_p = (subdir / 'equity_order_plan.v1.json').resolve()
    if not plan_p.exists():
        plan_p = (subdir / 'order_plan.v1.json').resolve()
    if not plan_p.exists():
        raise SystemExit(f'FAIL: MISSING_SUPPORTED_PLAN: {subdir}')
    plan = _read_json(plan_p)
    intent_sha = str(plan.get('intent_sha256') or '').strip()
    if not intent_sha:
        raise SystemExit(f'FAIL: MISSING_INTENT_SHA256: {plan_p}')

    auth_p = (truth_root / 'engine_activity_v1' / 'authorization_v1' / day / f'{intent_sha}.authorization.v1.json').resolve()
    if not auth_p.exists():
        raise SystemExit(f'FAIL: MISSING_AUTHORIZATION: {auth_p}')
    auth = _read_json(auth_p)
    _append_ref(artifacts, stage='authorization', artifact_type='authorization_artifact', path=auth_p, producer=str((auth.get('producer') or {}).get('module') or 'authorization_v1'))
    for item in list(auth.get('input_manifest') or []):
        if isinstance(item, dict) and str(item.get('path') or '').strip() and str(item.get('sha256') or '').strip():
            artifacts.append({
                'stage': 'authorization',
                'artifact_type': str(item.get('type') or 'input_manifest'),
                'path': str(item.get('path')),
                'sha256': str(item.get('sha256')),
                'producer': str(item.get('producer')) if item.get('producer') is not None else None,
            })

    stream_matches = 0

    for p in sorted(subdir.iterdir()):
        if p.is_file() and p.suffix == '.json':
            _append_ref(artifacts, stage='submission', artifact_type=p.name, path=p, producer='submission_bundle')

    stream_dir = (truth_root / 'execution_stream_v1' / day).resolve()
    for p in sorted(stream_dir.glob('*.execution_event_stream_record.v1.json')):
        obj = _read_json(p)
        if str(obj.get('submission_id') or '').strip() == submission_id:
            stream_matches += 1
            _append_ref(artifacts, stage='execution_stream', artifact_type='execution_event_stream_record', path=p, producer=str((obj.get('producer') or {}).get('module') or 'execution_stream_v1'))

    if stream_matches == 0:
        raise SystemExit(f'FAIL: MISSING_EXECUTION_STREAM_FOR_SUBMISSION: {submission_id}')

    ledger_p = (truth_root / 'fill_ledger_v1' / day / f'{submission_id}.fill_ledger.v1.json').resolve()
    if not ledger_p.exists():
        raise SystemExit(f'FAIL: MISSING_FILL_LEDGER: {ledger_p}')
    ledger = _read_json(ledger_p)
    _append_ref(artifacts, stage='fill_ledger', artifact_type='fill_ledger', path=ledger_p, producer=str((ledger.get('producer') or {}).get('module') or 'fill_ledger_v1'))

    artifacts = sorted(artifacts, key=lambda item: (item['stage'], item['artifact_type'], item['path']))
    _validate_artifact_rows(artifacts)
    _validate_stage_coverage(artifacts, submission_dir=subdir)
    bundle = {
        'schema_id': 'C2_RUNTIME_TRACE_BUNDLE_V1',
        'schema_version': 1,
        'produced_utc': produced_utc,
        'day_utc': day,
        'source_truth_root': str(truth_root),
        'submission_id': submission_id,
        'artifacts': artifacts,
        'canonical_json_hash': '',
    }
    bundle['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1(bundle)
    validate_against_repo_schema_v1(bundle, REPO_ROOT, SCHEMA)
    out_path = (truth_root / 'reports' / 'runtime_trace_bundle_v1' / day / f'{submission_id}.runtime_trace_bundle.v1.json').resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(canonical_json_bytes_v1(bundle) + b'\n')
    print(f'OK: RUNTIME_TRACE_BUNDLE_WRITTEN path={out_path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
