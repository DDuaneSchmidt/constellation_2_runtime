from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

try:
    from constellation_2.common.runtime_base_v1 import ensure_repo_root_on_sys_path
except ModuleNotFoundError:  # pragma: no cover - direct script execution bootstrap
    import importlib.util

    _RUNTIME_BASE_PATH = Path(__file__).resolve().parents[2] / 'constellation_2' / 'common' / 'runtime_base_v1.py'
    _RUNTIME_BASE_SPEC = importlib.util.spec_from_file_location('constellation_2.common.runtime_base_v1', _RUNTIME_BASE_PATH)
    if _RUNTIME_BASE_SPEC is None or _RUNTIME_BASE_SPEC.loader is None:
        raise RuntimeError(f'RUNTIME_BASE_IMPORT_FAILED: {_RUNTIME_BASE_PATH}')
    _runtime_base_v1 = importlib.util.module_from_spec(_RUNTIME_BASE_SPEC)
    _RUNTIME_BASE_SPEC.loader.exec_module(_runtime_base_v1)
    ensure_repo_root_on_sys_path = _runtime_base_v1.ensure_repo_root_on_sys_path

REPO_ROOT = ensure_repo_root_on_sys_path(__file__)

from constellation_2.common.runtime_guardrails_v1 import classify_failure, format_failure_line
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

SCHEMA = 'governance/04_DATA/SCHEMAS/C2/REPORTS/replay_manifest.v1.schema.json'


def _require_truth_root(raw: str, *, label: str) -> Path:
    p = Path(str(raw).strip()).expanduser().resolve()
    if not p.is_absolute() or not p.exists() or not p.is_dir():
        raise SystemExit(f'FAIL: invalid --{label}: {p}')
    return p


def _read_json(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(obj, dict):
        raise SystemExit(f'FAIL: TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.write_text(json.dumps(obj, sort_keys=True, separators=(',', ':')) + '\n', encoding='utf-8')


def _sha_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _sha_tree(root: Path) -> str:
    if not root.exists():
        return hashlib.sha256(b'').hexdigest()
    h = hashlib.sha256()
    for p in sorted(root.rglob('*')):
        if p.is_file():
            h.update(str(p.relative_to(root)).replace('\\', '/').encode('utf-8'))
            h.update(b'\n')
            h.update(_sha_file(p).encode('utf-8'))
            h.update(b'\n')
    return h.hexdigest()


def _rewrite_replay_references(*, source_truth_root: Path, replay_truth_root: Path) -> None:
    readiness_path = replay_truth_root / 'trade_submit_readiness_c2_v1' / 'status.json'
    if readiness_path.exists():
        readiness_obj = _read_json(readiness_path)
        provenance = readiness_obj.get('provenance')
        if isinstance(provenance, dict):
            provenance['truth_root'] = str(replay_truth_root)
            _write_json(readiness_path, readiness_obj)

    head_path = replay_truth_root / 'run_pointer_v2' / 'canonical_authority_head.v1.json'
    if head_path.exists():
        head_obj = _read_json(head_path)
        points_to = str(head_obj.get('points_to') or '').strip()
        source_root_str = str(source_truth_root)
        if points_to.startswith(source_root_str):
            rel_tail = Path(points_to).resolve().relative_to(source_truth_root)
            head_obj['points_to'] = str((replay_truth_root / rel_tail).resolve())
            _write_json(head_path, head_obj)


def _compare_path(*, artifact_role: str, source_path: Path, replay_path: Path) -> Dict[str, Any]:
    source_exists = source_path.exists()
    replay_exists = replay_path.exists()
    if not source_exists:
        return {
            'artifact_role': artifact_role,
            'source_path': str(source_path),
            'replay_path': str(replay_path),
            'source_sha256': None,
            'replay_sha256': None,
            'comparison_status': 'missing_source',
        }
    if not replay_exists:
        return {
            'artifact_role': artifact_role,
            'source_path': str(source_path),
            'replay_path': str(replay_path),
            'source_sha256': _sha_tree(source_path) if source_path.is_dir() else _sha_file(source_path),
            'replay_sha256': None,
            'comparison_status': 'missing_replay',
        }
    source_sha = _sha_tree(source_path) if source_path.is_dir() else _sha_file(source_path)
    replay_sha = _sha_tree(replay_path) if replay_path.is_dir() else _sha_file(replay_path)
    return {
        'artifact_role': artifact_role,
        'source_path': str(source_path),
        'replay_path': str(replay_path),
        'source_sha256': source_sha,
        'replay_sha256': replay_sha,
        'comparison_status': 'identical' if source_sha == replay_sha else 'different',
    }


def _offline_tool_runs(*, source_truth_root: Path, replay_truth_root: Path) -> List[Dict[str, Any]]:
    return [
        {'tool': 'offline_copytree', 'rc': 0, 'stdout': f'{source_truth_root} -> {replay_truth_root}', 'stderr': ''},
        {'tool': 'offline_pointer_rewrite', 'rc': 0, 'stdout': '', 'stderr': ''},
    ]


def _reason_codes(comparisons: List[Dict[str, Any]]) -> List[str]:
    codes: List[str] = []
    for item in comparisons:
        status = str(item['comparison_status'])
        if status != 'identical':
            codes.append(f'REPLAY_COMPARE_{status.upper()}:{item["artifact_role"]}')
    return codes


def _matching_stream_records(stream_dir: Path, submission_id: str) -> List[Path]:
    if not stream_dir.exists() or not stream_dir.is_dir():
        return []
    matches: List[Path] = []
    for path in sorted(stream_dir.glob('*.execution_event_stream_record.v1.json')):
        obj = _read_json(path)
        if str(obj.get('submission_id') or '').strip() == submission_id:
            matches.append(path)
    return matches


def _completeness_reason_codes(*, truth_root: Path, day: str, submission_id: str, prefix: str) -> List[str]:
    codes: List[str] = []
    authorization_dir = truth_root / 'engine_activity_v1' / 'authorization_v1' / day
    if authorization_dir.exists() and authorization_dir.is_dir():
        if not list(authorization_dir.glob('*.authorization.v1.json')):
            codes.append(f'REPLAY_{prefix}_INCOMPLETE:authorization_dir:no_authorization_artifacts')

    submission_dir = truth_root / 'execution_evidence_v1' / 'submissions' / day / submission_id
    if submission_dir.exists() and submission_dir.is_dir():
        broker_submission = submission_dir / 'broker_submission_record.v2.json'
        if not broker_submission.exists():
            codes.append(f'REPLAY_{prefix}_INCOMPLETE:submission_dir:missing_broker_submission_record')

    execution_stream_dir = truth_root / 'execution_stream_v1' / day
    if execution_stream_dir.exists() and execution_stream_dir.is_dir():
        if not _matching_stream_records(execution_stream_dir, submission_id):
            codes.append(f'REPLAY_{prefix}_INCOMPLETE:execution_stream_dir:no_matching_submission_records')

    return codes


def main() -> int:
    ap = argparse.ArgumentParser(prog='run_runtime_replay_day_v1')
    ap.add_argument('--day_utc', required=True)
    ap.add_argument('--source_truth_root', required=True)
    ap.add_argument('--replay_truth_root', required=True)
    ap.add_argument('--submission_id', required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    source_truth_root = _require_truth_root(args.source_truth_root, label='source_truth_root')
    replay_truth_root = Path(str(args.replay_truth_root).strip()).expanduser().resolve()
    submission_id = str(args.submission_id).strip()

    if replay_truth_root.exists():
        if any(replay_truth_root.iterdir()):
            raise SystemExit(f'FAIL: REPLAY_TRUTH_ROOT_NOT_EMPTY: {replay_truth_root}')
        replay_truth_root.rmdir()

    shutil.copytree(source_truth_root, replay_truth_root)
    _rewrite_replay_references(source_truth_root=source_truth_root, replay_truth_root=replay_truth_root)

    source_subdir = source_truth_root / 'execution_evidence_v1' / 'submissions' / day / submission_id
    replay_subdir = replay_truth_root / 'execution_evidence_v1' / 'submissions' / day / submission_id
    source_completeness = _completeness_reason_codes(truth_root=source_truth_root, day=day, submission_id=submission_id, prefix='SOURCE')
    replay_completeness = _completeness_reason_codes(truth_root=replay_truth_root, day=day, submission_id=submission_id, prefix='REPLAY')

    comparisons = [
        _compare_path(
            artifact_role='allocation',
            source_path=source_truth_root / 'allocation_v1' / 'capital_authority_allocation_v1' / day / 'capital_authority_allocation.v1.json',
            replay_path=replay_truth_root / 'allocation_v1' / 'capital_authority_allocation_v1' / day / 'capital_authority_allocation.v1.json',
        ),
        _compare_path(
            artifact_role='authorization_dir',
            source_path=source_truth_root / 'engine_activity_v1' / 'authorization_v1' / day,
            replay_path=replay_truth_root / 'engine_activity_v1' / 'authorization_v1' / day,
        ),
        _compare_path(
            artifact_role='submission_dir',
            source_path=source_subdir,
            replay_path=replay_subdir,
        ),
        _compare_path(
            artifact_role='execution_stream_dir',
            source_path=source_truth_root / 'execution_stream_v1' / day,
            replay_path=replay_truth_root / 'execution_stream_v1' / day,
        ),
        _compare_path(
            artifact_role='fill_ledger',
            source_path=source_truth_root / 'fill_ledger_v1' / day / f'{submission_id}.fill_ledger.v1.json',
            replay_path=replay_truth_root / 'fill_ledger_v1' / day / f'{submission_id}.fill_ledger.v1.json',
        ),
    ]
    reason_codes = sorted(set(source_completeness + replay_completeness + _reason_codes(comparisons)))

    manifest = {
        'schema_id': 'C2_REPLAY_MANIFEST_V1',
        'schema_version': 1,
        'produced_utc': f'{day}T00:00:00Z',
        'day_utc': day,
        'source_truth_root': str(source_truth_root),
        'replay_truth_root': str(replay_truth_root),
        'submission_id': submission_id,
        'status': 'OK' if not reason_codes else 'FAIL',
        'reason_codes': reason_codes,
        'tool_runs': _offline_tool_runs(source_truth_root=source_truth_root, replay_truth_root=replay_truth_root),
        'comparisons': comparisons,
        'canonical_json_hash': '',
    }
    manifest['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1(manifest)
    validate_against_repo_schema_v1(manifest, REPO_ROOT, SCHEMA)
    out_path = (replay_truth_root / 'reports' / 'replay_manifest_v1' / day / f'{submission_id}.replay_manifest.v1.json').resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(canonical_json_bytes_v1(manifest) + b'\n')
    if reason_codes:
        print(
            format_failure_line(
                'run_runtime_replay_day_v1',
                classify_failure('|'.join(reason_codes)),
                manifest=str(out_path),
                reason_codes=reason_codes,
            ),
            file=sys.stderr,
        )
        return 2
    print(f'OK: REPLAY_MANIFEST_WRITTEN path={out_path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
