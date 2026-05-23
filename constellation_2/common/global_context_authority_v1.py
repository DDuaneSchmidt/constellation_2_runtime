
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root, resolve_truth_sleeves_root
from constellation_2.common.sleeve_execution_root_v1 import resolve_sleeve_execution_root_v1
from constellation_2.common.trade_submit_readiness_authority_v1 import resolve_governed_account_binding
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_REGISTRY_RELPATH = 'governance/02_REGISTRIES/C2_GLOBAL_CONTEXT_MANIFESTS_V1.json'
MANIFEST_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/REPORTS/global_context_dependency_manifest.v1.schema.json'
BUILD_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/REPORTS/global_context_build.v1.schema.json'
PACKAGE_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/CONTEXT/global_context_package.v1.schema.json'
BUILD_REPORT_FAMILY = 'global_context_build_v1'
PACKAGE_FAMILY = 'global_context_package_v1'

STATUS_PRESENT = 'PRESENT'
STATUS_MISSING = 'MISSING'
STATUS_STALE = 'STALE'
STATUS_FAILED = 'FAILED'
STATUS_BLOCKED_BY_UPSTREAM = 'BLOCKED_BY_UPSTREAM'
STATUS_UNOWNED = 'UNOWNED'

CLOSURE_COMPLETE = 'COMPLETE'
CLOSURE_BLOCKED = 'BLOCKED'


@dataclass(frozen=True)
class GlobalContext:
    repo_root: Path
    canonical_truth_root: Path
    truth_sleeves_root: Path
    execution_truth_root: Path
    day_utc: str
    sleeve_id: str
    environment: str
    ib_account: str
    operation_type: str
    context_hash: str


def _anchor_utc(day_utc: str) -> str:
    return f'{day_utc}T00:00:00Z'


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(str(path))
    obj = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT:path={path}')
    return obj


def _write_canonical_json(path: Path, obj: Dict[str, Any]) -> str:
    payload = json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding='utf-8')
    return _sha256_bytes(payload.encode('utf-8'))


def compute_global_context_hash_v1(*, day_utc: str, sleeve_id: str, environment: str, ib_account: str, operation_type: str) -> str:
    return canonical_hash_for_c2_artifact_v1(
        {
            'account_id': str(ib_account).strip().upper(),
            'day_utc': str(day_utc).strip(),
            'environment': str(environment).strip().upper(),
            'operation_type': str(operation_type).strip(),
            'sleeve_id': str(sleeve_id).strip().upper(),
        }
    )


def resolve_global_context_v1(*, repo_root: Path, operation_type: str, day_utc: str, sleeve_id: str, environment: str, ib_account: str) -> GlobalContext:
    repo_root = repo_root.resolve()
    environment = str(environment).strip().upper()
    sleeve_id = str(sleeve_id).strip().upper()
    account_binding = resolve_governed_account_binding(
        repo_root=repo_root,
        environment=environment,
        requested_ib_account=str(ib_account).strip(),
        sleeve_id=sleeve_id,
    )
    execution_root = resolve_sleeve_execution_root_v1(
        repo_root=repo_root,
        environment=environment,
        ib_account=account_binding.ib_account,
        sleeve_id=sleeve_id,
    )
    return GlobalContext(
        repo_root=repo_root,
        canonical_truth_root=resolve_canonical_truth_root().resolve(),
        truth_sleeves_root=resolve_truth_sleeves_root().resolve(),
        execution_truth_root=execution_root.execution_root_path.resolve(),
        day_utc=str(day_utc).strip(),
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=account_binding.ib_account,
        operation_type=operation_type,
        context_hash=compute_global_context_hash_v1(
            day_utc=day_utc,
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=account_binding.ib_account,
            operation_type=operation_type,
        ),
    )


def global_context_build_path_v1(*, ctx: GlobalContext) -> Path:
    return (ctx.canonical_truth_root / 'reports' / BUILD_REPORT_FAMILY / ctx.day_utc / ctx.context_hash / 'global_context_build.v1.json').resolve()


def global_context_package_path_v1(*, ctx: GlobalContext) -> Path:
    return (ctx.execution_truth_root / PACKAGE_FAMILY / ctx.day_utc / ctx.context_hash / 'global_context_package.v1.json').resolve()


def _load_manifest(repo_root: Path, operation_type: str) -> Dict[str, Any]:
    registry_path = (repo_root / MANIFEST_REGISTRY_RELPATH).resolve()
    registry = _read_json(registry_path)
    manifests = registry.get('manifests')
    if not isinstance(manifests, dict):
        raise ValueError('GLOBAL_CONTEXT_MANIFEST_REGISTRY_INVALID')
    manifest = manifests.get(operation_type)
    if not isinstance(manifest, dict):
        raise ValueError(f'GLOBAL_CONTEXT_MANIFEST_NOT_FOUND:operation_type={operation_type}')
    out = {'schema_id': 'global_context_dependency_manifest', 'schema_version': 'v1', **manifest}
    validate_against_repo_schema_v1(out, repo_root, MANIFEST_SCHEMA_RELPATH)
    return out


def _dependency_path(ctx: GlobalContext, dependency: Dict[str, Any]) -> Path:
    pattern = str(dependency.get('path_pattern') or '').strip()
    return Path(pattern.format(
        canonical_truth_root=str(ctx.canonical_truth_root),
        execution_truth_root=str(ctx.execution_truth_root),
        day_utc=ctx.day_utc,
        sleeve_id=ctx.sleeve_id,
        environment=ctx.environment,
        account_id=ctx.ib_account,
        ib_account=ctx.ib_account,
        context_hash=ctx.context_hash,
        operation_type=ctx.operation_type,
    )).resolve()


def _status_value(obj: Dict[str, Any]) -> str:
    for field in ('status', 'state', 'overall_status', 'decision'):
        value = str(obj.get(field) or '').strip().upper()
        if value:
            return value
    return ''


def _day_value(obj: Dict[str, Any]) -> str:
    for field in ('day_utc', 'session_date'):
        value = str(obj.get(field) or '').strip()
        if value:
            return value
    return ''


def _evaluate_semantics(*, dependency_id: str, obj: Dict[str, Any], path: Path, ctx: GlobalContext) -> tuple[str, str]:
    day_value = _day_value(obj)
    if day_value and day_value != ctx.day_utc:
        return STATUS_STALE, f'DAY_MISMATCH:expected={ctx.day_utc}:actual={day_value}:path={path}'

    if dependency_id == 'day_activation_package_v1':
        sealed = bool(obj.get('sealed') is True)
        mode = str(obj.get('mode') or '').strip().upper()
        sleeve_id = str(obj.get('sleeve_id') or '').strip().upper()
        account_id = str(obj.get('account_id') or '').strip().upper()
        op_type = str(obj.get('operation_type') or '').strip()
        context_hash = str(obj.get('context_hash') or '').strip()
        if not sealed:
            return STATUS_FAILED, f'DAY_ACTIVATION_PACKAGE_UNSEALED:path={path}'
        if mode != ctx.environment or sleeve_id != ctx.sleeve_id or account_id != ctx.ib_account.upper() or op_type != ctx.operation_type or context_hash != ctx.context_hash:
            return STATUS_STALE, f'DAY_ACTIVATION_PACKAGE_CONTEXT_MISMATCH:path={path}'
        return STATUS_PRESENT, 'DAY_ACTIVATION_PACKAGE_SEALED'

    status = _status_value(obj)
    if status and status not in {'PASS', 'OK', 'ACTIVE', 'SUCCESS', 'BOOTSTRAP_PASS'}:
        return STATUS_FAILED, f'STATUS_NOT_PASS:status={status}:path={path}'
    return STATUS_PRESENT, 'ARTIFACT_PRESENT'


def _evaluate_dependencies(ctx: GlobalContext, manifest: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    results: Dict[str, Dict[str, Any]] = {}
    for dependency in manifest.get('dependencies', []):
        dep_id = str(dependency.get('dependency_id') or '').strip()
        upstream_ids = [str(x).strip() for x in dependency.get('upstream_dependency_ids') or [] if str(x).strip()]
        path = _dependency_path(ctx, dependency)
        upstream_blockers = [uid for uid in upstream_ids if results.get(uid, {}).get('status') != STATUS_PRESENT]
        owner_ref = dependency.get('owner_ref')
        producer_ref = dependency.get('producer_ref')

        if not owner_ref:
            status = STATUS_UNOWNED
            detail = 'UNOWNED_DEPENDENCY'
            sha256 = '0' * 64
        else:
            if path.exists() and path.is_file():
                sha256 = _sha256_file(path)
                try:
                    obj = _read_json(path)
                    status, detail = _evaluate_semantics(dependency_id=dep_id, obj=obj, path=path, ctx=ctx)
                except Exception as exc:
                    status = STATUS_FAILED
                    detail = f'PARSE_OR_VALIDATION_FAILED:{type(exc).__name__}:{exc}'
            else:
                sha256 = '0' * 64
                status = STATUS_MISSING
                detail = f'INPUT_FILE_MISSING:{path}'
                if dep_id == 'day_activation_package_v1':
                    build_path = _blocking_build_path(ctx, dependency)
                    if build_path is not None and build_path.exists() and build_path.is_file():
                        build_obj = _read_json(build_path)
                        build_closure = str(build_obj.get('closure_status') or '').strip().upper()
                        first_blocker = build_obj.get('first_real_blocker') if isinstance(build_obj.get('first_real_blocker'), dict) else {}
                        first_dep = str(first_blocker.get('dependency_id') or '').strip() or 'UNKNOWN'
                        if build_closure != 'COMPLETE':
                            status = STATUS_FAILED
                            detail = f'DAY_ACTIVATION_BLOCKED:first_real_blocker={first_dep}:path={first_blocker.get("path")}:build_ref={build_path}'
                        else:
                            status = STATUS_STALE
                            detail = f'DAY_ACTIVATION_PACKAGE_MISSING_AFTER_COMPLETE_BUILD:build_ref={build_path}'
            if upstream_blockers and status != STATUS_UNOWNED:
                status = STATUS_BLOCKED_BY_UPSTREAM
                detail = f'BLOCKED_BY_UPSTREAM:{upstream_blockers}'

        results[dep_id] = {
            'dependency_id': dep_id,
            'stage_id': dependency.get('stage_id'),
            'role_class': dependency.get('role_class'),
            'owner_ref': owner_ref,
            'producer_ref': producer_ref,
            'path': str(path),
            'required': bool(dependency.get('required') is True),
            'advisory_only': bool(dependency.get('advisory_only') is True),
            'post_submit_only': bool(dependency.get('post_submit_only') is True),
            'reusable': bool(dependency.get('reusable') is True),
            'fresh_materialization_required': bool(dependency.get('fresh_materialization_required') is True),
            'upstream_dependency_ids': upstream_ids,
            'upstream_blockers': upstream_blockers,
            'status': status,
            'detail': detail,
            'sha256': sha256,
        }
    return results


def _blocking_build_path(ctx: GlobalContext, dependency: Dict[str, Any]) -> Optional[Path]:
    pattern = str(dependency.get('blocking_build_path_pattern') or '').strip()
    if not pattern:
        return None
    values = {
        'canonical_truth_root': str(ctx.canonical_truth_root),
        'execution_truth_root': str(ctx.execution_truth_root),
        'day_utc': ctx.day_utc,
        'sleeve_id': ctx.sleeve_id,
        'environment': ctx.environment,
        'account_id': ctx.ib_account,
        'ib_account': ctx.ib_account,
        'context_hash': ctx.context_hash,
        'operation_type': ctx.operation_type,
    }
    return Path(pattern.format(**values)).resolve()


def _stage_dependencies(manifest: Dict[str, Any], stage_id: str) -> List[Dict[str, Any]]:
    return [dep for dep in manifest.get('dependencies', []) if str(dep.get('stage_id') or '') == stage_id]


def _producer_command(*, producer_ref: str, ctx: GlobalContext) -> List[str]:
    if producer_ref == 'day_activation_authority_v1':
        return [
            sys.executable,
            str((ctx.repo_root / 'ops/tools/run_day_activation_authority_v1.py').resolve()),
            '--operation_type',
            ctx.operation_type,
            '--day_utc',
            ctx.day_utc,
            '--sleeve_id',
            ctx.sleeve_id,
            '--environment',
            ctx.environment,
            '--ib_account',
            ctx.ib_account,
            '--materialize',
            'YES',
            '--emit_package',
            'YES',
        ]
    raise ValueError(f'GLOBAL_CONTEXT_UNKNOWN_PRODUCER:{producer_ref}')


def _run_materializers(ctx: GlobalContext, manifest: Dict[str, Any], results: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    materialized: List[Dict[str, Any]] = []
    executed_producers: set[str] = set()
    current_results = results
    progress = True
    while progress:
        progress = False
        for stage_id in manifest.get('stage_order', []):
            producer_refs: List[str] = []
            for dependency in _stage_dependencies(manifest, stage_id):
                dep_id = str(dependency.get('dependency_id') or '').strip()
                result = current_results.get(dep_id, {})
                producer_ref = str(result.get('producer_ref') or '').strip()
                if not producer_ref or producer_ref in executed_producers:
                    continue
                if result.get('status') in {STATUS_MISSING, STATUS_STALE, STATUS_FAILED} and not result.get('upstream_blockers'):
                    producer_refs.append(producer_ref)
            for producer_ref in producer_refs:
                cmd = _producer_command(producer_ref=producer_ref, ctx=ctx)
                proc = subprocess.run(cmd, cwd=str(ctx.repo_root), capture_output=True, text=True)
                executed_producers.add(producer_ref)
                materialized.append(
                    {
                        'stage_id': stage_id,
                        'producer_ref': producer_ref,
                        'command': cmd,
                        'returncode': int(proc.returncode),
                        'stdout': proc.stdout[-4000:],
                        'stderr': proc.stderr[-4000:],
                    }
                )
                current_results = _evaluate_dependencies(ctx, manifest)
                progress = True
    return materialized


def _materializable_now(results: Dict[str, Dict[str, Any]], manifest: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for dependency in manifest.get('dependencies', []):
        dep_id = str(dependency.get('dependency_id') or '').strip()
        result = results.get(dep_id, {})
        if result.get('status') in {STATUS_MISSING, STATUS_STALE, STATUS_FAILED} and result.get('producer_ref') and not result.get('upstream_blockers'):
            out.append(dep_id)
    return out


def _collapse_first_real_blocker(results: Dict[str, Dict[str, Any]], dependency_id: str) -> Dict[str, Any]:
    row = results[dependency_id]
    if row['status'] == STATUS_PRESENT:
        return row
    for upstream_id in row.get('upstream_dependency_ids') or []:
        upstream = results.get(upstream_id)
        if upstream and upstream.get('status') != STATUS_PRESENT:
            return _collapse_first_real_blocker(results, upstream_id)
    return row


def _blocking_chain(results: Dict[str, Dict[str, Any]], blocker_id: Optional[str]) -> List[Dict[str, Any]]:
    if not blocker_id:
        return []
    ordered: List[Dict[str, Any]] = []
    seen: set[str] = set()
    def visit(dep_id: str) -> None:
        if dep_id in seen:
            return
        seen.add(dep_id)
        row = results.get(dep_id)
        if not row:
            return
        ordered.append({'dependency_id': dep_id, 'status': row.get('status'), 'detail': row.get('detail'), 'path': row.get('path')})
        for upstream_id in row.get('upstream_dependency_ids') or []:
            if results.get(upstream_id, {}).get('status') != STATUS_PRESENT:
                visit(upstream_id)
    visit(blocker_id)
    return ordered


def _dependency_results_list(results: Dict[str, Dict[str, Any]], manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [results[str(dep.get('dependency_id') or '').strip()] for dep in manifest.get('dependencies', [])]


def _sealable(results: Dict[str, Dict[str, Any]], manifest: Dict[str, Any]) -> bool:
    required = {str(x).strip() for x in manifest.get('seal_requires') or [] if str(x).strip()}
    return all(results.get(dep_id, {}).get('status') == STATUS_PRESENT for dep_id in required)


def run_global_context_authority_v1(*, repo_root: Path, operation_type: str, day_utc: str, sleeve_id: str, environment: str, ib_account: str, materialize: bool = True, emit_package: bool = True) -> Dict[str, Any]:
    ctx = resolve_global_context_v1(repo_root=repo_root, operation_type=operation_type, day_utc=day_utc, sleeve_id=sleeve_id, environment=environment, ib_account=ib_account)
    manifest = _load_manifest(ctx.repo_root, operation_type)
    results = _evaluate_dependencies(ctx, manifest)
    materialized_nodes: List[Dict[str, Any]] = []
    if materialize:
        materialized_nodes = _run_materializers(ctx, manifest, results)
        results = _evaluate_dependencies(ctx, manifest)
    first_blocker = None
    first_blocker_id = None
    for dependency in manifest.get('dependencies', []):
        dep_id = str(dependency.get('dependency_id') or '').strip()
        row = results[dep_id]
        if row['required'] and not row['advisory_only'] and not row['post_submit_only'] and row['status'] != STATUS_PRESENT:
            first_blocker = _collapse_first_real_blocker(results, dep_id)
            first_blocker_id = str(first_blocker.get('dependency_id') or dep_id)
            break
    closure_status = CLOSURE_COMPLETE if _sealable(results, manifest) else CLOSURE_BLOCKED
    build_obj: Dict[str, Any] = {
        'schema_id': 'global_context_build',
        'schema_version': 'v1',
        'day_utc': ctx.day_utc,
        'sleeve_id': ctx.sleeve_id,
        'mode': ctx.environment,
        'account_id': ctx.ib_account,
        'operation_type': ctx.operation_type,
        'context_hash': ctx.context_hash,
        'dependency_manifest_ref': str((ctx.repo_root / MANIFEST_REGISTRY_RELPATH).resolve()),
        'dependency_results': _dependency_results_list(results, manifest),
        'closure_status': closure_status,
        'first_real_blocker': first_blocker,
        'blocking_chain': _blocking_chain(results, first_blocker_id),
        'materialized_nodes': materialized_nodes,
        'materializable_now': _materializable_now(results, manifest),
        'unowned_dependencies': [dep_id for dep_id, row in results.items() if row.get('status') == STATUS_UNOWNED],
        'generated_utc': _anchor_utc(ctx.day_utc),
        'canonical_json_hash': None,
    }
    build_obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1(build_obj)
    validate_against_repo_schema_v1(build_obj, ctx.repo_root, BUILD_SCHEMA_RELPATH)
    build_path = global_context_build_path_v1(ctx=ctx)
    build_sha = _write_canonical_json(build_path, build_obj)
    package_path = None
    package_obj = None
    if emit_package and closure_status == CLOSURE_COMPLETE:
        refs = []
        ref_map: Dict[str, Dict[str, Any]] = {}
        for dependency in manifest.get('dependencies', []):
            dep_id = str(dependency.get('dependency_id') or '').strip()
            row = results[dep_id]
            if row['required'] and not row['advisory_only'] and not row['post_submit_only']:
                entry = {'dependency_id': dep_id, 'path': row['path'], 'sha256': row['sha256'], 'owner_ref': row['owner_ref'], 'role_class': row['role_class'], 'status': row['status']}
                refs.append(entry)
                ref_map[dep_id] = entry
        activation_payload = {}
        activation_ref = ref_map.get('day_activation_package_v1')
        if isinstance(activation_ref, dict) and str(activation_ref.get('path') or '').strip():
            try:
                activation_payload = _read_json(Path(str(activation_ref.get('path'))))
            except Exception:
                activation_payload = {}
        package_obj = {
            'schema_id': 'global_context_package',
            'schema_version': 'v1',
            'day_utc': ctx.day_utc,
            'sleeve_id': ctx.sleeve_id,
            'mode': ctx.environment,
            'account_id': ctx.ib_account,
            'operation_type': ctx.operation_type,
            'context_hash': ctx.context_hash,
            'package_hash': None,
            'day_activation_package_ref': ref_map.get('day_activation_package_v1'),
            'runtime_evaluation_hash': str(activation_payload.get('runtime_evaluation_hash') or ''),
            'market_session_status': str(activation_payload.get('market_session_status') or ''),
            'calendar_session_classification': str(activation_payload.get('calendar_session_classification') or ''),
            'enabled_sleeves': activation_payload.get('enabled_sleeves') if isinstance(activation_payload.get('enabled_sleeves'), list) else [ctx.sleeve_id],
            'disabled_sleeves': activation_payload.get('disabled_sleeves') if isinstance(activation_payload.get('disabled_sleeves'), list) else [],
            'policy_version': str(activation_payload.get('policy_version') or 'global_context_authority.v1'),
            'data_readiness_hash': str(activation_payload.get('data_readiness_hash') or ''),
            'sleeve_readiness_hash': str(activation_payload.get('sleeve_readiness_hash') or ''),
            'manual_intent_hash': str(activation_payload.get('manual_intent_hash') or ''),
            'target_day_admission_ref': activation_payload.get('target_day_admission_ref') if isinstance(activation_payload.get('target_day_admission_ref'), dict) else {},
            'validation_status': 'VALID',
            'build_ref': {'path': str(build_path), 'sha256': build_sha},
            'manifest_ref': str((ctx.repo_root / MANIFEST_REGISTRY_RELPATH).resolve()),
            'dependency_refs': refs,
            'seal_basis': 'global context closure achieved',
            'sealed': True,
            'sealed_utc': _anchor_utc(ctx.day_utc),
            'broker_execution_allowed': False,
            'broker_submit_transmit_allowed': False,
            'order_routing_allowed': False,
            'autonomous_execution_allowed': False,
            'trade_advice_allowed': False,
        }
        package_obj['package_hash'] = canonical_hash_for_c2_artifact_v1(package_obj)
        validate_against_repo_schema_v1(package_obj, ctx.repo_root, PACKAGE_SCHEMA_RELPATH)
        package_path = global_context_package_path_v1(ctx=ctx)
        _write_canonical_json(package_path, package_obj)
    return {'context': ctx, 'manifest': manifest, 'results': results, 'build_path': build_path, 'build_obj': build_obj, 'package_path': package_path, 'package_obj': package_obj}
