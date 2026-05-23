
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from constellation_2.common.global_context_authority_v1 import (
    compute_global_context_hash_v1,
    global_context_build_path_v1,
    global_context_package_path_v1,
    resolve_global_context_v1,
)
from constellation_2.common.constitutional_runtime_v1 import (
    FINALITY_FINALIZED,
    FINALITY_PROVISIONAL,
    assert_constitutional_writer_allowed_v1,
    build_artifact_dependency_declaration_v1,
    build_governed_artifact_lineage_v1,
    get_constitutional_artifact_contract_v1,
)
from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root, resolve_truth_sleeves_root
from constellation_2.common.sleeve_execution_root_v1 import resolve_sleeve_execution_root_v1
from constellation_2.common.trade_submit_readiness_authority_v1 import resolve_governed_account_binding
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_REGISTRY_RELPATH = 'governance/02_REGISTRIES/C2_ECONOMIC_STATE_MANIFESTS_V1.json'
MANIFEST_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/REPORTS/economic_dependency_manifest.v1.schema.json'
BUILD_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/REPORTS/economic_state_build.v1.schema.json'
PACKAGE_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/ECONOMIC/economic_state_package.v1.schema.json'
BUILD_REPORT_FAMILY = 'economic_state_build_v1'
PACKAGE_FAMILY = 'economic_state_package_v1'

STATUS_PRESENT = 'PRESENT'
STATUS_MISSING = 'MISSING'
STATUS_STALE = 'STALE'
STATUS_FAILED = 'FAILED'
STATUS_BLOCKED_BY_UPSTREAM = 'BLOCKED_BY_UPSTREAM'
STATUS_UNOWNED = 'UNOWNED'

CLOSURE_COMPLETE = 'COMPLETE'
CLOSURE_BLOCKED = 'BLOCKED'

RET_Q = Decimal('0.00000000')
DD_Q = Decimal('0.000001')
REALLOCATION_RETURN_DEADBAND = Decimal('0.002500')


@dataclass(frozen=True)
class EconomicContext:
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
    global_context_hash: str
    global_context_build_path: Path
    global_context_package_path: Path


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


def _git_sha(repo_root: Path) -> str:
    try:
        out = subprocess.check_output(['/usr/bin/git', '-C', str(repo_root), 'rev-parse', 'HEAD'])
        value = out.decode('utf-8').strip()
        if value:
            return value
    except Exception:
        pass
    return '0' * 40


def _day_obj(day_utc: str) -> date:
    return date.fromisoformat(str(day_utc).strip())


def _day_str(day_obj: date) -> str:
    return day_obj.isoformat()


def _decimal_or_zero(value: Any) -> Decimal:
    raw = str(value or '').strip()
    if not raw:
        return Decimal('0')
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError):
        return Decimal('0')


def _decimal_text(value: Decimal, quant: Decimal = RET_Q) -> str:
    return format(value.quantize(quant, rounding=ROUND_HALF_UP), 'f')


def _decimal_to_int_cents(value: Any) -> int:
    return int((_decimal_or_zero(value) * Decimal('100')).to_integral_value(rounding=ROUND_HALF_UP))


def _optional_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return None
    return _read_json(path)


def _runtime_hash_for_day(truth_root: Path, day_utc: str) -> str:
    path = truth_root / "reports" / "aegis_runtime_truth_kernel_v1" / day_utc / "runtime_evaluation.v1.json"
    obj = _optional_json(path) or {}
    return str(obj.get("deterministic_output_hash") or obj.get("runtime_evaluation_hash") or "")


def _operator_statement_path(ctx: EconomicContext) -> Path:
    preferred = ctx.execution_truth_root / "operator_inputs" / "cash_ledger_operator_statements" / ctx.day_utc / "operator_statement.v1.json"
    if preferred.exists():
        return preferred.resolve()
    return (ctx.canonical_truth_root / "operator_inputs" / "cash_ledger_operator_statements" / ctx.day_utc / "operator_statement.v1.json").resolve()


def compute_economic_state_context_hash_v1(*, day_utc: str, sleeve_id: str, environment: str, ib_account: str, operation_type: str) -> str:
    return canonical_hash_for_c2_artifact_v1(
        {
            'account_id': str(ib_account).strip().upper(),
            'day_utc': str(day_utc).strip(),
            'environment': str(environment).strip().upper(),
            'operation_type': str(operation_type).strip(),
            'sleeve_id': str(sleeve_id).strip().upper(),
        }
    )


def resolve_economic_state_context_v1(*, repo_root: Path, operation_type: str, day_utc: str, sleeve_id: str, environment: str, ib_account: str) -> EconomicContext:
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
    global_ctx = resolve_global_context_v1(
        repo_root=repo_root,
        operation_type=operation_type,
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=account_binding.ib_account,
    )
    return EconomicContext(
        repo_root=repo_root,
        canonical_truth_root=resolve_canonical_truth_root().resolve(),
        truth_sleeves_root=resolve_truth_sleeves_root().resolve(),
        execution_truth_root=execution_root.execution_root_path.resolve(),
        day_utc=str(day_utc).strip(),
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=account_binding.ib_account,
        operation_type=operation_type,
        context_hash=compute_economic_state_context_hash_v1(
            day_utc=day_utc,
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=account_binding.ib_account,
            operation_type=operation_type,
        ),
        global_context_hash=global_ctx.context_hash,
        global_context_build_path=global_context_build_path_v1(ctx=global_ctx),
        global_context_package_path=global_context_package_path_v1(ctx=global_ctx),
    )


def economic_state_build_path_v1(*, ctx: EconomicContext) -> Path:
    return (ctx.canonical_truth_root / 'reports' / BUILD_REPORT_FAMILY / ctx.day_utc / ctx.context_hash / 'economic_state_build.v1.json').resolve()


def economic_state_package_path_v1(*, ctx: EconomicContext) -> Path:
    return (ctx.execution_truth_root / PACKAGE_FAMILY / ctx.day_utc / ctx.context_hash / 'economic_state_package.v1.json').resolve()


def _load_manifest(repo_root: Path, operation_type: str) -> Dict[str, Any]:
    registry_path = (repo_root / MANIFEST_REGISTRY_RELPATH).resolve()
    registry = _read_json(registry_path)
    manifests = registry.get('manifests')
    if not isinstance(manifests, dict):
        raise ValueError('ECONOMIC_STATE_MANIFEST_REGISTRY_INVALID')
    manifest = manifests.get(operation_type)
    if not isinstance(manifest, dict):
        raise ValueError(f'ECONOMIC_STATE_MANIFEST_NOT_FOUND:operation_type={operation_type}')
    out = {'schema_id': 'economic_dependency_manifest', 'schema_version': 'v1', **manifest}
    validate_against_repo_schema_v1(out, repo_root, MANIFEST_SCHEMA_RELPATH)
    return out


def _dependency_path(ctx: EconomicContext, dependency: Dict[str, Any]) -> Path:
    pattern = str(dependency.get('path_pattern') or '').strip()
    return Path(pattern.format(
        canonical_truth_root=str(ctx.canonical_truth_root),
        execution_truth_root=str(ctx.execution_truth_root),
        day_utc=ctx.day_utc,
        sleeve_id=ctx.sleeve_id,
        environment=ctx.environment,
        mode=ctx.environment,
        ib_account=ctx.ib_account,
        account_id=ctx.ib_account,
        context_hash=ctx.context_hash,
        global_context_hash=ctx.global_context_hash,
        operation_type=ctx.operation_type,
    )).resolve()


def _blocking_build_path(ctx: EconomicContext, dependency: Dict[str, Any]) -> Optional[Path]:
    pattern = str(dependency.get('blocking_build_path_pattern') or '').strip()
    if not pattern:
        return None
    return Path(pattern.format(
        canonical_truth_root=str(ctx.canonical_truth_root),
        execution_truth_root=str(ctx.execution_truth_root),
        day_utc=ctx.day_utc,
        context_hash=ctx.context_hash,
        global_context_hash=ctx.global_context_hash,
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


def _evaluate_semantics(*, dependency_id: str, obj: Dict[str, Any], path: Path, ctx: EconomicContext) -> tuple[str, str]:
    day_value = _day_value(obj)
    if day_value and day_value != ctx.day_utc:
        return STATUS_STALE, f'DAY_MISMATCH:expected={ctx.day_utc}:actual={day_value}:path={path}'
    artifact_runtime_hash = str(obj.get('runtime_evaluation_hash') or '').strip()
    current_runtime_hash = _runtime_hash_for_day(ctx.canonical_truth_root, ctx.day_utc)
    source_type = str(obj.get('source_type') or '').strip().upper()
    runtime_bound_source_dependencies = {
        'global_context_package_v1',
        'capital_authority_allocation_v1',
    }
    immutable_account_source_dependencies = {
        'cash_ledger_snapshot_v1',
        'positions_snapshot_v5',
        'position_lifecycle_snapshot_v2',
        'accounting_nav_v2',
    }
    if (
        artifact_runtime_hash
        and current_runtime_hash
        and artifact_runtime_hash != current_runtime_hash
        and dependency_id not in immutable_account_source_dependencies
    ):
        return STATUS_STALE, f'RUNTIME_EVALUATION_HASH_MISMATCH:expected={current_runtime_hash}:actual={artifact_runtime_hash}:path={path}'
    if dependency_id in immutable_account_source_dependencies and artifact_runtime_hash and current_runtime_hash and artifact_runtime_hash != current_runtime_hash:
        if source_type not in {'STATIC_RISK_BUDGET', 'SIMULATION_LEDGER', 'MANUAL_DECLARATION', 'BROKER_EXPORT'}:
            return STATUS_STALE, f'ACCOUNT_SOURCE_RUNTIME_HASH_MISMATCH_WITH_UNKNOWN_SOURCE:expected={current_runtime_hash}:actual={artifact_runtime_hash}:path={path}'

    if dependency_id == 'global_context_package_v1':
        sealed = bool(obj.get('sealed') is True)
        mode = str(obj.get('mode') or '').strip().upper()
        sleeve_id = str(obj.get('sleeve_id') or '').strip().upper()
        account_id = str(obj.get('account_id') or '').strip().upper()
        op_type = str(obj.get('operation_type') or '').strip()
        context_hash = str(obj.get('context_hash') or '').strip()
        if not sealed:
            return STATUS_FAILED, f'GLOBAL_CONTEXT_PACKAGE_UNSEALED:path={path}'
        if mode != ctx.environment or sleeve_id != ctx.sleeve_id or account_id != ctx.ib_account.upper() or op_type != ctx.operation_type or context_hash != ctx.global_context_hash:
            return STATUS_STALE, f'GLOBAL_CONTEXT_PACKAGE_CONTEXT_MISMATCH:path={path}'
        return STATUS_PRESENT, 'GLOBAL_CONTEXT_PACKAGE_SEALED'

    if dependency_id == 'cash_ledger_snapshot_v1':
        if str(obj.get('schema_id') or '').strip() != 'C2_CASH_LEDGER_SNAPSHOT_V1':
            return STATUS_FAILED, f'CASH_LEDGER_SCHEMA_MISMATCH:path={path}'
        status = _status_value(obj)
        if status and status not in {'OK', 'DEGRADED_OPERATOR_INPUT'}:
            return STATUS_FAILED, f'CASH_LEDGER_STATUS_NOT_OK:status={status}:path={path}'
        snapshot = obj.get('snapshot') if isinstance(obj.get('snapshot'), dict) else {}
        if not isinstance(snapshot.get('cash_total_cents'), int):
            return STATUS_FAILED, f'CASH_LEDGER_TOTAL_INVALID:path={path}'
        return STATUS_PRESENT, 'CASH_LEDGER_OK'

    if dependency_id == 'positions_snapshot_v5':
        if str(obj.get('schema_id') or '').strip() != 'C2_POSITIONS_SNAPSHOT_V5':
            return STATUS_FAILED, f'POSITIONS_SCHEMA_MISMATCH:path={path}'
        status = _status_value(obj)
        if status and status != 'OK':
            return STATUS_FAILED, f'POSITIONS_STATUS_NOT_OK:status={status}:path={path}'
        items = obj.get('items')
        if not isinstance(items, list):
            return STATUS_FAILED, f'POSITIONS_ITEMS_INVALID:path={path}'
        reconciliation = obj.get('reconciliation') if isinstance(obj.get('reconciliation'), dict) else {}
        if not isinstance(reconciliation.get('positions_status'), str):
            return STATUS_FAILED, f'POSITIONS_RECONCILIATION_INVALID:path={path}'
        source_type = str(obj.get('source_type') or '').strip().upper()
        reason_codes = {str(code).strip().upper() for code in obj.get('reason_codes') or [] if str(code).strip()}
        broker_statement_present = bool(reconciliation.get('broker_statement_present') is True)
        if (
            source_type == 'SIMULATION_LEDGER'
            and not items
            and not broker_statement_present
            and 'BROKER_STATEMENT_MISSING_INTERNAL_STATE_ONLY' in reason_codes
        ):
            return STATUS_PRESENT, 'EMPTY_POSITIONS_STATIC_PAPER'
        return STATUS_PRESENT, 'POSITIONS_CONTEXT_OK'

    if dependency_id == 'position_lifecycle_snapshot_v2':
        if str(obj.get('schema_id') or '').strip() != 'C2_POSITION_LIFECYCLE_SNAPSHOT':
            return STATUS_FAILED, f'LIFECYCLE_SCHEMA_MISMATCH:path={path}'
        status = _status_value(obj)
        if status and status != 'OK':
            return STATUS_FAILED, f'LIFECYCLE_STATUS_NOT_OK:status={status}:path={path}'
        items = obj.get('items')
        if not isinstance(items, list):
            return STATUS_FAILED, f'LIFECYCLE_ITEMS_INVALID:path={path}'
        return STATUS_PRESENT, 'POSITION_LIFECYCLE_OK'

    if dependency_id == 'accounting_nav_v2':
        nav = obj.get('nav') if isinstance(obj.get('nav'), dict) else {}
        nav_total = nav.get('nav_total')
        status = _status_value(obj)
        if not isinstance(nav_total, int):
            return STATUS_FAILED, f'NAV_TOTAL_INVALID:path={path}'
        if status and status not in {'OK', 'ACTIVE', 'BOOTSTRAP', 'PASS'}:
            return STATUS_FAILED, f'ACCOUNTING_NAV_STATUS_NOT_OK:status={status}:path={path}'
        return STATUS_PRESENT, 'ACCOUNTING_NAV_OK'

    if dependency_id == 'capital_authority_allocation_v1':
        schema_id = str(obj.get('schema_id') or '').strip()
        if schema_id not in {'C2_CAPITAL_AUTHORITY_ALLOCATION_V1', 'capital_authority_allocation'}:
            return STATUS_FAILED, f'CAPITAL_AUTHORITY_SCHEMA_MISMATCH:path={path}'
        status = _status_value(obj)
        validation_status = str(obj.get('validation_status') or '').strip().upper()
        if status in {'OK', 'PASS', 'AUTHORIZED', 'ACTIVE'} or validation_status == 'VALID':
            decision_chain = obj.get('decision_chain') if isinstance(obj.get('decision_chain'), dict) else {}
            if not isinstance(decision_chain.get('authorized_trade_intents'), list):
                return STATUS_FAILED, f'CAPITAL_AUTHORITY_DECISION_CHAIN_INVALID:path={path}'
            return STATUS_PRESENT, f'CAPITAL_AUTHORITY_ALLOCATION_{validation_status or status or "VALID"}'
        return STATUS_FAILED, f'CAPITAL_AUTHORITY_ALLOCATION_NOT_OK:status={status or validation_status or "MISSING"}:path={path}'

    return STATUS_PRESENT, 'ARTIFACT_PRESENT'


def _producer_commands(*, producer_ref: str, ctx: EconomicContext) -> List[List[str]]:
    py = sys.executable
    git_sha = "0" * 40
    try:
        out = subprocess.check_output(["/usr/bin/git", "-C", str(ctx.repo_root), "rev-parse", "HEAD"])
        git_sha = out.decode("utf-8").strip() or git_sha
    except Exception:
        pass
    repo_name = ctx.repo_root.name
    day = ctx.day_utc
    if producer_ref == "global_context_authority_v1":
        return [[
            py,
            str((ctx.repo_root / "ops/tools/run_global_context_authority_v1.py").resolve()),
            "--operation_type", ctx.operation_type,
            "--day_utc", day,
            "--sleeve_id", ctx.sleeve_id,
            "--environment", ctx.environment,
            "--ib_account", ctx.ib_account,
            "--materialize", "YES",
            "--emit_package", "YES",
        ]]
    if producer_ref == "cash_ledger_snapshot_day_v1":
        return [[
            py,
            "-m", "constellation_2.phaseF.cash_ledger.run.run_cash_ledger_snapshot_day_v1",
            "--day_utc", day,
            "--operator_statement_json", str(_operator_statement_path(ctx)),
            "--producer_git_sha", git_sha,
            "--producer_repo", repo_name,
        ]]
    if producer_ref == "positions_snapshot_day_v5":
        return [[
            py,
            "-m", "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v5",
            "--day_utc", day,
            "--producer_git_sha", git_sha,
            "--producer_repo", repo_name,
            "--truth_root", str(ctx.canonical_truth_root),
            "--ib_account", ctx.ib_account,
        ]]
    if producer_ref == "position_lifecycle_snapshot_v2":
        return [[
            py,
            str((ctx.repo_root / "ops/tools/run_position_lifecycle_snapshot_v2.py").resolve()),
            "--day_utc", day,
            "--truth_root", str(ctx.canonical_truth_root),
        ]]
    if producer_ref == "accounting_nav_v2_day_v1":
        return [[
            py,
            str((ctx.repo_root / "ops/tools/run_accounting_nav_v2_day_v1.py").resolve()),
            "--day_utc", day,
            "--producer_git_sha", git_sha,
            "--producer_repo", repo_name,
            "--truth_root", str(ctx.canonical_truth_root),
        ]]
    if producer_ref == "allocation_day_v2":
        return [[
            py,
            str((ctx.repo_root / "constellation_2/phaseG/allocation/run/run_allocation_day_v2.py").resolve()),
            "--day_utc", day,
            "--producer_git_sha", git_sha,
            "--producer_repo", repo_name,
            "--truth_root", str(ctx.canonical_truth_root),
        ]]
    if producer_ref == "correlation_envelope_gate_v1":
        return [
            [
                py,
                str((ctx.repo_root / "ops/tools/run_engine_daily_returns_day_v1.py").resolve()),
                "--day_utc", day,
                "--truth_root", str(ctx.canonical_truth_root),
            ],
            [
                py,
                str((ctx.repo_root / "constellation_2/phaseJ/monitoring/run/run_engine_correlation_matrix_day_v1.py").resolve()),
                "--day_utc", day,
                "--truth_root", str(ctx.canonical_truth_root),
            ],
            [
                py,
                str((ctx.repo_root / "ops/tools/run_correlation_envelope_gate_v1.py").resolve()),
                "--day_utc", day,
                "--truth_root", str(ctx.canonical_truth_root),
                "--produced_utc", _anchor_utc(day),
                "--mode", ctx.environment,
            ],
        ]
    if producer_ref == "capital_risk_envelope_gate_v2":
        return [[
            py,
            str((ctx.repo_root / "ops/tools/run_c2_capital_risk_envelope_gate_v2.py").resolve()),
            "--out_day_utc", day,
            "--input_day_utc", day,
            "--truth_root", str(ctx.canonical_truth_root),
            "--produced_utc", _anchor_utc(day),
        ]]
    if producer_ref == "exposure_net_day_v1":
        return [[
            py,
            str((ctx.repo_root / "ops/tools/run_exposure_net_day_v1.py").resolve()),
            "--day_utc", day,
            "--truth_root", str(ctx.canonical_truth_root),
        ]]
    if producer_ref == "capital_authority_allocation_day_v1":
        authority_verdict_path = (
            ctx.execution_truth_root
            / "reports"
            / "authorization_gate_verdict_v1"
            / day
            / "authorization_gate_verdict.v1.json"
        ).resolve()
        if not authority_verdict_path.exists():
            authority_verdict_path = (
                ctx.execution_truth_root
                / "reports"
                / "gate_stack_verdict_v1"
                / day
                / "gate_stack_verdict.v1.json"
            ).resolve()
        return [
            [
                py,
                str((ctx.repo_root / "ops/tools/run_execution_positions_snapshot_v5_bridge_v1.py").resolve()),
                "--day_utc", day,
                "--source_truth_root", str(ctx.canonical_truth_root),
                "--truth_root", str(ctx.execution_truth_root),
            ],
            [
                py,
                str((ctx.repo_root / "ops/tools/run_capital_authority_allocation_day_v1.py").resolve()),
                "--day_utc", day,
                "--truth_root", str(ctx.execution_truth_root),
                "--canonical_sequence_owner", "ops/tools/run_c2_paper_day_orchestrator_v2.py",
                "--authority_verdict_path", str(authority_verdict_path),
            ],
        ]
    raise ValueError(f"ECONOMIC_STATE_UNKNOWN_PRODUCER:{producer_ref}")

def _evaluate_dependencies(ctx: EconomicContext, manifest: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    results: Dict[str, Dict[str, Any]] = {}
    for dependency in manifest.get('dependencies', []):
        dep_id = str(dependency.get('dependency_id') or '').strip()
        upstream_ids = [str(x).strip() for x in dependency.get('upstream_dependency_ids') or [] if str(x).strip()]
        path = _dependency_path(ctx, dependency)
        upstream_blockers = [uid for uid in upstream_ids if results.get(uid, {}).get('status') != STATUS_PRESENT]
        owner_ref = dependency.get('owner_ref')
        producer_ref = dependency.get('producer_ref')
        blocking_build_ref = None
        first_real_blocker_dependency_id = None

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
                if dep_id == 'global_context_package_v1':
                    build_path = _blocking_build_path(ctx, dependency)
                    if build_path is not None and build_path.exists() and build_path.is_file():
                        build_obj = _read_json(build_path)
                        blocking_build_ref = str(build_path)
                        first_blocker = build_obj.get('first_real_blocker') if isinstance(build_obj.get('first_real_blocker'), dict) else {}
                        first_real_blocker_dependency_id = str(first_blocker.get('dependency_id') or '').strip() or None
                        if str(build_obj.get('closure_status') or '').strip().upper() != 'COMPLETE':
                            status = STATUS_FAILED
                            detail = f'GLOBAL_CONTEXT_BLOCKED:first_real_blocker={first_real_blocker_dependency_id or "UNKNOWN"}:path={first_blocker.get("path")}:build_ref={build_path}'
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
            'semantic_validation': str(dependency.get('semantic_validation') or ''),
            'upstream_dependency_ids': upstream_ids,
            'upstream_blockers': upstream_blockers,
            'status': status,
            'detail': detail,
            'sha256': sha256,
            'blocking_build_ref': blocking_build_ref,
            'first_real_blocker_dependency_id': first_real_blocker_dependency_id,
        }
    return results


def _stage_dependencies(manifest: Dict[str, Any], stage_id: str) -> List[Dict[str, Any]]:
    return [dep for dep in manifest.get('dependencies', []) if str(dep.get('stage_id') or '') == stage_id]


def _materializable_now(results: Dict[str, Dict[str, Any]], manifest: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for dependency in manifest.get('dependencies', []):
        dep_id = str(dependency.get('dependency_id') or '').strip()
        row = results.get(dep_id, {})
        if row.get('status') in {STATUS_MISSING, STATUS_STALE, STATUS_FAILED} and row.get('producer_ref') and not row.get('upstream_blockers'):
            out.append(dep_id)
    return out


def _run_materializers(ctx: EconomicContext, manifest: Dict[str, Any], results: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    materialized: List[Dict[str, Any]] = []
    executed_producers: set[str] = set()
    env = dict(os.environ)
    env['C2_TRUTH_ROOT'] = str(ctx.canonical_truth_root)
    current_results = results
    for stage_id in manifest.get('stage_order', []):
        while True:
            producer_refs: List[str] = []
            for dependency in _stage_dependencies(manifest, stage_id):
                dep_id = str(dependency.get('dependency_id') or '').strip()
                result = current_results.get(dep_id, {})
                producer_ref = str(result.get('producer_ref') or '').strip()
                if not producer_ref or producer_ref in executed_producers:
                    continue
                if result.get('status') in {STATUS_MISSING, STATUS_STALE, STATUS_FAILED} and not result.get('upstream_blockers'):
                    producer_refs.append(producer_ref)
            if not producer_refs:
                break
            for producer_ref in producer_refs:
                commands = _producer_commands(producer_ref=producer_ref, ctx=ctx)
                for cmd in commands:
                    proc = subprocess.run(cmd, cwd=str(ctx.repo_root), capture_output=True, text=True, env=env)
                    materialized.append({
                        'stage_id': stage_id,
                        'producer_ref': producer_ref,
                        'command': cmd,
                        'returncode': int(proc.returncode),
                        'stdout': proc.stdout[-4000:],
                        'stderr': proc.stderr[-4000:],
                    })
                executed_producers.add(producer_ref)
            current_results = _evaluate_dependencies(ctx, manifest)
    return materialized

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


def _required_dep_path(results: Dict[str, Dict[str, Any]], dependency_id: str) -> Path:
    row = results.get(dependency_id) or {}
    path = Path(str(row.get('path') or '')).resolve()
    if not str(path):
        raise ValueError(f'ECONOMIC_DEPENDENCY_PATH_MISSING:{dependency_id}')
    return path


def _paper_bootstrap_execution_nav_path(ctx: EconomicContext, day_utc: str) -> Path:
    return (ctx.execution_truth_root / 'accounting_v2' / 'nav' / day_utc / 'nav.v2.json').resolve()


def _select_nav_evaluation_basis(
    *,
    ctx: EconomicContext,
    canonical_nav_path: Path,
    day_utc: str,
) -> tuple[Path, str]:
    if ctx.environment == 'PAPER' and ctx.operation_type == 'fresh_paper_entry_v1':
        execution_nav_path = _paper_bootstrap_execution_nav_path(ctx, day_utc)
        if execution_nav_path.exists() and execution_nav_path.is_file():
            execution_nav_obj = _optional_json(execution_nav_path)
            if isinstance(execution_nav_obj, dict):
                status, _detail = _evaluate_semantics(
                    dependency_id='accounting_nav_v2',
                    obj=execution_nav_obj,
                    path=execution_nav_path,
                    ctx=ctx,
                )
                if status == STATUS_PRESENT:
                    return execution_nav_path, 'EXECUTION_TRUTH_ROOT_PAPER_BOOTSTRAP'
    return canonical_nav_path, 'CANONICAL_TRUTH_ROOT'


def _load_marks_by_symbol(truth_root: Path, day_utc: str) -> tuple[Optional[Path], Dict[str, Dict[str, Any]]]:
    path = (truth_root / 'market_data_snapshot_v1' / 'broker_marks_v1' / day_utc / 'broker_marks.v1.json').resolve()
    obj = _optional_json(path)
    if obj is None:
        return None, {}
    out: Dict[str, Dict[str, Any]] = {}
    for row in obj.get('marks') or []:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get('symbol') or '').strip().upper()
        if not symbol:
            continue
        out[symbol] = {
            'qty': _decimal_or_zero(row.get('qty')),
            'market_value': int(_decimal_or_zero(row.get('market_value'))),
            'price': _decimal_or_zero(row.get('implied_price')),
            'avg_cost': _decimal_or_zero(row.get('avg_cost')),
        }
    return path, out


def _position_symbol(position: Dict[str, Any]) -> str:
    instrument = position.get('instrument') if isinstance(position.get('instrument'), dict) else {}
    return str(instrument.get('symbol') or instrument.get('underlying') or '').strip().upper()


def _position_value_from_marks(position: Dict[str, Any], marks_by_symbol: Dict[str, Dict[str, Any]]) -> Optional[int]:
    qty = abs(int(position.get('qty') or 0))
    if qty == 0:
        return 0
    symbol = _position_symbol(position)
    mark = marks_by_symbol.get(symbol)
    if not isinstance(mark, dict):
        return None
    mark_qty = int(abs(mark.get('qty') or 0))
    market_value = int(abs(mark.get('market_value') or 0))
    if mark_qty == qty and market_value >= 0:
        return market_value
    price = _decimal_or_zero(mark.get('price'))
    if price <= 0:
        return None
    return int((price * Decimal(qty)).to_integral_value(rounding=ROUND_HALF_UP))


def _group_values_by_engine_and_sleeve(
    *,
    positions_obj: Dict[str, Any],
    marks_by_symbol: Dict[str, Dict[str, Any]],
    engine_to_sleeve: Dict[str, str],
) -> tuple[Dict[str, int], Dict[str, int], List[str]]:
    engine_values: Dict[str, int] = {}
    sleeve_values: Dict[str, int] = {}
    reason_codes: List[str] = []
    for item in positions_obj.get('items') or []:
        if not isinstance(item, dict):
            continue
        engine_id = str(item.get('engine_id') or '').strip() or 'UNKNOWN_ENGINE'
        sleeve_id = engine_to_sleeve.get(engine_id, 'UNASSIGNED_IMPORTED')
        value = _position_value_from_marks(item, marks_by_symbol)
        if value is None:
            reason_codes.append(f'MISSING_MARK:{_position_symbol(item) or "UNKNOWN"}')
            continue
        engine_values[engine_id] = engine_values.get(engine_id, 0) + int(value)
        sleeve_values[sleeve_id] = sleeve_values.get(sleeve_id, 0) + int(value)
    return engine_values, sleeve_values, sorted(set(reason_codes))


def _point_return(current_value: int, previous_value: int) -> Optional[Decimal]:
    if previous_value <= 0:
        return None
    return (Decimal(current_value) / Decimal(previous_value)) - Decimal('1')


def _market_snapshot_path(truth_root: Path, day_utc: str, symbol: str) -> Path:
    return (truth_root / 'market_data_snapshot_v1' / 'snapshots' / day_utc / f'{symbol}.market_data_snapshot.v1.json').resolve()


def _market_snapshot_close(path: Path) -> Optional[Decimal]:
    obj = _optional_json(path)
    if obj is None:
        return None
    return _decimal_or_zero(obj.get('close'))


def _load_submission_fill_rows(truth_root: Path, day_utc: str) -> List[Dict[str, Any]]:
    submissions_root = (truth_root / 'execution_evidence_v1' / 'submissions' / day_utc).resolve()
    ledgers_root = (truth_root / 'fill_ledger_v1' / day_utc).resolve()
    if not submissions_root.exists() or not submissions_root.is_dir():
        return []
    rows: List[Dict[str, Any]] = []
    for subdir in sorted([p for p in submissions_root.iterdir() if p.is_dir()], key=lambda p: p.name):
        plan_path = (subdir / 'equity_order_plan.v2.json').resolve()
        if not plan_path.exists():
            plan_path = (subdir / 'equity_order_plan.v1.json').resolve()
        if not plan_path.exists():
            continue
        ledger_path = (ledgers_root / f'{subdir.name}.fill_ledger.v1.json').resolve()
        plan_obj = _optional_json(plan_path)
        ledger_obj = _optional_json(ledger_path)
        if plan_obj is None or ledger_obj is None:
            continue
        rows.append(
            {
                'submission_id': subdir.name,
                'engine_id': str(ledger_obj.get('engine_id') or plan_obj.get('engine_id') or '').strip(),
                'symbol': str(plan_obj.get('symbol') or '').strip().upper(),
                'action': str(plan_obj.get('action') or '').strip().upper(),
                'filled_qty': int(ledger_obj.get('filled_qty') or 0),
                'avg_fill_price_weighted': _decimal_or_zero(ledger_obj.get('avg_fill_price_weighted')),
                'intent_sha256': str(ledger_obj.get('intent_sha256') or '').strip(),
                'source_intent_id': str(ledger_obj.get('source_intent_id') or '').strip(),
            }
        )
    rows.sort(key=lambda row: (str(row['symbol']), str(row['engine_id']), str(row['submission_id'])))
    return rows


def _find_matching_close_fill(
    *,
    fill_rows: List[Dict[str, Any]],
    engine_id: str,
    symbol: str,
    closing_action: str,
) -> Optional[Dict[str, Any]]:
    matches = [
        row for row in fill_rows
        if str(row.get('engine_id') or '').strip() == engine_id
        and str(row.get('symbol') or '').strip().upper() == symbol.upper()
        and str(row.get('action') or '').strip().upper() == closing_action
        and int(row.get('filled_qty') or 0) > 0
    ]
    if not matches:
        return None
    matches.sort(key=lambda row: (-int(row.get('filled_qty') or 0), str(row.get('submission_id') or '')))
    return matches[0]


def _find_intent_file(truth_root: Path, opened_day_utc: str, intent_sha256: str) -> Optional[Path]:
    if len(str(intent_sha256 or '').strip()) != 64:
        return None
    preferred = (truth_root / 'intents_v1' / 'snapshots' / opened_day_utc / f'{intent_sha256}.exposure_intent.v1.json').resolve()
    if preferred.exists() and preferred.is_file():
        return preferred
    day_dir = (truth_root / 'intents_v1' / 'snapshots' / opened_day_utc).resolve()
    if day_dir.exists() and day_dir.is_dir():
        matches = sorted(day_dir.glob(f'{intent_sha256}*.json'))
        if matches:
            return matches[0].resolve()
    return None


def _extract_max_risk_pct(intent_obj: Dict[str, Any]) -> Optional[Decimal]:
    constraints = intent_obj.get('constraints') if isinstance(intent_obj.get('constraints'), dict) else {}
    if constraints:
        value = _decimal_or_zero(constraints.get('max_risk_pct'))
        if value > 0:
            return value
    sizing = intent_obj.get('sizing') if isinstance(intent_obj.get('sizing'), dict) else {}
    if sizing:
        value = _decimal_or_zero(sizing.get('max_risk_pct'))
        if value > 0:
            return value
    return None


def _build_economic_evaluation(ctx: EconomicContext, results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    day_utc = ctx.day_utc
    prev_day_utc = _day_str(_day_obj(day_utc) - timedelta(days=1))

    cash_path = _required_dep_path(results, 'cash_ledger_snapshot_v1')
    positions_path = _required_dep_path(results, 'positions_snapshot_v5')
    lifecycle_path = _required_dep_path(results, 'position_lifecycle_snapshot_v2')
    canonical_nav_path = _required_dep_path(results, 'accounting_nav_v2')
    nav_path, nav_basis = _select_nav_evaluation_basis(
        ctx=ctx,
        canonical_nav_path=canonical_nav_path,
        day_utc=day_utc,
    )
    nav_truth_root = ctx.execution_truth_root if nav_basis == 'EXECUTION_TRUTH_ROOT_PAPER_BOOTSTRAP' else ctx.canonical_truth_root
    capauth_path = _required_dep_path(results, 'capital_authority_allocation_v1')

    cash_obj = _read_json(cash_path)
    positions_obj = _read_json(positions_path)
    lifecycle_obj = _read_json(lifecycle_path)
    nav_obj = _read_json(nav_path)
    capauth_obj = _read_json(capauth_path)

    prev_nav_path = (nav_truth_root / 'accounting_v2' / 'nav' / prev_day_utc / 'nav.v2.json').resolve()
    prev_positions_path = (ctx.canonical_truth_root / 'positions_v1' / 'snapshots' / prev_day_utc / 'positions_snapshot.v5.json').resolve()
    prev_nav_obj = _optional_json(prev_nav_path)
    prev_positions_obj = _optional_json(prev_positions_path)

    current_marks_path, current_marks = _load_marks_by_symbol(ctx.canonical_truth_root, day_utc)
    prev_marks_path, prev_marks = _load_marks_by_symbol(ctx.canonical_truth_root, prev_day_utc)

    nav_total = int(((nav_obj.get('nav') or {}) if isinstance(nav_obj.get('nav'), dict) else {}).get('nav_total') or 0)
    prev_nav_total = int(((prev_nav_obj.get('nav') or {}) if isinstance((prev_nav_obj or {}).get('nav'), dict) else {}).get('nav_total') or 0)
    gross_positions_value = int(((nav_obj.get('nav') or {}) if isinstance(nav_obj.get('nav'), dict) else {}).get('gross_positions_value') or 0)
    prev_gross_positions_value = int(((prev_nav_obj.get('nav') or {}) if isinstance((prev_nav_obj or {}).get('nav'), dict) else {}).get('gross_positions_value') or 0)
    portfolio_pnl = int(nav_total - prev_nav_total) if prev_nav_obj is not None else 0
    portfolio_return = _point_return(nav_total, prev_nav_total)

    engine_to_sleeve: Dict[str, str] = {}
    for row in capauth_obj.get('per_sleeve') or []:
        if not isinstance(row, dict):
            continue
        sleeve_id = str(row.get('sleeve_id') or '').strip()
        for engine_id in row.get('engine_ids') or []:
            engine_text = str(engine_id or '').strip()
            if engine_text:
                engine_to_sleeve[engine_text] = sleeve_id

    current_engine_values, current_sleeve_values, current_mark_reasons = _group_values_by_engine_and_sleeve(
        positions_obj=positions_obj,
        marks_by_symbol=current_marks,
        engine_to_sleeve=engine_to_sleeve,
    )
    prev_engine_values, prev_sleeve_values, prev_mark_reasons = _group_values_by_engine_and_sleeve(
        positions_obj=prev_positions_obj or {'items': []},
        marks_by_symbol=prev_marks,
        engine_to_sleeve=engine_to_sleeve,
    )

    sleeve_rows: List[Dict[str, Any]] = []
    for sleeve_id in sorted(set(current_sleeve_values) | set(prev_sleeve_values)):
        current_value = int(current_sleeve_values.get(sleeve_id, 0))
        previous_value = int(prev_sleeve_values.get(sleeve_id, 0))
        sleeve_return = _point_return(current_value, previous_value)
        sleeve_rows.append(
            {
                'sleeve_id': sleeve_id,
                'current_value': current_value,
                'previous_value': previous_value,
                'pnl': int(current_value - previous_value),
                'return': (_decimal_text(sleeve_return) if sleeve_return is not None else None),
                'return_status': ('OK' if sleeve_return is not None else ('NEW_DEPLOYMENT' if current_value > 0 and previous_value == 0 else 'NO_PRIOR_VALUE')),
                'capital_efficiency_return_vs_deployed_capital': (
                    _decimal_text(Decimal(int(current_value - previous_value)) / Decimal(previous_value))
                    if previous_value > 0 else None
                ),
            }
        )

    engine_rows: List[Dict[str, Any]] = []
    for engine_id in sorted(set(current_engine_values) | set(prev_engine_values)):
        current_value = int(current_engine_values.get(engine_id, 0))
        previous_value = int(prev_engine_values.get(engine_id, 0))
        engine_return = _point_return(current_value, previous_value)
        engine_rows.append(
            {
                'engine_id': engine_id,
                'strategy_sleeve_id': engine_to_sleeve.get(engine_id, 'UNASSIGNED_IMPORTED'),
                'current_value': current_value,
                'previous_value': previous_value,
                'pnl': int(current_value - previous_value),
                'return': (_decimal_text(engine_return) if engine_return is not None else None),
                'return_status': ('OK' if engine_return is not None else ('NEW_DEPLOYMENT' if current_value > 0 and previous_value == 0 else 'NO_PRIOR_VALUE')),
            }
        )

    nav_days: List[str] = []
    nav_root = (nav_truth_root / 'accounting_v2' / 'nav').resolve()
    if nav_root.exists() and nav_root.is_dir():
        for child in sorted(nav_root.iterdir()):
            if child.is_dir():
                try:
                    if _day_obj(child.name) <= _day_obj(day_utc):
                        nav_days.append(child.name)
                except Exception:
                    continue
    rolling_peak_nav = 0
    for nav_day in nav_days:
        obj = _optional_json(nav_root / nav_day / 'nav.v2.json') or {}
        nav_value = int((((obj.get('nav') or {}) if isinstance(obj.get('nav'), dict) else {}).get('nav_total')) or 0)
        if nav_value > rolling_peak_nav:
            rolling_peak_nav = nav_value
    drawdown = Decimal('0')
    if rolling_peak_nav > 0 and nav_total < rolling_peak_nav:
        drawdown = (Decimal(nav_total) / Decimal(rolling_peak_nav)) - Decimal('1')

    target_symbols = sorted(
        {
            str(row.get('symbol') or '').strip().upper()
            for row in (((capauth_obj.get('allocation_state') or {}) if isinstance(capauth_obj.get('allocation_state'), dict) else {}).get('target_rows') or [])
            if isinstance(row, dict) and str(row.get('symbol') or '').strip()
        }
    )
    external_rows: List[Dict[str, Any]] = []
    for symbol in target_symbols:
        cur_close = _market_snapshot_close(_market_snapshot_path(ctx.canonical_truth_root, day_utc, symbol))
        prev_close = _market_snapshot_close(_market_snapshot_path(ctx.canonical_truth_root, prev_day_utc, symbol))
        if cur_close is None or prev_close is None or prev_close <= 0:
            continue
        benchmark_return = (cur_close / prev_close) - Decimal('1')
        external_rows.append(
            {
                'benchmark_id': f'SYMBOL_CLOSE_{symbol}',
                'symbol': symbol,
                'daily_return': _decimal_text(benchmark_return),
                'comparison_vs_portfolio_return': (
                    _decimal_text((portfolio_return or Decimal('0')) - benchmark_return)
                    if portfolio_return is not None else None
                ),
            }
        )

    fill_rows = _load_submission_fill_rows(ctx.canonical_truth_root, day_utc)
    prev_positions_by_id = {
        str(item.get('position_id') or '').strip(): item
        for item in ((prev_positions_obj or {}).get('items') or [])
        if isinstance(item, dict) and str(item.get('position_id') or '').strip()
    }
    r_rows: List[Dict[str, Any]] = []
    tax_rows: List[Dict[str, Any]] = []
    for item in positions_obj.get('items') or []:
        if not isinstance(item, dict):
            continue
        if str(item.get('status') or '').strip().upper() != 'CLOSED':
            continue
        if str(item.get('last_transition_type') or '').strip().upper() != 'FULL_CLOSE':
            continue
        position_id = str(item.get('position_id') or '').strip()
        prev_item = prev_positions_by_id.get(position_id)
        symbol = _position_symbol(prev_item or item)
        prev_qty = int((prev_item or {}).get('qty') or 0)
        closing_action = 'SELL' if prev_qty >= 0 else 'BUY'
        matching_fill = _find_matching_close_fill(
            fill_rows=fill_rows,
            engine_id=str((prev_item or item).get('engine_id') or '').strip(),
            symbol=symbol,
            closing_action=closing_action,
        )
        holding_days = (_day_obj(day_utc) - _day_obj(str(item.get('opened_day_utc') or day_utc))).days
        tax_rows.append(
            {
                'position_id': position_id,
                'symbol': symbol,
                'holding_days': int(max(holding_days, 0)),
                'term_classification': ('LONG_TERM' if holding_days >= 365 else 'SHORT_TERM'),
                'realized_tax_amount': None,
                'after_tax_pnl': None,
                'status': 'TERM_CLASSIFICATION_ONLY',
            }
        )
        reason_codes: List[str] = []
        r_multiple_text: Optional[str] = None
        if prev_item is None:
            reason_codes.append('R_METRIC_PREV_POSITION_MISSING')
        elif matching_fill is None:
            reason_codes.append('R_METRIC_CLOSE_FILL_MISSING')
        else:
            intent_path = _find_intent_file(ctx.canonical_truth_root, str(item.get('opened_day_utc') or day_utc), str(item.get('intent_sha256') or ''))
            if intent_path is None:
                reason_codes.append('R_METRIC_OPEN_INTENT_MISSING')
            else:
                intent_obj = _read_json(intent_path)
                max_risk_pct = _extract_max_risk_pct(intent_obj)
                if max_risk_pct is None or max_risk_pct <= 0:
                    reason_codes.append('R_METRIC_MAX_RISK_UNPROVEN')
                else:
                    entry_nav_path = (nav_truth_root / 'accounting_v2' / 'nav' / str(item.get('opened_day_utc') or day_utc) / 'nav.v2.json').resolve()
                    entry_nav_obj = _optional_json(entry_nav_path)
                    entry_nav_total = int((((entry_nav_obj or {}).get('nav') or {}) if isinstance((entry_nav_obj or {}).get('nav'), dict) else {}).get('nav_total') or 0)
                    entry_qty = abs(int((prev_item or {}).get('qty') or 0))
                    entry_price = Decimal(int((prev_item or {}).get('avg_cost_cents') or 0)) / Decimal('100')
                    exit_price = _decimal_or_zero(matching_fill.get('avg_fill_price_weighted'))
                    if entry_nav_total <= 0 or entry_qty <= 0 or entry_price <= 0 or exit_price <= 0:
                        reason_codes.append('R_METRIC_ENTRY_BASIS_INCOMPLETE')
                    else:
                        initial_r_value = (
                            (Decimal(entry_nav_total) * max_risk_pct) / Decimal(entry_qty)
                        ).quantize(Decimal('0.0001'), rounding=ROUND_CEILING)
                        if initial_r_value <= 0:
                            reason_codes.append('R_METRIC_INITIAL_R_INVALID')
                        else:
                            side_short = prev_qty < 0
                            realized_r = ((entry_price - exit_price) / initial_r_value) if side_short else ((exit_price - entry_price) / initial_r_value)
                            r_multiple_text = _decimal_text(realized_r)
        r_rows.append(
            {
                'position_id': position_id,
                'symbol': symbol,
                'status': ('OK' if r_multiple_text is not None else 'UNKNOWN'),
                'r_multiple': r_multiple_text,
                'reason_codes': sorted(set(reason_codes)) if reason_codes else [],
            }
        )

    reallocation_rows: List[Dict[str, Any]] = []
    portfolio_return_value = portfolio_return if portfolio_return is not None else Decimal('0')
    for row in sleeve_rows:
        sleeve_return_raw = row.get('return')
        sleeve_return = _decimal_or_zero(sleeve_return_raw) if sleeve_return_raw is not None else None
        signal = 'MAINTAIN'
        reason_code = 'INSUFFICIENT_HISTORY'
        if sleeve_return is not None:
            delta = sleeve_return - portfolio_return_value
            if abs(delta) <= REALLOCATION_RETURN_DEADBAND:
                reason_code = 'WITHIN_PORTFOLIO_DEADBAND'
            elif sleeve_return > portfolio_return_value:
                signal = 'INCREASE'
                reason_code = 'OUTPERFORMING_PORTFOLIO'
            elif sleeve_return < portfolio_return_value:
                signal = 'DECREASE'
                reason_code = 'UNDERPERFORMING_PORTFOLIO'
            else:
                reason_code = 'INLINE_WITH_PORTFOLIO'
        reallocation_rows.append(
            {
                'sleeve_id': row['sleeve_id'],
                'signal': signal,
                'reason_code': reason_code,
                'observed_return': sleeve_return_raw,
                'portfolio_return': (_decimal_text(portfolio_return_value) if portfolio_return is not None else None),
            }
        )

    deployed_capital_efficiency = (
        _decimal_text(Decimal(portfolio_pnl) / Decimal(prev_gross_positions_value))
        if prev_gross_positions_value > 0 else None
    )

    evaluation_manifest: List[Dict[str, Any]] = [
        {'type': 'cash_ledger_snapshot_v1', 'path': str(cash_path), 'sha256': _sha256_file(cash_path)},
        {'type': 'positions_snapshot_v5', 'path': str(positions_path), 'sha256': _sha256_file(positions_path)},
        {'type': 'position_lifecycle_snapshot_v2', 'path': str(lifecycle_path), 'sha256': _sha256_file(lifecycle_path)},
        {'type': 'accounting_nav_v2', 'path': str(nav_path), 'sha256': _sha256_file(nav_path), 'nav_basis': nav_basis},
        {'type': 'capital_authority_allocation_v1', 'path': str(capauth_path), 'sha256': _sha256_file(capauth_path)},
    ]
    if nav_path != canonical_nav_path:
        evaluation_manifest.append({'type': 'accounting_nav_v2_canonical_required', 'path': str(canonical_nav_path), 'sha256': _sha256_file(canonical_nav_path), 'nav_basis': 'CANONICAL_TRUTH_ROOT_REQUIRED_DEPENDENCY'})
    if prev_nav_obj is not None:
        evaluation_manifest.append({'type': 'accounting_nav_v2_prev', 'path': str(prev_nav_path), 'sha256': _sha256_file(prev_nav_path)})
    if prev_positions_obj is not None:
        evaluation_manifest.append({'type': 'positions_snapshot_v5_prev', 'path': str(prev_positions_path), 'sha256': _sha256_file(prev_positions_path)})
    if current_marks_path is not None:
        evaluation_manifest.append({'type': 'broker_marks_v1', 'path': str(current_marks_path), 'sha256': _sha256_file(current_marks_path)})
    if prev_marks_path is not None:
        evaluation_manifest.append({'type': 'broker_marks_v1_prev', 'path': str(prev_marks_path), 'sha256': _sha256_file(prev_marks_path)})

    return {
        'benchmark_state': {
            'policy_baseline': {
                'benchmark_id': 'PREV_NAV_ZERO_RETURN_BASELINE_V1',
                'status': ('OK' if prev_nav_obj is not None else 'GENESIS'),
                'baseline_daily_return': _decimal_text(Decimal('0')),
                'comparison_vs_portfolio_return': (_decimal_text(portfolio_return) if portfolio_return is not None else None),
                'reason_codes': ([] if prev_nav_obj is not None else ['NO_PREVIOUS_NAV_FOR_POLICY_BASELINE']),
            },
            'external_benchmarks': external_rows,
            'reason_codes': ([] if external_rows else ['NO_EXTERNAL_BENCHMARK_SNAPSHOTS_AVAILABLE']),
        },
        'performance_state': {
            'portfolio': {
                'current_nav_total': int(nav_total),
                'previous_nav_total': (int(prev_nav_total) if prev_nav_obj is not None else None),
                'daily_pnl': int(portfolio_pnl),
                'daily_return': (_decimal_text(portfolio_return) if portfolio_return is not None else None),
                'status': ('OK' if portfolio_return is not None else 'GENESIS'),
                'nav_basis': nav_basis,
                'nav_path': str(nav_path),
            },
            'sleeves': sleeve_rows,
            'edges': engine_rows,
            'reason_codes': sorted(set(current_mark_reasons + prev_mark_reasons)),
        },
        'attribution_state': {
            'portfolio_pnl': int(portfolio_pnl),
            'sleeve': [
                {
                    'sleeve_id': row['sleeve_id'],
                    'pnl': row['pnl'],
                    'portfolio_return_contribution': (
                        _decimal_text(Decimal(int(row['pnl'])) / Decimal(prev_nav_total))
                        if prev_nav_total > 0 else None
                    ),
                }
                for row in sleeve_rows
            ],
            'edge': [
                {
                    'engine_id': row['engine_id'],
                    'strategy_sleeve_id': row['strategy_sleeve_id'],
                    'pnl': row['pnl'],
                    'portfolio_return_contribution': (
                        _decimal_text(Decimal(int(row['pnl'])) / Decimal(prev_nav_total))
                        if prev_nav_total > 0 else None
                    ),
                }
                for row in engine_rows
            ],
            'timing': {'status': 'UNKNOWN', 'reason_codes': ['TIMING_ATTRIBUTION_NOT_PROVEN_FROM_BUNDLE_A_B']},
            'execution': {'status': 'UNKNOWN', 'reason_codes': ['EXECUTION_ATTRIBUTION_NOT_PROVEN_FROM_BUNDLE_A_B']},
            'allocation': {'status': 'UNKNOWN', 'reason_codes': ['ALLOCATION_ATTRIBUTION_NOT_PROVEN_FROM_BUNDLE_A_B']},
            'tax': {'status': 'UNKNOWN', 'reason_codes': ['TAX_ATTRIBUTION_RUNTIME_NOT_INTEGRATED']},
        },
        'risk_state': {
            'rolling_peak_nav': int(rolling_peak_nav),
            'drawdown_pct': _decimal_text(drawdown, DD_Q),
            'nav_basis': nav_basis,
            'nav_path': str(nav_path),
            'reason_codes': ([] if rolling_peak_nav > 0 else ['NO_NAV_HISTORY_AVAILABLE']),
        },
        'r_metrics_state': {
            'closed_trade_r_rows': r_rows,
            'reason_codes': [row['status'] for row in r_rows if row.get('status') != 'OK'],
        },
        'tax_economic_state': {
            'status': ('TERM_CLASSIFICATION_ONLY' if tax_rows else 'UNKNOWN_NO_CLOSED_TRADES'),
            'closed_trade_tax_rows': tax_rows,
            'after_tax_portfolio_return': None,
            'reason_codes': ['RUNTIME_TAX_AMOUNT_NOT_PROVEN'],
        },
        'capital_efficiency_state': {
            'previous_deployed_capital': (int(prev_gross_positions_value) if prev_nav_obj is not None else None),
            'return_vs_deployed_capital': deployed_capital_efficiency,
            'reason_codes': ([] if deployed_capital_efficiency is not None else ['NO_PREVIOUS_DEPLOYED_CAPITAL']),
        },
        'reallocation_signal_state': {
            'signals': reallocation_rows,
            'reason_codes': [],
        },
        'input_manifest': evaluation_manifest,
        'reason_codes': ['BUNDLE_C_CANONICAL_ECONOMIC_EVALUATION_V1'],
        'upstream_truth': {
            'cash_ledger_snapshot_v1': str(cash_path),
            'positions_snapshot_v5': str(positions_path),
            'position_lifecycle_snapshot_v2': str(lifecycle_path),
            'accounting_nav_v2': str(nav_path),
            'accounting_nav_v2_required_dependency': str(canonical_nav_path),
            'capital_authority_allocation_v1': str(capauth_path),
        },
    }


def _constitutional_dependency_refs(*, repo_root: Path, results: Dict[str, Dict[str, Any]], dependency_ids: Sequence[str]) -> List[Dict[str, Any]]:
    refs: List[Dict[str, Any]] = []
    for dep_id in dependency_ids:
        row = results.get(dep_id) or {}
        if str(row.get('status') or '').strip() != STATUS_PRESENT:
            continue
        contract = get_constitutional_artifact_contract_v1(repo_root, dep_id)
        refs.append(
            {
                'artifact_id': dep_id,
                'path': str(row.get('path') or '').strip(),
                'sha256': str(row.get('sha256') or '').strip(),
                'artifact_class': str(contract.get('artifact_class') or '').strip(),
                'finality_state': str(contract.get('initial_finality_state') or '').strip(),
            }
        )
    return refs


def run_economic_state_authority_v1(*, repo_root: Path, operation_type: str, day_utc: str, sleeve_id: str, environment: str, ib_account: str, materialize: bool = True, emit_package: bool = True) -> Dict[str, Any]:
    ctx = resolve_economic_state_context_v1(repo_root=repo_root, operation_type=operation_type, day_utc=day_utc, sleeve_id=sleeve_id, environment=environment, ib_account=ib_account)
    manifest = _load_manifest(ctx.repo_root, operation_type)
    build_contract = assert_constitutional_writer_allowed_v1(
        ctx.repo_root,
        'economic_state_build_v1',
        'constellation_2.common.economic_state_authority_v1',
    )
    package_contract = assert_constitutional_writer_allowed_v1(
        ctx.repo_root,
        'economic_state_package_v1',
        'constellation_2.common.economic_state_authority_v1',
    )
    results = _evaluate_dependencies(ctx, manifest)
    materialized_nodes: List[Dict[str, Any]] = []
    if materialize and not _sealable(results, manifest):
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
    economic_evaluation = _build_economic_evaluation(ctx, results) if closure_status == CLOSURE_COMPLETE else None
    constitutional_refs = _constitutional_dependency_refs(
        repo_root=ctx.repo_root,
        results=results,
        dependency_ids=[str(x).strip() for x in build_contract.get('required_upstream_dependencies') or [] if str(x).strip()],
    )
    constitutional_dependency_declaration = build_artifact_dependency_declaration_v1(
        artifact_type='economic_state_build_v1',
        artifact_class=str(build_contract.get('artifact_class') or '').strip(),
        authority_id='economic_state_build_v1',
        declared_dependency_artifacts=[str(x).strip() for x in build_contract.get('required_upstream_dependencies') or [] if str(x).strip()],
        dependency_refs=constitutional_refs,
    )
    constitutional_lineage = build_governed_artifact_lineage_v1(
        artifact_type='economic_state_build_v1',
        artifact_version='v1',
        artifact_class=str(build_contract.get('artifact_class') or '').strip(),
        authority_id='economic_state_build_v1',
        producer_id='constellation_2.common.economic_state_authority_v1',
        generated_at_utc=_anchor_utc(ctx.day_utc),
        effective_at_utc=_anchor_utc(ctx.day_utc),
        finality_state=(FINALITY_FINALIZED if closure_status == CLOSURE_COMPLETE else FINALITY_PROVISIONAL),
        input_artifact_refs=constitutional_refs,
        policy_snapshot_refs=[],
        code_version=_git_sha(ctx.repo_root),
        run_id=ctx.context_hash,
    )
    build_obj: Dict[str, Any] = {
        'schema_id': 'economic_state_build',
        'schema_version': 'v1',
        'day_utc': ctx.day_utc,
        'sleeve_id': ctx.sleeve_id,
        'mode': ctx.environment,
        'account_id': ctx.ib_account,
        'operation_type': ctx.operation_type,
        'context_hash': ctx.context_hash,
        'dependency_manifest_ref': str((ctx.repo_root / MANIFEST_REGISTRY_RELPATH).resolve()),
        'dependency_results': _dependency_results_list(results, manifest),
        'constitutional_dependency_declaration': constitutional_dependency_declaration,
        'constitutional_lineage': constitutional_lineage,
        'closure_status': closure_status,
        'first_real_blocker': first_blocker,
        'blocking_chain': _blocking_chain(results, first_blocker_id),
        'materialized_nodes': materialized_nodes,
        'materializable_now': _materializable_now(results, manifest),
        'unowned_dependencies': [dep_id for dep_id, row in results.items() if row.get('status') == STATUS_UNOWNED],
        'economic_evaluation': economic_evaluation,
        'generated_utc': _anchor_utc(ctx.day_utc),
        'canonical_json_hash': None,
    }
    build_obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1(build_obj)
    validate_against_repo_schema_v1(build_obj, ctx.repo_root, BUILD_SCHEMA_RELPATH)
    build_path = economic_state_build_path_v1(ctx=ctx)
    build_sha = _write_canonical_json(build_path, build_obj)

    package_path = None
    package_obj = None
    if emit_package and closure_status == CLOSURE_COMPLETE:
        dependency_refs = []
        ref_map: Dict[str, Dict[str, Any]] = {}
        for dependency in manifest.get('dependencies', []):
            dep_id = str(dependency.get('dependency_id') or '').strip()
            row = results[dep_id]
            if row['required'] and not row['advisory_only'] and not row['post_submit_only']:
                entry = {'dependency_id': dep_id, 'path': row['path'], 'sha256': row['sha256'], 'owner_ref': row['owner_ref'], 'role_class': row['role_class'], 'status': row['status']}
                dependency_refs.append(entry)
                ref_map[dep_id] = entry
        package_constitutional_refs = [
            {
                'artifact_id': 'economic_state_build_v1',
                'path': str(build_path),
                'sha256': str(build_sha),
                'artifact_class': str(build_contract.get('artifact_class') or '').strip(),
                'finality_state': FINALITY_FINALIZED,
            }
        ]
        package_obj = {
            'schema_id': 'economic_state_package',
            'schema_version': 'v1',
            'day_utc': ctx.day_utc,
            'sleeve_id': ctx.sleeve_id,
            'mode': ctx.environment,
            'account_id': ctx.ib_account,
            'operation_type': ctx.operation_type,
            'context_hash': ctx.context_hash,
            'package_hash': None,
            'global_context_package_ref': ref_map.get('global_context_package_v1'),
            'cash_ledger_ref': ref_map.get('cash_ledger_snapshot_v1'),
            'positions_ref': ref_map.get('positions_snapshot_v5'),
            'position_lifecycle_ref': ref_map.get('position_lifecycle_snapshot_v2'),
            'nav_ref': ref_map.get('accounting_nav_v2'),
            'capital_authority_allocation_ref': ref_map.get('capital_authority_allocation_v1'),
            'validation_status': 'VALID',
            'cash_ledger_hash': str((ref_map.get('cash_ledger_snapshot_v1') or {}).get('sha256') or ''),
            'positions_hash': str((ref_map.get('positions_snapshot_v5') or {}).get('sha256') or ''),
            'position_lifecycle_hash': str((ref_map.get('position_lifecycle_snapshot_v2') or {}).get('sha256') or ''),
            'accounting_nav_hash': str((ref_map.get('accounting_nav_v2') or {}).get('sha256') or ''),
            'capital_allocation_hash': str((ref_map.get('capital_authority_allocation_v1') or {}).get('sha256') or ''),
            'current_exposure_candidate_hash': ctx.context_hash,
            'build_ref': {'path': str(build_path), 'sha256': build_sha},
            'economic_evaluation_ref': {
                'path': str(build_path),
                'sha256': build_sha,
                'logical_name': 'economic_state_build_v1.economic_evaluation',
            },
            'manifest_ref': str((ctx.repo_root / MANIFEST_REGISTRY_RELPATH).resolve()),
            'dependency_refs': dependency_refs,
            'seal_basis': 'economic closure achieved',
            'sealed': True,
            'sealed_utc': _anchor_utc(ctx.day_utc),
            'constitutional_dependency_declaration': build_artifact_dependency_declaration_v1(
                artifact_type='economic_state_package_v1',
                artifact_class=str(package_contract.get('artifact_class') or '').strip(),
                authority_id='economic_state_package_v1',
                declared_dependency_artifacts=[str(x).strip() for x in package_contract.get('required_upstream_dependencies') or [] if str(x).strip()],
                dependency_refs=package_constitutional_refs,
            ),
            'constitutional_lineage': build_governed_artifact_lineage_v1(
                artifact_type='economic_state_package_v1',
                artifact_version='v1',
                artifact_class=str(package_contract.get('artifact_class') or '').strip(),
                authority_id='economic_state_package_v1',
                producer_id='constellation_2.common.economic_state_authority_v1',
                generated_at_utc=_anchor_utc(ctx.day_utc),
                effective_at_utc=_anchor_utc(ctx.day_utc),
                finality_state=FINALITY_FINALIZED,
                input_artifact_refs=package_constitutional_refs,
                policy_snapshot_refs=[],
                code_version=_git_sha(ctx.repo_root),
                run_id=ctx.context_hash,
            ),
        }
        package_obj['package_hash'] = canonical_hash_for_c2_artifact_v1(package_obj)
        validate_against_repo_schema_v1(package_obj, ctx.repo_root, PACKAGE_SCHEMA_RELPATH)
        package_path = economic_state_package_path_v1(ctx=ctx)
        _write_canonical_json(package_path, package_obj)

    return {'context': ctx, 'manifest': manifest, 'results': results, 'build_path': build_path, 'build_obj': build_obj, 'package_path': package_path, 'package_obj': package_obj}
