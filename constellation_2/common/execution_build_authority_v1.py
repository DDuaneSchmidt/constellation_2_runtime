
from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from constellation_2.common.economic_state_authority_v1 import (
    compute_economic_state_context_hash_v1,
    economic_state_build_path_v1,
    economic_state_package_path_v1,
    resolve_economic_state_context_v1,
)
from constellation_2.common.constitutional_runtime_v1 import (
    FINALITY_FINALIZED,
    FINALITY_PROVISIONAL,
    assert_constitutional_writer_allowed_v1,
    build_artifact_dependency_declaration_v1,
    build_frozen_decision_input_bundle_v1,
    build_governed_artifact_lineage_v1,
    get_constitutional_artifact_contract_v1,
)
from constellation_2.common.execution_identity_authority_v1 import resolve_submission_identity_v1
from constellation_2.common.global_context_authority_v1 import (
    compute_global_context_hash_v1,
    global_context_build_path_v1,
    global_context_package_path_v1,
    resolve_global_context_v1,
)
from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root, resolve_truth_sleeves_root
from constellation_2.common.sleeve_execution_root_v1 import resolve_sleeve_execution_root_v1
from constellation_2.common.trade_submit_readiness_authority_v1 import resolve_governed_account_binding
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from ops.tools.aegis_artifact_ledger_v1 import infer_runtime_root_v1, write_artifact_ledger_record_v1
from ops.tools.aegis_runtime_mode_v1 import runtime_mode_from_truth_root_v1

REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_REGISTRY_RELPATH = 'governance/02_REGISTRIES/C2_EXECUTION_BUILD_MANIFESTS_V1.json'
MANIFEST_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/REPORTS/execution_dependency_manifest.v1.schema.json'
BUILD_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/REPORTS/execution_build.v1.schema.json'
PACKAGE_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_package.v1.schema.json'
BUILD_REPORT_FAMILY = 'execution_build_v1'
PACKAGE_FAMILY = 'execution_package_v1'

STATUS_PRESENT = 'PRESENT'
STATUS_MISSING = 'MISSING'
STATUS_STALE = 'STALE'
STATUS_FAILED = 'FAILED'
STATUS_BLOCKED_BY_UPSTREAM = 'BLOCKED_BY_UPSTREAM'
STATUS_UNOWNED = 'UNOWNED'

CLOSURE_COMPLETE = 'COMPLETE'
CLOSURE_BLOCKED = 'BLOCKED'


@dataclass(frozen=True)
class CandidateContext:
    repo_root: Path
    canonical_truth_root: Path
    truth_sleeves_root: Path
    execution_truth_root: Path
    candidate_path: Path
    candidate_day_dir: Path
    day_utc: str
    sleeve_id: str
    environment: str
    attempt_id: str
    intent_hash: str
    ib_account: str
    plan_path: Path
    plan_obj: Dict[str, Any]
    plan_summary: Dict[str, Any]
    binding_path: Path
    binding_obj: Dict[str, Any]
    binding_schema_version: str
    submit_preflight_path: Path
    submit_preflight_obj: Dict[str, Any]
    execution_identity_path: Path
    execution_identity_obj: Dict[str, Any]
    intent_id: str
    trade_instance_id: str
    submission_id: str
    plan_hash: str
    binding_hash: str
    global_context_hash: str
    global_context_build_path: Path
    global_context_package_path: Path
    economic_context_hash: str
    economic_build_path: Path
    economic_package_path: Path


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


def _read_json_or_empty(path: Path) -> Dict[str, Any]:
    try:
        return _read_json(path)
    except Exception:
        return {}


def _write_canonical_json(path: Path, obj: Dict[str, Any]) -> str:
    payload = json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False) + '\n'
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


def _to_cents(value: Any, *, field_name: str) -> int:
    text = str(value or '').strip()
    if not text:
        raise ValueError(f'EXECUTION_BUILD_PLAN_{field_name}_MISSING')
    try:
        dec = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f'EXECUTION_BUILD_PLAN_{field_name}_INVALID:{text}') from exc
    cents = (dec * Decimal('100')).quantize(Decimal('1'), rounding=ROUND_HALF_UP)
    if cents <= 0:
        raise ValueError(f'EXECUTION_BUILD_PLAN_{field_name}_NOT_POSITIVE:{text}')
    return int(cents)


def _normalize_options_legs(*, plan_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    legs = plan_obj.get('legs')
    if not isinstance(legs, list) or not legs:
        raise ValueError('EXECUTION_BUILD_OPTIONS_LEGS_MISSING')
    normalized: List[Dict[str, Any]] = []
    for idx, leg in enumerate(legs, start=1):
        if not isinstance(leg, dict):
            raise ValueError(f'EXECUTION_BUILD_OPTIONS_LEG_INVALID:index={idx}:reason=NOT_OBJECT')
        action = str(leg.get('action') or '').strip().upper()
        right = str(leg.get('right') or '').strip().upper()
        strike = str(leg.get('strike') or '').strip()
        expiry = str(leg.get('expiry_utc') or leg.get('expiration') or '').strip()
        ratio_raw = leg.get('ratio')
        try:
            ratio = int(ratio_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f'EXECUTION_BUILD_OPTIONS_LEG_INVALID:index={idx}:reason=RATIO_INVALID') from exc
        if action not in {'BUY', 'SELL'}:
            raise ValueError(f'EXECUTION_BUILD_OPTIONS_LEG_INVALID:index={idx}:reason=ACTION_MISSING')
        if right not in {'PUT', 'CALL'}:
            raise ValueError(f'EXECUTION_BUILD_OPTIONS_LEG_INVALID:index={idx}:reason=RIGHT_MISSING')
        if not strike:
            raise ValueError(f'EXECUTION_BUILD_OPTIONS_LEG_INVALID:index={idx}:reason=STRIKE_MISSING')
        if not expiry:
            raise ValueError(f'EXECUTION_BUILD_OPTIONS_LEG_INVALID:index={idx}:reason=EXPIRY_MISSING')
        if ratio <= 0:
            raise ValueError(f'EXECUTION_BUILD_OPTIONS_LEG_INVALID:index={idx}:reason=RATIO_NOT_POSITIVE')
        normalized_leg: Dict[str, Any] = {
            'action': action,
            'right': right,
            'strike': strike,
            'expiration': expiry,
            'ratio': ratio,
        }
        conid = leg.get('ib_conId')
        if conid is None:
            conid = leg.get('conId')
        if conid is not None:
            normalized_leg['conId'] = conid
        local_symbol = str(leg.get('ib_localSymbol') or leg.get('local_symbol') or '').strip()
        if local_symbol:
            normalized_leg['local_symbol'] = local_symbol
        for key in ('bid', 'ask', 'mid'):
            if key in leg and leg.get(key) is not None:
                normalized_leg[key] = leg.get(key)
        normalized.append(normalized_leg)
    return normalized


def _build_plan_summary(*, plan_obj: Dict[str, Any], plan_path: Path) -> Dict[str, Any]:
    schema_id = str(plan_obj.get('schema_id') or '').strip()
    schema_version = str(plan_obj.get('schema_version') or '').strip()
    order_terms = plan_obj.get('order_terms')
    order_terms_obj = order_terms if isinstance(order_terms, dict) else {}
    order_type = str(order_terms_obj.get('order_type') or '').strip().upper()
    if schema_id == 'equity_order_plan':
        quantity_raw = plan_obj.get('qty_shares')
        if quantity_raw is None:
            quantity_raw = plan_obj.get('quantity')
        try:
            quantity = int(quantity_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError('EXECUTION_BUILD_EQUITY_QUANTITY_INVALID') from exc
        if quantity <= 0:
            raise ValueError('EXECUTION_BUILD_EQUITY_QUANTITY_NOT_POSITIVE')
        symbol = str(plan_obj.get('symbol') or '').strip().upper()
        if not symbol:
            raise ValueError('EXECUTION_BUILD_EQUITY_SYMBOL_MISSING')
        return {
            'plan_kind': 'EQUITY',
            'schema_id': schema_id,
            'schema_version': schema_version,
            'exposure_type': str(plan_obj.get('structure') or '').strip() or 'EQUITY',
            'symbol': symbol,
            'underlying': {'symbol': symbol, 'currency': str(plan_obj.get('currency') or '').strip() or 'USD'},
            'quantity': quantity,
            'order_type': order_type,
            'broker_order': copy.deepcopy(order_terms_obj),
            'protective_stop': copy.deepcopy(plan_obj.get('protective_stop')),
            'take_profit': copy.deepcopy(plan_obj.get('take_profit')),
            'bracket': copy.deepcopy(plan_obj.get('bracket')),
            'risk_contract_ref': copy.deepcopy(plan_obj.get('risk_contract_ref')),
            'legs': None,
            'defined_risk_proven': None,
            'max_defined_loss_cents': None,
            'risk_per_unit_cents': None,
            'required_risk_cents': None,
            'evidence_artifacts': [str(plan_path)],
        }
    if schema_id != 'order_plan':
        raise ValueError(f'EXECUTION_BUILD_PLAN_SCHEMA_UNSUPPORTED:schema_id={schema_id or "MISSING"}')
    legs = _normalize_options_legs(plan_obj=plan_obj)
    underlying = plan_obj.get('underlying')
    underlying_obj = underlying if isinstance(underlying, dict) else {}
    symbol = str(underlying_obj.get('symbol') or plan_obj.get('symbol') or '').strip().upper()
    if not symbol:
        raise ValueError('EXECUTION_BUILD_OPTIONS_UNDERLYING_SYMBOL_MISSING')
    risk_proof = plan_obj.get('risk_proof')
    risk_proof_obj = risk_proof if isinstance(risk_proof, dict) else {}
    if risk_proof_obj.get('defined_risk_proven') is not True:
        raise ValueError('EXECUTION_BUILD_OPTIONS_DEFINED_RISK_NOT_PROVEN')
    max_loss_cents = _to_cents(risk_proof_obj.get('max_loss_usd'), field_name='MAX_LOSS_USD')
    contracts_raw = risk_proof_obj.get('contracts')
    try:
        contracts = int(contracts_raw)
    except (TypeError, ValueError) as exc:
        raise ValueError('EXECUTION_BUILD_OPTIONS_CONTRACTS_INVALID') from exc
    if contracts <= 0:
        raise ValueError('EXECUTION_BUILD_OPTIONS_CONTRACTS_NOT_POSITIVE')
    if max_loss_cents % contracts != 0:
        raise ValueError('EXECUTION_BUILD_OPTIONS_RISK_PER_UNIT_NOT_DERIVABLE')
    risk_per_unit_cents = max_loss_cents // contracts
    return {
        'plan_kind': 'OPTIONS',
        'schema_id': schema_id,
        'schema_version': schema_version,
        'exposure_type': str(plan_obj.get('structure') or '').strip() or 'OPTIONS',
        'symbol': symbol,
        'underlying': {'symbol': symbol, 'currency': str(underlying_obj.get('currency') or '').strip() or 'USD'},
        'quantity': contracts,
        'order_type': order_type,
        'broker_order': copy.deepcopy(order_terms_obj),
        'legs': legs,
        'risk_contract_ref': copy.deepcopy(plan_obj.get('risk_contract_ref')),
        'defined_risk_proven': True,
        'max_defined_loss_cents': max_loss_cents,
        'risk_per_unit_cents': risk_per_unit_cents,
        'required_risk_cents': risk_per_unit_cents * contracts,
        'evidence_artifacts': [str(plan_path)],
    }


def _resolve_order_plan(candidate_path: Path) -> tuple[Path, Dict[str, Any], Dict[str, Any]]:
    candidate_path = candidate_path.resolve()
    plan_candidates = (
        'equity_order_plan.v2.json',
        'equity_order_plan.v1.json',
        'order_plan.v1.json',
    )
    for filename in plan_candidates:
        path = (candidate_path / filename).resolve()
        if not path.exists():
            continue
        plan_obj = _read_json(path)
        plan_summary = _build_plan_summary(plan_obj=plan_obj, plan_path=path)
        return path, plan_obj, plan_summary
    raise ValueError(f'EXECUTION_BUILD_PLAN_MISSING:path={candidate_path}')


def _require_hex64_field(*, obj: Dict[str, Any], field_name: str, error_prefix: str) -> str:
    value = str(obj.get(field_name) or '').strip().lower()
    if len(value) != 64 or any(ch not in '0123456789abcdef' for ch in value):
        raise ValueError(f'{error_prefix}_{field_name.upper()}_INVALID_OR_MISSING')
    return value


def _resolve_binding_record(
    *,
    candidate_path: Path,
    day_utc: str,
    environment: str,
    intent_hash: str,
) -> tuple[Path, Dict[str, Any], str]:
    candidate_path = candidate_path.resolve()
    binding_candidates = (
        ('binding_record.v2.json', 'v2'),
        ('binding_record.v1.json', 'v1'),
    )
    for filename, expected_version in binding_candidates:
        path = (candidate_path / filename).resolve()
        if not path.exists():
            continue
        obj = _read_json(path)
        schema_id = str(obj.get('schema_id') or '').strip()
        schema_version = str(obj.get('schema_version') or '').strip().lower()
        if schema_id != 'binding_record':
            raise ValueError(f'EXECUTION_BUILD_BINDING_SCHEMA_ID_INVALID:path={path}:schema_id={schema_id or "MISSING"}')
        if schema_version != expected_version:
            raise ValueError(
                f'EXECUTION_BUILD_BINDING_SCHEMA_VERSION_MISMATCH:path={path}:expected={expected_version}:actual={schema_version or "MISSING"}'
            )
        _require_hex64_field(obj=obj, field_name='plan_hash', error_prefix='EXECUTION_BUILD_BINDING')
        _require_hex64_field(obj=obj, field_name='mapping_ledger_hash', error_prefix='EXECUTION_BUILD_BINDING')
        row_day = str(obj.get('day_utc') or '').strip()
        if row_day and row_day != day_utc:
            raise ValueError(f'EXECUTION_BUILD_BINDING_DAY_MISMATCH:expected={day_utc}:actual={row_day}:path={path}')
        row_env = str(obj.get('environment') or '').strip().upper()
        if row_env and row_env != environment:
            raise ValueError(f'EXECUTION_BUILD_BINDING_ENVIRONMENT_MISMATCH:expected={environment}:actual={row_env}:path={path}')
        if expected_version == 'v2':
            record_intent_hash = str(obj.get('intent_hash') or '').strip().lower()
            if not record_intent_hash:
                raise ValueError(f'EXECUTION_BUILD_BINDING_INTENT_HASH_MISSING:path={path}')
            if record_intent_hash != intent_hash.lower():
                raise ValueError(
                    f'EXECUTION_BUILD_BINDING_INTENT_HASH_MISMATCH:expected={intent_hash.lower()}:actual={record_intent_hash}:path={path}'
                )
        elif 'intent_hash' in obj and str(obj.get('intent_hash') or '').strip().lower() not in {'', intent_hash.lower()}:
            raise ValueError(
                f'EXECUTION_BUILD_BINDING_INTENT_HASH_MISMATCH:expected={intent_hash.lower()}:actual={str(obj.get("intent_hash")).strip().lower()}:path={path}'
            )
        return path, obj, expected_version
    raise ValueError(f'EXECUTION_BUILD_BINDING_RECORD_MISSING:path={candidate_path}')


def _load_manifest(repo_root: Path, operation_type: str) -> Dict[str, Any]:
    registry_path = (repo_root / MANIFEST_REGISTRY_RELPATH).resolve()
    registry = _read_json(registry_path)
    manifests = registry.get('manifests')
    if not isinstance(manifests, dict):
        raise ValueError('EXECUTION_BUILD_MANIFEST_REGISTRY_INVALID')
    manifest = manifests.get(operation_type)
    if not isinstance(manifest, dict):
        raise ValueError(f'EXECUTION_BUILD_MANIFEST_NOT_FOUND:operation_type={operation_type}')
    out = {'schema_id': 'execution_dependency_manifest', 'schema_version': 'v1', **manifest}
    validate_against_repo_schema_v1(out, repo_root, MANIFEST_SCHEMA_RELPATH)
    return out


def resolve_candidate_context(*, repo_root: Path, candidate_path: Path) -> CandidateContext:
    repo_root = repo_root.resolve()
    truth_sleeves_root = resolve_truth_sleeves_root().resolve()
    canonical_truth_root = resolve_canonical_truth_root().resolve()
    candidate_path = candidate_path.resolve()
    rel = candidate_path.relative_to(truth_sleeves_root)
    parts = rel.parts
    if len(parts) < 6 or parts[2] != 'phaseC_preflight_v1':
        raise ValueError(f'EXECUTION_BUILD_INVALID_CANDIDATE_PATH:path={candidate_path}')

    sleeve_id = str(parts[0]).strip().upper()
    environment = str(parts[1]).strip().upper()
    day_utc = str(parts[3]).strip()
    attempt_part = str(parts[4]).strip()
    if not attempt_part.startswith('attempt_'):
        raise ValueError(f'EXECUTION_BUILD_INVALID_ATTEMPT_PATH:path={candidate_path}')
    attempt_id = attempt_part[len('attempt_'):]
    intent_hash = str(parts[5]).strip()

    account_binding = resolve_governed_account_binding(
        repo_root=repo_root,
        environment=environment,
        requested_ib_account='',
        sleeve_id=sleeve_id,
    )
    execution_root = resolve_sleeve_execution_root_v1(
        repo_root=repo_root,
        environment=environment,
        ib_account=account_binding.ib_account,
        sleeve_id=sleeve_id,
    )
    execution_truth_root = execution_root.execution_root_path.resolve()
    candidate_day_dir = (execution_truth_root / 'phaseC_preflight_v1' / day_utc).resolve()

    plan_path, plan_obj, plan_summary = _resolve_order_plan(candidate_path)
    binding_path, binding_obj, binding_schema_version = _resolve_binding_record(
        candidate_path=candidate_path,
        day_utc=day_utc,
        environment=environment,
        intent_hash=intent_hash,
    )
    submit_preflight_path = (candidate_path / 'submit_preflight_decision.v1.json').resolve()
    execution_identity_path = (candidate_path / 'execution_identity_record.v1.json').resolve()

    submit_preflight_obj = _read_json(submit_preflight_path)
    execution_identity_obj = _read_json(execution_identity_path)

    binding_hash = canonical_hash_for_c2_artifact_v1(binding_obj)
    resolved_submission = resolve_submission_identity_v1(
        plan_obj=plan_obj,
        binding_obj=binding_obj,
        binding_hash=binding_hash,
        execution_identity_obj=execution_identity_obj,
    )
    intent_id = str(execution_identity_obj.get('intent_id') or binding_obj.get('intent_id') or plan_obj.get('source_intent_id') or '').strip()
    trade_instance_id = str(execution_identity_obj.get('trade_instance_id') or binding_obj.get('trade_instance_id') or '').strip()
    global_context_hash = compute_global_context_hash_v1(
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=account_binding.ib_account,
        operation_type='fresh_paper_entry_v1',
    )
    global_ctx = resolve_global_context_v1(
        repo_root=repo_root,
        operation_type='fresh_paper_entry_v1',
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=account_binding.ib_account,
    )
    economic_context_hash = compute_economic_state_context_hash_v1(
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=account_binding.ib_account,
        operation_type='fresh_paper_entry_v1',
    )
    econ_ctx = resolve_economic_state_context_v1(
        repo_root=repo_root,
        operation_type='fresh_paper_entry_v1',
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        ib_account=account_binding.ib_account,
    )

    return CandidateContext(
        repo_root=repo_root,
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
        execution_truth_root=execution_truth_root,
        candidate_path=candidate_path,
        candidate_day_dir=candidate_day_dir,
        day_utc=day_utc,
        sleeve_id=sleeve_id,
        environment=environment,
        attempt_id=attempt_id,
        intent_hash=intent_hash,
        ib_account=account_binding.ib_account,
        plan_path=plan_path,
        plan_obj=plan_obj,
        plan_summary=plan_summary,
        binding_path=binding_path,
        binding_obj=binding_obj,
        binding_schema_version=binding_schema_version,
        submit_preflight_path=submit_preflight_path,
        submit_preflight_obj=submit_preflight_obj,
        execution_identity_path=execution_identity_path,
        execution_identity_obj=execution_identity_obj,
        intent_id=intent_id,
        trade_instance_id=trade_instance_id,
        submission_id=resolved_submission.submission_id,
        plan_hash=resolved_submission.plan_hash,
        binding_hash=binding_hash,
        global_context_hash=global_context_hash,
        global_context_build_path=global_context_build_path_v1(ctx=global_ctx),
        global_context_package_path=global_context_package_path_v1(ctx=global_ctx),
        economic_context_hash=economic_context_hash,
        economic_build_path=economic_state_build_path_v1(ctx=econ_ctx),
        economic_package_path=economic_state_package_path_v1(ctx=econ_ctx),
    )


def _dependency_path(ctx: CandidateContext, dependency: Dict[str, Any]) -> Path:
    dep_id = str(dependency.get('dependency_id') or '').strip()
    if dep_id == 'equity_order_plan_v2':
        return ctx.plan_path
    if dep_id == 'binding_record_v2':
        return ctx.binding_path
    pattern = str(dependency.get('path_pattern') or '').strip()
    values = {
        'candidate_path': str(ctx.candidate_path),
        'candidate_day_dir': str(ctx.candidate_day_dir),
        'canonical_truth_root': str(ctx.canonical_truth_root),
        'execution_truth_root': str(ctx.execution_truth_root),
        'day_utc': ctx.day_utc,
        'attempt_id': ctx.attempt_id,
        'intent_hash': ctx.intent_hash,
        'intent_id': ctx.intent_id,
        'trade_instance_id': ctx.trade_instance_id,
        'submission_id': ctx.submission_id,
        'ib_account': ctx.ib_account,
        'sleeve_id': ctx.sleeve_id,
        'environment': ctx.environment,
        'global_context_hash': ctx.global_context_hash,
        'economic_context_hash': ctx.economic_context_hash,
    }
    return Path(pattern.format(**values)).resolve()


def _present_status_ok(value: str) -> bool:
    return value in {'PASS', 'OK', 'BOOTSTRAP_PASS', 'AUTHORIZED', 'SUCCESS', 'READY_NOW', 'ENABLED', 'OPEN'}


def _bundle_b_authorized_row_for_intent(
    alloc_obj: Dict[str, Any],
    intent_hash: str,
    intent_id: str,
    fallback_intent_ids: Sequence[str] | None = None,
) -> Dict[str, Any]:
    decision_chain = alloc_obj.get('decision_chain')
    if not isinstance(decision_chain, dict):
        raise ValueError('CAPITAL_AUTHORITY_DECISION_CHAIN_INVALID')
    rows = decision_chain.get('authorized_trade_intents')
    if not isinstance(rows, list):
        raise ValueError('CAPITAL_AUTHORITY_AUTHORIZED_TRADE_INTENTS_INVALID')
    allowed_fallback_ids = {str(intent_id or '').strip()}
    for value in (fallback_intent_ids or []):
        v = str(value or '').strip()
        if v:
            allowed_fallback_ids.add(v)
    fallback_by_intent_id: Dict[str, Any] | None = None
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get('intent_hash') or '').strip() == intent_hash:
            return row
        if allowed_fallback_ids and str(row.get('intent_id') or '').strip() in allowed_fallback_ids and fallback_by_intent_id is None:
            fallback_by_intent_id = row
    if fallback_by_intent_id is not None:
        return fallback_by_intent_id
    raise ValueError(f'CAPITAL_AUTHORITY_INTENT_HASH_MISSING:intent_hash={intent_hash}:intent_id={intent_id}')


def _int_or_zero(value: Any, *, field_name: str) -> int:
    if value is None:
        return 0
    if isinstance(value, str) and not value.strip():
        return 0
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f'EXECUTION_PACKAGE_{field_name}_INVALID:{value}') from exc


def _resolve_package_authorization_row(
    *,
    ctx: CandidateContext,
    ref_map: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    allocation_ref = ref_map.get('capital_authority_allocation_v1')
    if not isinstance(allocation_ref, dict):
        raise ValueError('EXECUTION_PACKAGE_CAPITAL_AUTHORITY_REF_MISSING')
    allocation_path_text = str(allocation_ref.get('path') or '').strip()
    if not allocation_path_text:
        raise ValueError('EXECUTION_PACKAGE_CAPITAL_AUTHORITY_PATH_MISSING')
    allocation_path = Path(allocation_path_text).resolve()
    alloc_obj = _read_json(allocation_path)
    row = _bundle_b_authorized_row_for_intent(
        alloc_obj,
        ctx.intent_hash,
        ctx.intent_id,
        fallback_intent_ids=[str(ctx.plan_obj.get('source_intent_id') or '').strip()],
    )
    outcome = str(row.get('authorization_outcome') or '').strip().upper()
    authorized_quantity = _int_or_zero(row.get('authorized_quantity'), field_name='AUTHORIZED_QUANTITY')
    if outcome not in {'APPROVED', 'RESIZED'} or authorized_quantity <= 0:
        raise ValueError(
            f'EXECUTION_PACKAGE_AUTHORIZED_QUANTITY_INVALID:outcome={outcome or "MISSING"}:authorized_quantity={authorized_quantity}'
        )
    return row


def _source_exposure_intent_obj(ctx: CandidateContext) -> Dict[str, Any] | None:
    candidates = [
        ctx.execution_truth_root / 'intents_v1' / 'snapshots' / ctx.day_utc / f'{ctx.intent_hash}.exposure_intent.v1.json',
        ctx.canonical_truth_root / 'intents_v1' / 'snapshots' / ctx.day_utc / f'{ctx.intent_hash}.exposure_intent.v1.json',
    ]
    for path in candidates:
        if path.exists() and path.is_file():
            obj = _read_json(path)
            if str(obj.get('intent_id') or '').strip() in {ctx.intent_id, str(ctx.plan_obj.get('source_intent_id') or '').strip()}:
                return obj
    return None


def _int_positive_or_none(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _price_decimal(value: Any) -> Decimal | None:
    text = str(value or '').strip()
    if not text:
        return None
    try:
        parsed = Decimal(text)
    except InvalidOperation:
        return None
    return parsed if parsed > 0 else None


def _price_text(value: Decimal) -> str:
    return str(value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))


def _expected_stop_price_text(*, entry_price: Decimal, action: str, stop_loss_bps: int) -> str | None:
    side = str(action or '').strip().upper()
    distance = Decimal(stop_loss_bps) / Decimal('10000')
    if side == 'BUY':
        return _price_text(entry_price * (Decimal('1') - distance))
    if side == 'SELL':
        return _price_text(entry_price * (Decimal('1') + distance))
    return None


def _candidate_equity_stop(ctx: CandidateContext) -> Dict[str, Any]:
    path = (ctx.candidate_path / 'equity_intent.v1.json').resolve()
    if not path.exists() or not path.is_file():
        return {}
    obj = _read_json(path)
    exit_policy = obj.get('exit_policy') if isinstance(obj.get('exit_policy'), dict) else {}
    stop = exit_policy.get('protective_stop') if isinstance(exit_policy.get('protective_stop'), dict) else {}
    return copy.deepcopy(stop)


def _validate_equity_protective_stop(ctx: CandidateContext) -> tuple[bool, str]:
    contract_ref = ctx.plan_summary.get('risk_contract_ref') if isinstance(ctx.plan_summary.get('risk_contract_ref'), dict) else {}
    contract_path = Path(str(contract_ref.get('path') or '')).resolve() if str(contract_ref.get('path') or '').strip() else Path()
    if not str(contract_ref.get('path') or '').strip() or not contract_path.exists():
        return False, 'EXECUTION_BUILD_RISK_DEFINITION_CONTRACT_MISSING'
    contract = _read_json(contract_path)
    if str(contract.get('validation_status') or '').strip().upper() != 'PASS':
        return False, 'EXECUTION_BUILD_RISK_DEFINITION_CONTRACT_NOT_PASS'
    if str(contract.get('risk_type') or '').strip().upper() != 'STOP_BASED':
        return False, 'EXECUTION_BUILD_RISK_DEFINITION_CONTRACT_TYPE_MISMATCH'
    if str(contract.get('intent_id') or '').strip() != ctx.intent_id:
        return False, 'EXECUTION_BUILD_RISK_DEFINITION_CONTRACT_INTENT_MISMATCH'
    if str(contract.get('day_utc') or '').strip() != ctx.day_utc:
        return False, 'EXECUTION_BUILD_RISK_DEFINITION_CONTRACT_DAY_MISMATCH'
    source_bps = _int_positive_or_none(contract.get('stop_loss_bps'))
    if source_bps is None:
        return False, 'EXECUTION_BUILD_RISK_DEFINITION_CONTRACT_STOP_BPS_MISSING'

    plan_stop = ctx.plan_obj.get('protective_stop') if isinstance(ctx.plan_obj.get('protective_stop'), dict) else {}
    if not plan_stop:
        return False, f'EXECUTION_BUILD_EQUITY_PROTECTIVE_STOP_MISSING:source_stop_loss_bps={source_bps}'
    plan_bps = _int_positive_or_none(plan_stop.get('stop_loss_bps'))
    if plan_bps != source_bps:
        return False, f'EXECUTION_BUILD_EQUITY_STOP_LOSS_BPS_CONFLICT:source={source_bps}:plan={plan_bps or "MISSING"}'
    if str(plan_stop.get('order_type') or '').strip().upper() != 'STOP':
        return False, 'EXECUTION_BUILD_EQUITY_PROTECTIVE_STOP_ORDER_TYPE_INVALID'
    if str(plan_stop.get('basis') or '').strip() != 'ENTRY_REFERENCE_PRICE':
        return False, 'EXECUTION_BUILD_EQUITY_PROTECTIVE_STOP_BASIS_INVALID'
    stop_price = _price_decimal(plan_stop.get('stop_price'))
    if stop_price is None:
        return False, 'EXECUTION_BUILD_EQUITY_PROTECTIVE_STOP_PRICE_MISSING'

    order_terms = ctx.plan_obj.get('order_terms') if isinstance(ctx.plan_obj.get('order_terms'), dict) else {}
    entry_price = _price_decimal(order_terms.get('limit_price')) if str(order_terms.get('order_type') or '').strip().upper() == 'LIMIT' else None
    if entry_price is None:
        return False, 'EXECUTION_BUILD_EQUITY_STOP_REFERENCE_PRICE_MISSING'
    expected_stop = _expected_stop_price_text(entry_price=entry_price, action=str(ctx.plan_obj.get('action') or ''), stop_loss_bps=source_bps)
    if expected_stop is None:
        return False, 'EXECUTION_BUILD_EQUITY_STOP_ACTION_UNSUPPORTED'
    if _price_text(stop_price) != expected_stop:
        return False, f'EXECUTION_BUILD_EQUITY_STOP_PRICE_CONFLICT:expected={expected_stop}:plan={_price_text(stop_price)}'

    candidate_stop = _candidate_equity_stop(ctx)
    if candidate_stop:
        candidate_bps = _int_positive_or_none(candidate_stop.get('stop_loss_bps'))
        if candidate_bps != source_bps:
            return False, f'EXECUTION_BUILD_EQUITY_PHASEC_STOP_LOSS_BPS_CONFLICT:source={source_bps}:phasec={candidate_bps or "MISSING"}'
        candidate_price = _price_decimal(candidate_stop.get('stop_price'))
        if candidate_price is not None and _price_text(candidate_price) != _price_text(stop_price):
            return False, f'EXECUTION_BUILD_EQUITY_PHASEC_STOP_PRICE_CONFLICT:phasec={_price_text(candidate_price)}:plan={_price_text(stop_price)}'
    return True, 'EQUITY_PROTECTIVE_STOP_MATCHES_SOURCE'


def _evaluate_semantics(*, dependency_id: str, obj: Dict[str, Any], path: Path, ctx: CandidateContext) -> tuple[str, str]:
    day_value = _day_value(obj)
    if day_value and day_value != ctx.day_utc:
        return STATUS_STALE, f'DAY_MISMATCH:expected={ctx.day_utc}:actual={day_value}:path={path}'

    if dependency_id == 'attempt_state_v1':
        state = str(obj.get('status') or '').strip().upper()
        if state == 'ACTIVE':
            return STATUS_PRESENT, 'ACTIVE'
        return STATUS_FAILED, f'ATTEMPT_NOT_ACTIVE:status={state or "MISSING"}:path={path}'

    if dependency_id == 'submit_preflight_decision_v1':
        decision = str(obj.get('decision') or '').strip().upper()
        if decision == 'ALLOW':
            return STATUS_PRESENT, 'ALLOW'
        return STATUS_FAILED, f'PREFLIGHT_NOT_ALLOW:decision={decision or "MISSING"}:path={path}'

    if dependency_id == 'binding_record_v2':
        if Path(path).resolve() != ctx.binding_path:
            return STATUS_STALE, f'BINDING_PATH_MISMATCH:expected={ctx.binding_path}:actual={path}'
        if ctx.binding_schema_version == 'v1':
            if ctx.submission_id != canonical_hash_for_c2_artifact_v1(ctx.binding_obj):
                return STATUS_FAILED, f'BINDING_V1_LEGACY_SUBMISSION_ID_MISMATCH:path={path}'
            return STATUS_PRESENT, 'BINDING_PRESENT:VERSION_V1_LEGACY'
        if str(obj.get('submission_id') or '').strip() == ctx.submission_id:
            return STATUS_PRESENT, 'BINDING_PRESENT:VERSION_V2_SUBMISSION_ID_MATCH'
        return STATUS_FAILED, f'BINDING_SUBMISSION_ID_MISMATCH:path={path}'

    if dependency_id == 'execution_identity_record_v1':
        if str(obj.get('submission_id') or '').strip() != ctx.submission_id:
            return STATUS_FAILED, f'EXECUTION_IDENTITY_SUBMISSION_ID_MISMATCH:path={path}'
        if str(obj.get('trade_instance_id') or '').strip() != ctx.trade_instance_id:
            return STATUS_FAILED, f'EXECUTION_IDENTITY_TRADE_INSTANCE_ID_MISMATCH:path={path}'
        return STATUS_PRESENT, 'EXECUTION_IDENTITY_MATCH'

    if dependency_id == 'equity_order_plan_v2':
        plan_kind = str(ctx.plan_summary.get('plan_kind') or '').strip().upper()
        if not plan_kind:
            return STATUS_FAILED, f'PLAN_KIND_MISSING:path={path}'
        if Path(path).resolve() != ctx.plan_path:
            return STATUS_STALE, f'PLAN_PATH_MISMATCH:expected={ctx.plan_path}:actual={path}'
        if plan_kind == 'EQUITY':
            stop_ok, stop_detail = _validate_equity_protective_stop(ctx)
            if not stop_ok:
                return STATUS_FAILED, f'{stop_detail}:path={path}'
        if plan_kind == 'OPTIONS':
            contract_ref = ctx.plan_summary.get('risk_contract_ref') if isinstance(ctx.plan_summary.get('risk_contract_ref'), dict) else {}
            if not str(contract_ref.get('path') or '').strip():
                return STATUS_FAILED, f'OPTIONS_RISK_DEFINITION_CONTRACT_REF_MISSING:path={path}'
            if ctx.plan_summary.get('defined_risk_proven') is not True:
                return STATUS_FAILED, f'OPTIONS_DEFINED_RISK_NOT_PROVEN:path={path}'
            legs = ctx.plan_summary.get('legs')
            if not isinstance(legs, list) or not legs:
                return STATUS_FAILED, f'OPTIONS_LEGS_MISSING:path={path}'
            if int(ctx.plan_summary.get('max_defined_loss_cents') or 0) <= 0:
                return STATUS_FAILED, f'OPTIONS_MAX_LOSS_MISSING:path={path}'
            if int(ctx.plan_summary.get('risk_per_unit_cents') or 0) <= 0:
                return STATUS_FAILED, f'OPTIONS_RISK_PER_UNIT_MISSING:path={path}'
        return STATUS_PRESENT, f'PLAN_PRESENT:{plan_kind}'

    if dependency_id == 'risk_definition_contract_v1':
        if str(obj.get('schema_id') or '').strip() != 'risk_definition_contract_v1':
            return STATUS_FAILED, f'RISK_CONTRACT_SCHEMA_ID_INVALID:path={path}'
        if str(obj.get('validation_status') or '').strip().upper() != 'PASS':
            return STATUS_FAILED, f'RISK_CONTRACT_NOT_PASS:blockers={obj.get("blockers")}:path={path}'
        if str(obj.get('intent_id') or '').strip() != ctx.intent_id:
            return STATUS_FAILED, f'RISK_CONTRACT_INTENT_MISMATCH:path={path}'
        if str(obj.get('day_utc') or '').strip() != ctx.day_utc:
            return STATUS_STALE, f'RISK_CONTRACT_DAY_MISMATCH:path={path}'
        plan_kind = str(ctx.plan_summary.get('plan_kind') or '').strip().upper()
        risk_type = str(obj.get('risk_type') or '').strip().upper()
        if plan_kind == 'EQUITY' and risk_type != 'STOP_BASED':
            return STATUS_FAILED, f'RISK_CONTRACT_TYPE_MISMATCH:expected=STOP_BASED:actual={risk_type or "MISSING"}:path={path}'
        if plan_kind == 'OPTIONS' and risk_type != 'DEFINED_RISK':
            return STATUS_FAILED, f'RISK_CONTRACT_TYPE_MISMATCH:expected=DEFINED_RISK:actual={risk_type or "MISSING"}:path={path}'
        contract_ref = ctx.plan_summary.get('risk_contract_ref') if isinstance(ctx.plan_summary.get('risk_contract_ref'), dict) else {}
        if str(contract_ref.get('path') or '').strip() and Path(str(contract_ref.get('path'))).resolve() != Path(path).resolve():
            return STATUS_STALE, f'RISK_CONTRACT_PLAN_REF_MISMATCH:path={path}:plan_ref={contract_ref.get("path")}'
        return STATUS_PRESENT, 'RISK_DEFINITION_CONTRACT_PASS'

    if dependency_id == 'global_context_package_v1':
        sealed = bool(obj.get('sealed') is True)
        mode = str(obj.get('mode') or '').strip().upper()
        sleeve_id = str(obj.get('sleeve_id') or '').strip().upper()
        account_id = str(obj.get('account_id') or '').strip().upper()
        op_type = str(obj.get('operation_type') or '').strip()
        context_hash = str(obj.get('context_hash') or '').strip()
        if not sealed:
            return STATUS_FAILED, f'GLOBAL_CONTEXT_PACKAGE_UNSEALED:path={path}'
        if mode != ctx.environment or sleeve_id != ctx.sleeve_id or account_id != ctx.ib_account.upper() or op_type != 'fresh_paper_entry_v1' or context_hash != ctx.global_context_hash:
            return STATUS_STALE, f'GLOBAL_CONTEXT_PACKAGE_CONTEXT_MISMATCH:path={path}'
        return STATUS_PRESENT, 'GLOBAL_CONTEXT_PACKAGE_SEALED'

    if dependency_id == 'economic_state_package_v1':
        sealed = bool(obj.get('sealed') is True)
        mode = str(obj.get('mode') or '').strip().upper()
        sleeve_id = str(obj.get('sleeve_id') or '').strip().upper()
        account_id = str(obj.get('account_id') or '').strip().upper()
        op_type = str(obj.get('operation_type') or '').strip()
        context_hash = str(obj.get('context_hash') or '').strip()
        lineage = obj.get('global_context_package_ref') if isinstance(obj.get('global_context_package_ref'), dict) else {}
        if not sealed:
            return STATUS_FAILED, f'ECONOMIC_STATE_PACKAGE_UNSEALED:path={path}'
        if mode != ctx.environment or sleeve_id != ctx.sleeve_id or account_id != ctx.ib_account.upper() or op_type != 'fresh_paper_entry_v1' or context_hash != ctx.economic_context_hash:
            return STATUS_STALE, f'ECONOMIC_STATE_PACKAGE_CONTEXT_MISMATCH:path={path}'
        lineage_path = str(lineage.get('path') or '').strip()
        if lineage_path and Path(lineage_path).resolve() != ctx.global_context_package_path:
            return STATUS_STALE, f'ECONOMIC_STATE_PACKAGE_GLOBAL_CONTEXT_REF_MISMATCH:path={path}'
        return STATUS_PRESENT, 'ECONOMIC_STATE_PACKAGE_SEALED'

    if dependency_id == 'global_kill_switch_state_v1':
        state = str(obj.get('state') or '').strip().upper()
        allow_entries = bool(obj.get('allow_entries') is True)
        if state == 'INACTIVE' and allow_entries:
            return STATUS_PRESENT, 'KILL_SWITCH_INACTIVE'
        return STATUS_FAILED, f'KILL_SWITCH_ACTIVE:state={state}:allow_entries={allow_entries}:path={path}'

    if dependency_id == 'ib_api_handshake_v1':
        status = str(obj.get('status') or '').strip().upper()
        ok = bool(obj.get('ok') is True)
        if ok and status in {'OK', 'PASS', 'READY', 'CONNECTED'}:
            return STATUS_PRESENT, 'HANDSHAKE_OK'
        return STATUS_FAILED, f'IB_API_HANDSHAKE_NOT_OK:status={status}:ok={ok}:path={path}'

    if dependency_id == 'trade_submit_readiness_c2_v1':
        status = str(obj.get('state') or '').strip().upper()
        ok = bool(obj.get('ok') is True)
        provenance = obj.get('provenance') if isinstance(obj.get('provenance'), dict) else {}
        if str(provenance.get('truth_root') or '').strip() != str(ctx.execution_truth_root):
            return STATUS_STALE, f'READINESS_TRUTH_ROOT_MISMATCH:path={path}'
        if ok and status == 'OK':
            return STATUS_PRESENT, 'READINESS_OK'
        return STATUS_FAILED, f'TRADE_SUBMIT_READINESS_NOT_OK:state={status}:ok={ok}:path={path}'

    if dependency_id == 'engine_activity_authorization_v1':
        auth = obj.get('authorization') if isinstance(obj.get('authorization'), dict) else {}
        status = str(obj.get('status') or '').strip().upper()
        decision = str(auth.get('decision') or '').strip().upper()
        qty = int(auth.get('authorized_quantity') or 0)
        schema_id = str(obj.get('schema_id') or '').strip()
        if schema_id == 'engine_activity_authorization':
            validation_status = str(obj.get('validation_status') or '').strip().upper()
            auth_status = str(obj.get('authorization_status') or '').strip().upper()
            if str(obj.get('day_utc') or '').strip() != ctx.day_utc:
                return STATUS_STALE, f'ENGINE_ACTIVITY_AUTHORIZATION_WRONG_DAY:path={path}'
            runtime_hash = str((_read_json_or_empty(ctx.canonical_truth_root / 'reports' / 'aegis_runtime_truth_kernel_v1' / ctx.day_utc / 'runtime_evaluation.v1.json')).get('deterministic_output_hash') or '').strip()
            if runtime_hash and str(obj.get('runtime_evaluation_hash') or '').strip() != runtime_hash:
                return STATUS_STALE, f'ENGINE_ACTIVITY_AUTHORIZATION_HASH_MISMATCH:expected_runtime={runtime_hash}:actual_runtime={obj.get("runtime_evaluation_hash")}:path={path}'
            identity_path = (ctx.canonical_truth_root / 'reports' / 'candidate_identity_set_v1' / ctx.day_utc / ctx.intent_hash.lower() / 'candidate_identity_set.v1.json').resolve()
            identity_hash = _sha256_file(identity_path) if identity_path.exists() else ''
            if identity_hash and str(obj.get('candidate_identity_hash') or '').strip() != identity_hash:
                return STATUS_STALE, f'ENGINE_ACTIVITY_AUTHORIZATION_HASH_MISMATCH:dependency=candidate_identity_set_v1:path={path}'
            if str(obj.get('intent_hash') or '').strip().lower() != ctx.intent_hash.lower():
                return STATUS_FAILED, f'ENGINE_ACTIVITY_AUTHORIZATION_INTENT_MISMATCH:path={path}'
            if str(obj.get('candidate_id') or '').strip() != ctx.intent_id:
                return STATUS_FAILED, f'ENGINE_ACTIVITY_AUTHORIZATION_CANDIDATE_MISMATCH:path={path}'
            if bool(obj.get('broker_submit_transmit_allowed') is True) or bool(obj.get('autonomous_execution_allowed') is True):
                return STATUS_FAILED, f'ENGINE_ACTIVITY_AUTHORIZATION_SCOPE_VIOLATION:path={path}'
            if validation_status == 'VALID' and auth_status == 'AUTHORIZED' and status == 'AUTHORIZED' and decision == 'AUTHORIZED' and qty > 0:
                return STATUS_PRESENT, 'ENGINE_ACTIVITY_AUTHORIZED'
            blockers = ','.join(str(code) for code in obj.get('blocker_codes') or obj.get('blocker_reasons') or [] if str(code))
            return STATUS_FAILED, f'ENGINE_ACTIVITY_AUTHORIZATION_REJECTED:status={status}:authorization_status={auth_status}:blockers={blockers}:path={path}'
        if status == 'AUTHORIZED' and decision == 'AUTHORIZED' and qty > 0:
            return STATUS_PRESENT, 'ENGINE_ACTIVITY_AUTHORIZED'
        return STATUS_FAILED, f'ENGINE_ACTIVITY_NOT_AUTHORIZED:status={status}:decision={decision}:authorized_quantity={qty}:path={path}'

    if dependency_id == 'capital_authority_allocation_v1':
        schema_id = str(obj.get('schema_id') or '').strip()
        if schema_id not in {'C2_CAPITAL_AUTHORITY_ALLOCATION_V1', 'capital_authority_allocation'}:
            return STATUS_FAILED, f'CAPITAL_AUTHORITY_SCHEMA_MISMATCH:path={path}'
        status = _status_value(obj)
        validation_status = str(obj.get('validation_status') or '').strip().upper()
        if status and status != 'OK' and validation_status != 'VALID':
            return STATUS_FAILED, f'CAPITAL_AUTHORITY_STATUS_NOT_OK:status={status}:validation_status={validation_status}:path={path}'
        try:
            row = _bundle_b_authorized_row_for_intent(
                obj,
                ctx.intent_hash,
                ctx.intent_id,
                fallback_intent_ids=[str(ctx.plan_obj.get('source_intent_id') or '').strip()],
            )
        except ValueError as exc:
            return STATUS_FAILED, f'{exc}:path={path}'
        outcome = str(row.get('authorization_outcome') or '').strip().upper()
        qty = int(row.get('authorized_quantity') or 0)
        if outcome in {'APPROVED', 'RESIZED'} and qty > 0:
            return STATUS_PRESENT, f'CAPITAL_AUTHORITY_AUTHORIZED:outcome={outcome}:authorized_quantity={qty}'
        return STATUS_FAILED, f'CAPITAL_AUTHORITY_NOT_AUTHORIZED:outcome={outcome}:authorized_quantity={qty}:path={path}'

    status = _status_value(obj)
    if status and not _present_status_ok(status):
        return STATUS_FAILED, f'STATUS_NOT_PASS:status={status}:path={path}'
    return STATUS_PRESENT, 'ARTIFACT_PRESENT'


def _blocking_build_path(ctx: CandidateContext, dependency: Dict[str, Any]) -> Optional[Path]:
    pattern = str(dependency.get('blocking_build_path_pattern') or '').strip()
    if not pattern:
        return None
    values = {
        'canonical_truth_root': str(ctx.canonical_truth_root),
        'execution_truth_root': str(ctx.execution_truth_root),
        'day_utc': ctx.day_utc,
        'global_context_hash': ctx.global_context_hash,
        'economic_context_hash': ctx.economic_context_hash,
    }
    return Path(pattern.format(**values)).resolve()


def _evaluate_dependencies(ctx: CandidateContext, manifest: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    results: Dict[str, Dict[str, Any]] = {}
    for dependency in manifest.get('dependencies', []):
        dep_id = str(dependency.get('dependency_id') or '').strip()
        upstream_ids = [str(x).strip() for x in dependency.get('upstream_dependency_ids') or [] if str(x).strip()]
        path = _dependency_path(ctx, dependency)
        upstream_blockers = [uid for uid in upstream_ids if results.get(uid, {}).get('status') != STATUS_PRESENT]
        owner_ref = dependency.get('owner_ref')
        producer_ref = dependency.get('producer_ref')
        first_real_blocker_dependency_id = None
        blocking_build_ref = None

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
                if dep_id in {'global_context_package_v1', 'economic_state_package_v1'}:
                    build_path = _blocking_build_path(ctx, dependency)
                    if build_path is not None and build_path.exists() and build_path.is_file():
                        build_obj = _read_json(build_path)
                        blocking_build_ref = str(build_path)
                        build_closure = str(build_obj.get('closure_status') or '').strip().upper()
                        first_blocker = build_obj.get('first_real_blocker') if isinstance(build_obj.get('first_real_blocker'), dict) else {}
                        first_real_blocker_dependency_id = str(first_blocker.get('dependency_id') or '').strip() or None
                        if build_closure != 'COMPLETE':
                            prefix = 'GLOBAL_CONTEXT_BLOCKED' if dep_id == 'global_context_package_v1' else 'ECONOMIC_STATE_BLOCKED'
                            status = STATUS_FAILED
                            detail = f'{prefix}:first_real_blocker={first_real_blocker_dependency_id or "UNKNOWN"}:path={first_blocker.get("path")}:build_ref={build_path}'
                        else:
                            prefix = 'GLOBAL_CONTEXT_PACKAGE' if dep_id == 'global_context_package_v1' else 'ECONOMIC_STATE_PACKAGE'
                            status = STATUS_STALE
                            detail = f'{prefix}_MISSING_AFTER_COMPLETE_BUILD:build_ref={build_path}'

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
            'first_real_blocker_dependency_id': first_real_blocker_dependency_id,
            'blocking_build_ref': blocking_build_ref,
        }
    return results


def _stage_dependencies(manifest: Dict[str, Any], stage_id: str) -> List[Dict[str, Any]]:
    return [dep for dep in manifest.get('dependencies', []) if str(dep.get('stage_id') or '') == stage_id]


def _producer_command(*, producer_ref: str, ctx: CandidateContext) -> List[str]:
    py = sys.executable
    _git_sha(ctx.repo_root)
    day = ctx.day_utc
    if producer_ref == 'global_context_authority_v1':
        return [py, str((ctx.repo_root / 'ops/tools/run_global_context_authority_v1.py').resolve()), '--operation_type', 'fresh_paper_entry_v1', '--day_utc', day, '--sleeve_id', ctx.sleeve_id, '--environment', ctx.environment, '--ib_account', ctx.ib_account, '--materialize', 'YES', '--emit_package', 'YES']
    if producer_ref == 'authorization_artifacts_day_v1':
        return [py, str((ctx.repo_root / 'ops/tools/run_authorization_artifacts_day_v1.py').resolve()), '--day_utc', day, '--truth_root', str(ctx.execution_truth_root)]
    if producer_ref == 'global_kill_switch_v1':
        return [py, str((ctx.repo_root / 'ops/tools/run_global_kill_switch_v1.py').resolve()), '--day_utc', day]
    if producer_ref == 'trade_submit_readiness_c2_v1':
        return [py, str((ctx.repo_root / 'ops/tools/run_trade_submit_readiness_c2_v1.py').resolve()), '--day_utc', day, '--ib_account', ctx.ib_account, '--environment', ctx.environment]
    if producer_ref == 'economic_state_authority_v1':
        return [py, str((ctx.repo_root / 'ops/tools/run_economic_state_authority_v1.py').resolve()), '--operation_type', 'fresh_paper_entry_v1', '--day_utc', day, '--sleeve_id', ctx.sleeve_id, '--environment', ctx.environment, '--ib_account', ctx.ib_account, '--materialize', 'YES', '--emit_package', 'YES']
    raise ValueError(f'EXECUTION_BUILD_UNKNOWN_PRODUCER:{producer_ref}')


def _materializable_now(results: Dict[str, Dict[str, Any]], manifest: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for dependency in manifest.get('dependencies', []):
        dep_id = str(dependency.get('dependency_id') or '').strip()
        result = results.get(dep_id, {})
        if result.get('post_submit_only'):
            continue
        if result.get('status') in {STATUS_MISSING, STATUS_STALE, STATUS_FAILED} and result.get('producer_ref') and not result.get('upstream_blockers'):
            out.append(dep_id)
    return out


def _run_materializers(ctx: CandidateContext, manifest: Dict[str, Any], results: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
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
                if result.get('post_submit_only'):
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
                # Re-evaluate after each producer run so downstream stage dependencies
                # (for example ECONOMIC_STATE after GLOBAL_CONTEXT) become materializable
                # within the same execution-build run.
                current_results = _evaluate_dependencies(ctx, manifest)
                progress = True
    return materialized


def _collapse_first_real_blocker(results: Dict[str, Dict[str, Any]], dependency_id: str) -> Dict[str, Any]:
    result = results[dependency_id]
    if result['status'] == STATUS_PRESENT:
        return result
    for upstream_id in result.get('upstream_dependency_ids') or []:
        upstream = results.get(upstream_id)
        if upstream and upstream.get('status') != STATUS_PRESENT:
            return _collapse_first_real_blocker(results, upstream_id)
    return result


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
    ordered: List[Dict[str, Any]] = []
    for dependency in manifest.get('dependencies', []):
        dep_id = str(dependency.get('dependency_id') or '').strip()
        ordered.append(results[dep_id])
    return ordered


def _sealable(results: Dict[str, Dict[str, Any]], manifest: Dict[str, Any]) -> bool:
    post_submit_only = {
        str(dep.get('dependency_id') or '').strip()
        for dep in manifest.get('dependencies', [])
        if bool(dep.get('post_submit_only') is True)
    }
    required = {
        str(x).strip()
        for x in manifest.get('seal_requires') or []
        if str(x).strip() and str(x).strip() not in post_submit_only
    }
    return all(results.get(dep_id, {}).get('status') == STATUS_PRESENT for dep_id in required)


def _build_artifact_path(ctx: CandidateContext) -> Path:
    return (ctx.canonical_truth_root / 'reports' / BUILD_REPORT_FAMILY / ctx.day_utc / ctx.submission_id / 'execution_build.v1.json').resolve()


def _package_path(ctx: CandidateContext) -> Path:
    return (ctx.execution_truth_root / PACKAGE_FAMILY / ctx.day_utc / ctx.submission_id / 'execution_package.v1.json').resolve()


def _selected_order_plan_ref(ctx: CandidateContext) -> Dict[str, Any]:
    return {'path': str(ctx.plan_path), 'sha256': _sha256_file(ctx.plan_path)}


def _candidate_ref(ctx: CandidateContext) -> Dict[str, Any]:
    return {
        'phasec_out_dir': str(ctx.candidate_path),
        'candidate_day_dir': str(ctx.candidate_day_dir),
        'execution_truth_root': str(ctx.execution_truth_root),
        'canonical_truth_root': str(ctx.canonical_truth_root),
        'sleeve_id': ctx.sleeve_id,
        'environment': ctx.environment,
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


def run_execution_build_authority_v1(*, repo_root: Path, operation_type: str, candidate_path: Path, materialize: bool = True, emit_package: bool = True) -> Dict[str, Any]:
    repo_root = repo_root.resolve()
    ctx = resolve_candidate_context(repo_root=repo_root, candidate_path=candidate_path)
    if not str(ctx.day_utc or '').strip():
        raise ValueError('EXECUTION_BUILD_DAY_UTC_MISSING')
    if not str(ctx.environment or '').strip():
        raise ValueError('EXECUTION_BUILD_ENVIRONMENT_MISSING')
    if not str(ctx.intent_hash or '').strip():
        raise ValueError('EXECUTION_BUILD_INTENT_HASH_MISSING')
    manifest = _load_manifest(repo_root, operation_type)
    build_contract = assert_constitutional_writer_allowed_v1(
        repo_root,
        'execution_build_v1',
        'constellation_2.common.execution_build_authority_v1',
    )
    package_contract = assert_constitutional_writer_allowed_v1(
        repo_root,
        'execution_package_v1',
        'constellation_2.common.execution_build_authority_v1',
    )
    results = _evaluate_dependencies(ctx, manifest)
    materialized_nodes: List[Dict[str, Any]] = []
    if materialize and not _sealable(results, manifest):
        materialized_nodes = _run_materializers(ctx, manifest, results)
        results = _evaluate_dependencies(ctx, manifest)

    first_blocker: Optional[Dict[str, Any]] = None
    first_blocker_id: Optional[str] = None
    for dependency in manifest.get('dependencies', []):
        dep_id = str(dependency.get('dependency_id') or '').strip()
        result = results[dep_id]
        if result['required'] and not result['advisory_only'] and not result['post_submit_only'] and result['status'] != STATUS_PRESENT:
            first_blocker = _collapse_first_real_blocker(results, dep_id)
            first_blocker_id = str(first_blocker.get('dependency_id') or dep_id)
            break

    closure_status = CLOSURE_COMPLETE if _sealable(results, manifest) else CLOSURE_BLOCKED
    constitutional_refs = _constitutional_dependency_refs(
        repo_root=repo_root,
        results=results,
        dependency_ids=[str(x).strip() for x in build_contract.get('required_upstream_dependencies') or [] if str(x).strip()],
    )
    constitutional_dependency_declaration = build_artifact_dependency_declaration_v1(
        artifact_type='execution_build_v1',
        artifact_class=str(build_contract.get('artifact_class') or '').strip(),
        authority_id='execution_build_v1',
        declared_dependency_artifacts=[str(x).strip() for x in build_contract.get('required_upstream_dependencies') or [] if str(x).strip()],
        dependency_refs=constitutional_refs,
    )
    constitutional_lineage = build_governed_artifact_lineage_v1(
        artifact_type='execution_build_v1',
        artifact_version='v1',
        artifact_class=str(build_contract.get('artifact_class') or '').strip(),
        authority_id='execution_build_v1',
        producer_id='constellation_2.common.execution_build_authority_v1',
        generated_at_utc=_anchor_utc(ctx.day_utc),
        effective_at_utc=_anchor_utc(ctx.day_utc),
        finality_state=(FINALITY_FINALIZED if closure_status == CLOSURE_COMPLETE else FINALITY_PROVISIONAL),
        input_artifact_refs=constitutional_refs,
        policy_snapshot_refs=[],
        code_version=_git_sha(repo_root),
        run_id=ctx.submission_id,
    )
    frozen_decision_input_bundle = build_frozen_decision_input_bundle_v1(
        artifact_type='execution_build_v1',
        authority_id='execution_build_v1',
        generated_at_utc=_anchor_utc(ctx.day_utc),
        effective_at_utc=_anchor_utc(ctx.day_utc),
        input_artifact_refs=constitutional_refs,
        policy_snapshot_refs=[],
        run_id=ctx.submission_id,
        reason_codes=['CONSTITUTIONAL_RUNTIME_FROZEN_INPUT_BUNDLE_V1'],
    )
    build_obj: Dict[str, Any] = {
        'schema_id': 'execution_build',
        'schema_version': 'v1',
        'operation_type': operation_type,
        'candidate_ref': _candidate_ref(ctx),
        'attempt_id': ctx.attempt_id,
        'intent_id': ctx.intent_id,
        'trade_instance_id': ctx.trade_instance_id,
        'submission_id': ctx.submission_id,
        'selected_binding_record_ref': {
            'path': str(ctx.binding_path),
            'schema_version': ctx.binding_schema_version,
            'sha256': _sha256_file(ctx.binding_path),
        },
        'dependency_manifest_ref': str((repo_root / MANIFEST_REGISTRY_RELPATH).resolve()),
        'global_context_package_ref': {'path': str(ctx.global_context_package_path)},
        'economic_state_package_ref': {'path': str(ctx.economic_package_path)},
        'dependency_results': _dependency_results_list(results, manifest),
        'constitutional_dependency_declaration': constitutional_dependency_declaration,
        'constitutional_lineage': constitutional_lineage,
        'frozen_decision_input_bundle': frozen_decision_input_bundle,
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
    validate_against_repo_schema_v1(build_obj, repo_root, BUILD_SCHEMA_RELPATH)
    build_path = _build_artifact_path(ctx)
    build_sha = _write_canonical_json(build_path, build_obj)
    write_artifact_ledger_record_v1(
        artifact_path=build_path,
        artifact_type='execution_build_v1',
        truth_root=ctx.canonical_truth_root,
        runtime_root=infer_runtime_root_v1(ctx.canonical_truth_root),
        runtime_mode=runtime_mode_from_truth_root_v1(ctx.canonical_truth_root),
        day=ctx.day_utc,
        recovery_command='PYTHONPATH="$PWD" python3 ops/tools/run_execution_package_from_authorized_intent_v1.py',
        account=ctx.ib_account,
        sleeve=ctx.sleeve_id,
        artifact_id=f'execution_build_v1:{ctx.day_utc}:{ctx.submission_id}:{build_sha[:16]}',
    )

    package_path = None
    package_obj = None
    if emit_package and closure_status == CLOSURE_COMPLETE:
        dependency_refs = []
        ref_map: Dict[str, Dict[str, Any]] = {}
        for dependency in manifest.get('dependencies', []):
            dep_id = str(dependency.get('dependency_id') or '').strip()
            row = results[dep_id]
            if row['required'] and not row['advisory_only'] and not row['post_submit_only']:
                ref = {'dependency_id': dep_id, 'path': row['path'], 'sha256': row['sha256'], 'owner_ref': row['owner_ref'], 'role_class': row['role_class'], 'status': row['status']}
                dependency_refs.append(ref)
                ref_map[dep_id] = ref
        auth_row = _resolve_package_authorization_row(ctx=ctx, ref_map=ref_map)
        plan_kind = str(ctx.plan_summary.get('plan_kind') or '').strip().upper()
        authorized_quantity = _int_or_zero(auth_row.get('authorized_quantity'), field_name='AUTHORIZED_QUANTITY')
        if authorized_quantity <= 0:
            raise ValueError('EXECUTION_PACKAGE_AUTHORIZED_QUANTITY_MISSING_OR_NONPOSITIVE')
        plan_quantity = _int_or_zero(ctx.plan_summary.get('quantity'), field_name='PLAN_QUANTITY')
        package_quantity = authorized_quantity
        if plan_quantity > 0:
            package_quantity = min(plan_quantity, authorized_quantity)
        if package_quantity <= 0:
            raise ValueError('EXECUTION_PACKAGE_QUANTITY_NOT_POSITIVE')

        package_legs = copy.deepcopy(ctx.plan_summary.get('legs'))
        package_defined_risk = ctx.plan_summary.get('defined_risk_proven')
        plan_max_defined_loss_cents = _int_or_zero(ctx.plan_summary.get('max_defined_loss_cents'), field_name='MAX_DEFINED_LOSS_CENTS')
        row_risk_per_unit_cents = _int_or_zero(auth_row.get('risk_per_unit_cents'), field_name='RISK_PER_UNIT_CENTS')
        plan_risk_per_unit_cents = _int_or_zero(ctx.plan_summary.get('risk_per_unit_cents'), field_name='RISK_PER_UNIT_CENTS')
        package_risk_per_unit_cents = row_risk_per_unit_cents if row_risk_per_unit_cents > 0 else plan_risk_per_unit_cents
        package_required_risk_cents = package_risk_per_unit_cents * package_quantity if package_risk_per_unit_cents > 0 else _int_or_zero(
            auth_row.get('required_risk_cents'),
            field_name='REQUIRED_RISK_CENTS',
        )
        package_max_defined_loss_cents = plan_max_defined_loss_cents
        if plan_kind == 'OPTIONS':
            if not isinstance(package_legs, list) or not package_legs:
                raise ValueError('EXECUTION_PACKAGE_OPTIONS_LEGS_MISSING')
            if package_defined_risk is not True:
                raise ValueError('EXECUTION_PACKAGE_OPTIONS_DEFINED_RISK_NOT_PROVEN')
            if package_risk_per_unit_cents <= 0:
                raise ValueError('EXECUTION_PACKAGE_OPTIONS_RISK_PER_UNIT_MISSING')
            if package_required_risk_cents <= 0:
                raise ValueError('EXECUTION_PACKAGE_OPTIONS_REQUIRED_RISK_MISSING')
            package_max_defined_loss_cents = package_risk_per_unit_cents * package_quantity
            if package_max_defined_loss_cents <= 0:
                raise ValueError('EXECUTION_PACKAGE_OPTIONS_MAX_DEFINED_LOSS_MISSING')

        package_obj = {
            'schema_id': 'execution_package',
            'schema_version': 'v1',
            'operation_type': operation_type,
            'day_utc': ctx.day_utc,
            'environment': ctx.environment,
            'ib_account': ctx.ib_account,
            'candidate_ref': _candidate_ref(ctx),
            'attempt_id': ctx.attempt_id,
            'intent_hash': ctx.intent_hash,
            'intent_id': ctx.intent_id,
            'trade_instance_id': ctx.trade_instance_id,
            'submission_id': ctx.submission_id,
            'exposure_type': ctx.plan_summary.get('exposure_type'),
            'symbol': ctx.plan_summary.get('symbol'),
            'underlying': ctx.plan_summary.get('underlying'),
            'quantity': package_quantity,
            'authorized_quantity': authorized_quantity,
            'order_type': ctx.plan_summary.get('order_type'),
            'legs': package_legs,
            'protective_stop': copy.deepcopy(ctx.plan_summary.get('protective_stop')),
            'take_profit': copy.deepcopy(ctx.plan_summary.get('take_profit')),
            'bracket': copy.deepcopy(ctx.plan_summary.get('bracket')),
            'risk_contract_ref': copy.deepcopy(ctx.plan_summary.get('risk_contract_ref')),
            'broker_order': copy.deepcopy(ctx.plan_summary.get('broker_order')),
            'defined_risk_proven': package_defined_risk,
            'max_defined_loss_cents': package_max_defined_loss_cents if package_max_defined_loss_cents > 0 else None,
            'risk_per_unit_cents': package_risk_per_unit_cents if package_risk_per_unit_cents > 0 else None,
            'required_risk_cents': package_required_risk_cents if package_required_risk_cents > 0 else None,
            'evidence_artifacts': copy.deepcopy(ctx.plan_summary.get('evidence_artifacts') or []),
            'selected_order_plan_ref': _selected_order_plan_ref(ctx),
            'dependency_manifest_ref': str((repo_root / MANIFEST_REGISTRY_RELPATH).resolve()),
            'build_ref': {'path': str(build_path), 'sha256': build_sha},
            'global_context_package_ref': ref_map.get('global_context_package_v1'),
            'economic_state_package_ref': ref_map.get('economic_state_package_v1'),
            'dependency_refs': dependency_refs,
            'sealed': True,
            'sealed_utc': _anchor_utc(ctx.day_utc),
            'seal_basis': 'full closure achieved',
            'constitutional_lineage': build_governed_artifact_lineage_v1(
                artifact_type='execution_package_v1',
                artifact_version='v1',
                artifact_class=str(package_contract.get('artifact_class') or '').strip(),
                authority_id='execution_package_v1',
                producer_id='constellation_2.common.execution_build_authority_v1',
                generated_at_utc=_anchor_utc(ctx.day_utc),
                effective_at_utc=_anchor_utc(ctx.day_utc),
                finality_state=FINALITY_FINALIZED,
                input_artifact_refs=constitutional_refs,
                policy_snapshot_refs=[],
                code_version=_git_sha(repo_root),
                run_id=ctx.submission_id,
            ),
            'canonical_json_hash': None,
        }
        package_obj['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1(package_obj)
        validate_against_repo_schema_v1(package_obj, repo_root, PACKAGE_SCHEMA_RELPATH)
        package_path = _package_path(ctx)
        package_sha = _write_canonical_json(package_path, package_obj)
        write_artifact_ledger_record_v1(
            artifact_path=package_path,
            artifact_type='execution_package_v1',
            truth_root=ctx.execution_truth_root,
            runtime_root=infer_runtime_root_v1(ctx.execution_truth_root),
            runtime_mode=runtime_mode_from_truth_root_v1(ctx.execution_truth_root),
            day=ctx.day_utc,
            recovery_command='PYTHONPATH="$PWD" python3 ops/tools/run_execution_package_from_authorized_intent_v1.py',
            account=ctx.ib_account,
            sleeve=ctx.sleeve_id,
            artifact_id=f'execution_package_v1:{ctx.day_utc}:{ctx.submission_id}:{package_sha[:16]}',
        )

    return {'context': ctx, 'manifest': manifest, 'results': results, 'build_path': build_path, 'build_obj': build_obj, 'package_path': package_path, 'package_obj': package_obj}
