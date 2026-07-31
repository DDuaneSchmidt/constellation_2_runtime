from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Mapping

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

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
from constellation_2.common.constitutional_decision_v1 import (
    DECISION_ENUMS_V1,
    SCOPE_LEVELS_V1,
    decision_hash_v1,
    decision_restrictiveness_rank_v1,
    evaluate_constitutional_decision_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

SCHEMA = 'governance/04_DATA/SCHEMAS/C2/REPORTS/replay_manifest.v1.schema.json'
CANDIDATE_POLICY_REPLAY_SCHEMA = (
    'governance/04_DATA/SCHEMAS/C2/REPORTS/candidate_policy_replay_report.v1.schema.json'
)
AUTO_DECISION_ENUMS = {'AUTO_EXECUTE', 'AUTO_EXECUTE_PROTECTIVE'}
BLOCK_DECISION_ENUMS = {'BLOCK', 'FREEZE_SCOPE'}
DEFAULT_CANDIDATE_POLICY_THRESHOLDS = {
    'decision_change_warn_pct': 20.0,
    'auto_execute_increase_warn_pct': 10.0,
    'block_rate_decrease_warn_pct': 10.0,
    'divergence_warn_pct': 20.0,
}


def _format_decimal_v1(value: float) -> str:
    return f'{float(value):.4f}'


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


def _iter_post_entry_boundary_paths(truth_root: Path, day: str) -> List[Path]:
    matches: List[Path] = []
    for path in sorted(truth_root.rglob('post_entry_submit_boundary.v1.json')):
        if day in path.parts and path.is_file():
            matches.append(path.resolve())
    return matches


def _relative_artifact_path(truth_root: Path, path: Path) -> str:
    resolved_path = path.resolve()
    try:
        return str(resolved_path.relative_to(truth_root.resolve()))
    except ValueError:
        return str(resolved_path)


def _collect_override_analysis(truth_root: Path, day: str) -> Dict[str, Any]:
    review_artifacts: Dict[str, Dict[str, Any]] = {}
    mismatch_cases: List[Dict[str, Any]] = []
    authorization_dir = truth_root / 'engine_activity_v1' / 'authorization_v1' / day
    if authorization_dir.exists() and authorization_dir.is_dir():
        for path in sorted(authorization_dir.glob('*.authorization.v1.json')):
            obj = _read_json(path)
            proposal_hash = str(obj.get('proposal_hash') or '').strip().lower()
            constitutional_shadow = dict(obj.get('constitutional_shadow') or {})
            review_packet = dict(constitutional_shadow.get('review_packet') or {})
            if proposal_hash and review_packet:
                review_artifacts[proposal_hash] = {
                    'artifact_type': 'authorization_v1',
                    'artifact_path': str(path),
                    'review_packet': review_packet,
                    'blocker_rules': list((constitutional_shadow.get('decision') or {}).get('blocker_rules') or []),
                }
            comparison = dict(obj.get('legacy_constitutional_comparison') or {})
            if str(comparison.get('comparison_status') or '').strip().upper() == 'MISMATCH':
                mismatch_cases.append(
                    {
                        'proposal_hash': proposal_hash,
                        'artifact_type': 'authorization_v1',
                        'artifact_path': _relative_artifact_path(truth_root, path),
                        'reason_codes': list(comparison.get('reason_codes') or []),
                    }
                )

    for path in _iter_post_entry_boundary_paths(truth_root, day):
        obj = _read_json(path)
        constitutional_shadow = dict(obj.get('constitutional_shadow') or {})
        proposal_hash = str(constitutional_shadow.get('proposal_hash') or '').strip().lower()
        review_packet = dict(constitutional_shadow.get('review_packet') or {})
        if proposal_hash and review_packet:
                review_artifacts[proposal_hash] = {
                    'artifact_type': 'post_entry_submit_boundary_v1',
                    'artifact_path': _relative_artifact_path(truth_root, path),
                    'review_packet': review_packet,
                    'blocker_rules': list((constitutional_shadow.get('decision') or {}).get('blocker_rules') or []),
                }

    approved_override_count = 0
    operator_decision_count = 0
    human_system_divergence_count = 0
    override_frequency_by_action_class: Dict[str, int] = {}
    block_reasons_distribution: Dict[str, int] = {}
    override_cases: List[Dict[str, Any]] = []
    operator_dir = truth_root / 'reports' / 'constitutional_operator_decision_v1' / day
    if operator_dir.exists() and operator_dir.is_dir():
        for path in sorted(operator_dir.glob('*.constitutional_operator_decision.v1.json')):
            obj = _read_json(path)
            proposal_hash = str(obj.get('proposal_hash') or '').strip().lower()
            operator_action = str(obj.get('operator_action') or '').strip().upper()
            final_decision_applied = str(obj.get('final_decision_applied') or '').strip().upper()
            original_decision_enum = str(obj.get('original_decision_enum') or '').strip().upper()
            review_artifact = review_artifacts.get(proposal_hash, {})
            review_packet = dict(review_artifact.get('review_packet') or {})
            action_class = str(review_packet.get('action_class') or '').strip().upper()
            blocker_rules = [
                str(item).strip()
                for item in (review_packet.get('blocker_rules') or review_artifact.get('blocker_rules') or [])
                if str(item).strip()
            ]
            operator_decision_count += 1
            if final_decision_applied != original_decision_enum:
                human_system_divergence_count += 1
            if operator_action == 'APPROVE':
                approved_override_count += 1
                override_frequency_by_action_class[action_class or 'UNKNOWN'] = (
                    override_frequency_by_action_class.get(action_class or 'UNKNOWN', 0) + 1
                )
                outcome_difference = 'EXECUTED_WITH_HUMAN_OVERRIDE'
            elif operator_action == 'REJECT':
                block_reasons_distribution['OPERATOR_REJECTED'] = block_reasons_distribution.get('OPERATOR_REJECTED', 0) + 1
                outcome_difference = 'BLOCKED_BY_OPERATOR_REJECTION'
            else:
                block_reasons_distribution['OPERATOR_DEFERRED'] = block_reasons_distribution.get('OPERATOR_DEFERRED', 0) + 1
                outcome_difference = 'REMAINS_PENDING_HUMAN_DECISION'
            for rule in blocker_rules:
                block_reasons_distribution[rule] = block_reasons_distribution.get(rule, 0) + 1
            override_cases.append(
                {
                    'proposal_hash': proposal_hash,
                    'action_class': action_class or 'UNKNOWN',
                    'source_artifact_type': str(review_artifact.get('artifact_type') or obj.get('source_artifact_ref', {}).get('artifact_type') or ''),
                    'system_decision_without_override': original_decision_enum,
                    'operator_action': operator_action,
                    'human_final_decision': final_decision_applied,
                    'outcome_difference': outcome_difference,
                }
            )
    return {
        'review_required_count': len(review_artifacts),
        'operator_decision_count': operator_decision_count,
        'approved_override_count': approved_override_count,
        'override_frequency_by_action_class': dict(sorted(override_frequency_by_action_class.items())),
        'mismatch_count': len(mismatch_cases),
        'mismatch_cases': sorted(mismatch_cases, key=lambda item: (item['proposal_hash'], item['artifact_path'])),
        'block_reasons_distribution': dict(sorted(block_reasons_distribution.items())),
        'human_system_divergence_count': human_system_divergence_count,
        'override_cases': sorted(
            override_cases,
            key=lambda item: (item['proposal_hash'], item['operator_action'], item['human_final_decision']),
        ),
    }


def _normalized_upper_list(raw: Any) -> List[str]:
    if not isinstance(raw, (list, tuple)):
        return []
    return sorted({str(item).strip().upper() for item in raw if str(item).strip()})


def _normalized_scope_authority_map(raw: Any) -> Dict[str, str]:
    if not isinstance(raw, Mapping):
        return {}
    normalized: Dict[str, str] = {}
    for level in SCOPE_LEVELS_V1:
        value = str(raw.get(level) or '').strip().upper()
        if not value:
            continue
        if value not in DECISION_ENUMS_V1:
            raise SystemExit(f'FAIL: CANDIDATE_POLICY_SCOPE_AUTHORITY_INVALID:{level}:{value}')
        normalized[level] = value
    return normalized


def _load_candidate_policy_profile(candidate_policy_json: str | None, candidate_policy_version: str | None) -> Dict[str, Any]:
    raw_obj: Dict[str, Any] = {}
    policy_source = 'same_as_original'
    if candidate_policy_json:
        source_path = Path(str(candidate_policy_json).strip()).expanduser().resolve()
        if not source_path.exists() or not source_path.is_file():
            raise SystemExit(f'FAIL: CANDIDATE_POLICY_JSON_MISSING: {source_path}')
        candidate_obj = _read_json(source_path)
        raw_obj = dict(candidate_obj)
        policy_source = str(source_path)
    policy_version = str(candidate_policy_version or raw_obj.get('policy_version') or 'same_as_original').strip()
    thresholds_raw = dict(raw_obj.get('thresholds') or {})
    thresholds: Dict[str, str] = {}
    for key, fallback in DEFAULT_CANDIDATE_POLICY_THRESHOLDS.items():
        value = thresholds_raw.get(key, fallback)
        try:
            thresholds[key] = _format_decimal_v1(float(value))
        except (TypeError, ValueError):
            raise SystemExit(f'FAIL: CANDIDATE_POLICY_THRESHOLD_INVALID:{key}:{value!r}')

    admissibility_raw = dict(raw_obj.get('admissibility_rules') or {})
    admissibility_rules: Dict[str, Any] = {}
    for section_name in (
        'general_admissibility_allow_by_action_class',
        'tax_admissibility_allow_by_action_class',
        'dependency_health_allow_by_action_class',
        'state_coherence_allow_by_action_class',
    ):
        section = dict(admissibility_raw.get(section_name) or {})
        normalized_section: Dict[str, List[str]] = {}
        for action_key in ('PROTECTIVE', 'CONSTRUCTIVE'):
            values = _normalized_upper_list(section.get(action_key))
            if values:
                normalized_section[action_key] = values
        if normalized_section:
            admissibility_rules[section_name] = normalized_section

    scope_raw = dict(raw_obj.get('scope_authorities') or {})
    scope_defaults = _normalized_scope_authority_map(scope_raw.get('defaults'))
    by_action_raw = dict(scope_raw.get('by_action_class') or {})
    scope_by_action: Dict[str, Dict[str, str]] = {}
    for action_key in ('PROTECTIVE', 'CONSTRUCTIVE'):
        normalized_map = _normalized_scope_authority_map(by_action_raw.get(action_key))
        if normalized_map:
            scope_by_action[action_key] = normalized_map

    profile: Dict[str, Any] = {
        'policy_version': policy_version,
        'policy_source': policy_source,
        'thresholds': thresholds,
    }
    if admissibility_rules:
        profile['admissibility_rules'] = admissibility_rules
    if scope_defaults or scope_by_action:
        profile['scope_authorities'] = {}
        if scope_defaults:
            profile['scope_authorities']['defaults'] = scope_defaults
        if scope_by_action:
            profile['scope_authorities']['by_action_class'] = scope_by_action
    profile['policy_input_hash'] = hashlib.sha256(canonical_json_bytes_v1({
        'policy_version': profile['policy_version'],
        'thresholds': profile['thresholds'],
        'admissibility_rules': profile.get('admissibility_rules') or {},
        'scope_authorities': profile.get('scope_authorities') or {},
    })).hexdigest()
    return profile


def _load_operator_decision_map(truth_root: Path, day: str) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    operator_dir = truth_root / 'reports' / 'constitutional_operator_decision_v1' / day
    if not operator_dir.exists() or not operator_dir.is_dir():
        return result
    for path in sorted(operator_dir.glob('*.constitutional_operator_decision.v1.json')):
        obj = _read_json(path)
        proposal_hash = str(obj.get('proposal_hash') or '').strip().lower()
        if len(proposal_hash) != 64:
            continue
        decided_at = str(obj.get('decided_at') or '').strip()
        current = result.get(proposal_hash)
        if current is None or (decided_at, path.name) >= (str(current.get('decided_at') or ''), str(current.get('artifact_name') or '')):
            result[proposal_hash] = {
                'artifact_path': _relative_artifact_path(truth_root, path),
                'artifact_name': path.name,
                'decided_at': decided_at,
                'payload': obj,
            }
    return result


def _extract_constitutional_row(
    *,
    source_artifact_type: str,
    source_artifact_path: Path,
    artifact_obj: Mapping[str, Any],
    operator_decisions: Mapping[str, Any],
    truth_root: Path,
) -> Dict[str, Any] | None:
    shadow = dict(artifact_obj.get('constitutional_shadow') or {})
    proposal = dict(shadow.get('proposal') or {})
    fact_bundle = dict(shadow.get('fact_bundle') or {})
    decision = dict(shadow.get('decision') or {})
    proposal_hash = str(shadow.get('proposal_hash') or artifact_obj.get('proposal_hash') or '').strip().lower()
    fact_bundle_hash = str(shadow.get('fact_bundle_hash') or artifact_obj.get('fact_bundle_hash') or '').strip().lower()
    if not proposal or not fact_bundle or not decision or len(proposal_hash) != 64 or len(fact_bundle_hash) != 64:
        return None
    return {
        'proposal_hash': proposal_hash,
        'fact_bundle_hash': fact_bundle_hash,
        'source_artifact_type': source_artifact_type,
        'source_artifact_path': _relative_artifact_path(truth_root, source_artifact_path),
        'proposal': proposal,
        'fact_bundle': fact_bundle,
        'stored_decision': decision,
        'operator_decision': dict(operator_decisions.get(proposal_hash, {}).get('payload') or {}),
    }


def _collect_constitutional_replay_rows(truth_root: Path, day: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    operator_decisions = _load_operator_decision_map(truth_root, day)
    authorization_dir = truth_root / 'engine_activity_v1' / 'authorization_v1' / day
    if authorization_dir.exists() and authorization_dir.is_dir():
        for path in sorted(authorization_dir.glob('*.authorization.v1.json')):
            row = _extract_constitutional_row(
                source_artifact_type='authorization_v1',
                source_artifact_path=path,
                artifact_obj=_read_json(path),
                operator_decisions=operator_decisions,
                truth_root=truth_root,
            )
            if row is not None:
                rows.append(row)
    for path in _iter_post_entry_boundary_paths(truth_root, day):
        row = _extract_constitutional_row(
            source_artifact_type='post_entry_submit_boundary_v1',
            source_artifact_path=path,
            artifact_obj=_read_json(path),
            operator_decisions=operator_decisions,
            truth_root=truth_root,
        )
        if row is not None:
            rows.append(row)
    rows.sort(key=lambda item: (str(item['proposal_hash']), str(item['source_artifact_path'])))
    return rows


def _gate_record(decision: Mapping[str, Any], gate_index: int) -> Dict[str, Any]:
    for row in list(decision.get('gate_results') or []):
        if not isinstance(row, Mapping):
            continue
        if int(row.get('gate_index') or 0) == int(gate_index):
            return dict(row)
    return {}


def _decision_input_state_from_stored(decision: Mapping[str, Any]) -> Dict[str, Any]:
    gate_7 = _gate_record(decision, 7)
    gate_8 = _gate_record(decision, 8)
    gate_9 = _gate_record(decision, 9)
    effective_authority = str(dict(decision.get('effective_scope') or {}).get('effective_authority') or 'REQUIRE_HUMAN_REVIEW').strip().upper()
    if effective_authority not in DECISION_ENUMS_V1:
        effective_authority = 'REQUIRE_HUMAN_REVIEW'
    policy_blockers: List[str] = []
    gate_8_detail = str(gate_8.get('detail') or '').strip()
    if str(gate_8.get('outcome') or '').strip().upper() == 'FAIL' and gate_8_detail and gate_8_detail != 'POLICY_BLOCKERS_ABSENT':
        policy_blockers = sorted({item.strip() for item in gate_8_detail.split(',') if item.strip()})
    return {
        'scope_authorities': {level: effective_authority for level in SCOPE_LEVELS_V1},
        'hard_envelope_ok': str(gate_7.get('outcome') or '').strip().upper() == 'PASS',
        'policy_blockers': policy_blockers,
        'persistence_ok': str(gate_9.get('outcome') or '').strip().upper() == 'PASS',
    }


def _candidate_scope_authorities(
    *,
    stored_decision: Mapping[str, Any],
    action_class: str,
    candidate_policy: Mapping[str, Any],
) -> Dict[str, str]:
    base = dict(_decision_input_state_from_stored(stored_decision)['scope_authorities'])
    scope_authorities = dict(candidate_policy.get('scope_authorities') or {})
    defaults = _normalized_scope_authority_map(scope_authorities.get('defaults'))
    base.update(defaults)
    by_action = dict(scope_authorities.get('by_action_class') or {})
    action_key = 'PROTECTIVE' if str(action_class or '').strip().upper() == 'PROTECTIVE' else 'CONSTRUCTIVE'
    base.update(_normalized_scope_authority_map(by_action.get(action_key)))
    return base


def _decision_category(decision_enum: str) -> str:
    normalized = str(decision_enum or '').strip().upper()
    if normalized in AUTO_DECISION_ENUMS:
        return 'AUTO'
    if normalized == 'REQUIRE_HUMAN_REVIEW':
        return 'REVIEW'
    if normalized in BLOCK_DECISION_ENUMS:
        return 'BLOCK'
    return 'OTHER'


def _divergence_type(original_decision: str, candidate_decision: str) -> str:
    original = str(original_decision or '').strip().upper()
    candidate = str(candidate_decision or '').strip().upper()
    if original == candidate:
        return 'SAME'
    if _decision_category(original) != _decision_category(candidate) and _decision_category(original) != 'AUTO' and _decision_category(candidate) != 'AUTO':
        return 'DIFFERENT_CLASSIFICATION'
    if decision_restrictiveness_rank_v1(candidate) < decision_restrictiveness_rank_v1(original):
        return 'MORE_PERMISSIVE'
    return 'MORE_RESTRICTIVE'


def _evaluate_candidate_policy_replay(
    *,
    source_truth_root: Path,
    replay_truth_root: Path,
    day: str,
    submission_id: str,
    candidate_policy: Mapping[str, Any],
) -> Dict[str, Any]:
    historical_override_analysis = _collect_override_analysis(source_truth_root, day)
    replay_rows = _collect_constitutional_replay_rows(source_truth_root, day)
    comparisons: List[Dict[str, Any]] = []
    reason_codes: List[str] = []

    for row in replay_rows:
        proposal = dict(row['proposal'])
        fact_bundle = dict(row['fact_bundle'])
        stored_decision = dict(row['stored_decision'])
        action_class = str(proposal.get('action_class') or '').strip().upper()
        input_state = _decision_input_state_from_stored(stored_decision)
        original_replayed = evaluate_constitutional_decision_v1(
            proposal=proposal,
            proposal_hash=str(row['proposal_hash']),
            fact_bundle=fact_bundle,
            fact_bundle_hash=str(row['fact_bundle_hash']),
            policy_version=str(stored_decision.get('policy_version') or candidate_policy.get('policy_version') or '').strip(),
            scope_authorities=input_state['scope_authorities'],
            hard_envelope_ok=bool(input_state['hard_envelope_ok']),
            policy_blockers=list(input_state['policy_blockers']),
            persistence_ok=bool(input_state['persistence_ok']),
            evaluated_at=str(stored_decision.get('evaluated_at') or stored_decision.get('decision_time_utc') or f'{day}T00:00:00Z').strip(),
        )
        original_reproduced = decision_hash_v1(stored_decision) == decision_hash_v1(original_replayed)
        if not original_reproduced:
            reason_codes.append(f'ORIGINAL_DECISION_REPLAY_MISMATCH:{row["proposal_hash"]}')

        candidate_policy_version = str(candidate_policy.get('policy_version') or '').strip()
        if candidate_policy_version == 'same_as_original' or not candidate_policy_version:
            candidate_policy_version = str(stored_decision.get('policy_version') or '').strip()
        candidate_decision = evaluate_constitutional_decision_v1(
            proposal=proposal,
            proposal_hash=str(row['proposal_hash']),
            fact_bundle=fact_bundle,
            fact_bundle_hash=str(row['fact_bundle_hash']),
            policy_version=candidate_policy_version,
            scope_authorities=_candidate_scope_authorities(
                stored_decision=stored_decision,
                action_class=action_class,
                candidate_policy=candidate_policy,
            ),
            hard_envelope_ok=bool(input_state['hard_envelope_ok']),
            policy_blockers=list(input_state['policy_blockers']),
            persistence_ok=bool(input_state['persistence_ok']),
            evaluated_at=str(stored_decision.get('evaluated_at') or stored_decision.get('decision_time_utc') or f'{day}T00:00:00Z').strip(),
            policy_profile=candidate_policy,
        )
        operator_decision = dict(row.get('operator_decision') or {})
        operator_action = str(operator_decision.get('operator_action') or '').strip().upper()
        human_final_decision = str(operator_decision.get('final_decision_applied') or '').strip().upper()
        original_decision_enum = str(stored_decision.get('decision_enum') or '').strip().upper()
        candidate_decision_enum = str(candidate_decision.get('decision_enum') or '').strip().upper()
        comparisons.append(
            {
                'proposal_hash': str(row['proposal_hash']),
                'fact_bundle_hash': str(row['fact_bundle_hash']),
                'source_artifact_type': str(row['source_artifact_type']),
                'source_artifact_path': str(row['source_artifact_path']),
                'action_type': str(proposal.get('action_type') or ''),
                'action_class': action_class,
                'original_policy_version': str(stored_decision.get('policy_version') or '').strip(),
                'candidate_policy_version': candidate_policy_version,
                'original_decision': original_decision_enum,
                'candidate_decision': candidate_decision_enum,
                'original_authorization_issuable': bool(stored_decision.get('authorization_issuable') is True),
                'candidate_authorization_issuable': bool(candidate_decision.get('authorization_issuable') is True),
                'divergence_type': _divergence_type(original_decision_enum, candidate_decision_enum),
                'original_reproduced': bool(original_reproduced),
                'original_reproduction_reason_codes': [] if original_reproduced else ['ORIGINAL_DECISION_REPLAY_MISMATCH'],
                'operator_action': operator_action or None,
                'human_final_decision': human_final_decision or None,
                'override_required_original': original_decision_enum == 'REQUIRE_HUMAN_REVIEW',
                'override_required_candidate': candidate_decision_enum == 'REQUIRE_HUMAN_REVIEW',
                'would_system_now_auto_execute': candidate_decision_enum in AUTO_DECISION_ENUMS,
                'would_system_now_block': candidate_decision_enum in BLOCK_DECISION_ENUMS,
                'candidate_matches_human_resolution': (
                    None if not human_final_decision else candidate_decision_enum == human_final_decision
                ),
                'original_blocker_rules': sorted(str(item).strip() for item in (stored_decision.get('blocker_rules') or []) if str(item).strip()),
                'candidate_blocker_rules': sorted(str(item).strip() for item in (candidate_decision.get('blocker_rules') or []) if str(item).strip()),
            }
        )

    proposal_count = len(comparisons)
    original_decision_counts: Dict[str, int] = {}
    candidate_decision_counts: Dict[str, int] = {}
    divergence_type_counts: Dict[str, int] = {}
    block_reasons_distribution: Dict[str, int] = {}
    candidate_block_reasons_distribution: Dict[str, int] = {}
    decision_changed_count = 0
    original_auto_execute_count = 0
    candidate_auto_execute_count = 0
    original_block_count = 0
    candidate_block_count = 0
    original_review_count = 0
    candidate_review_count = 0
    override_reduction_count = 0
    constructive_more_permissive_count = 0
    protective_more_permissive_count = 0
    review_to_auto_count = 0
    block_to_auto_count = 0
    candidate_matches_human_resolution_count = 0
    for item in comparisons:
        original_decision = str(item['original_decision'])
        candidate_decision_enum = str(item['candidate_decision'])
        divergence_type = str(item['divergence_type'])
        original_decision_counts[original_decision] = original_decision_counts.get(original_decision, 0) + 1
        candidate_decision_counts[candidate_decision_enum] = candidate_decision_counts.get(candidate_decision_enum, 0) + 1
        divergence_type_counts[divergence_type] = divergence_type_counts.get(divergence_type, 0) + 1
        if divergence_type != 'SAME':
            decision_changed_count += 1
        if original_decision in AUTO_DECISION_ENUMS:
            original_auto_execute_count += 1
        if candidate_decision_enum in AUTO_DECISION_ENUMS:
            candidate_auto_execute_count += 1
        if original_decision in BLOCK_DECISION_ENUMS:
            original_block_count += 1
        if candidate_decision_enum in BLOCK_DECISION_ENUMS:
            candidate_block_count += 1
        if original_decision == 'REQUIRE_HUMAN_REVIEW':
            original_review_count += 1
        if candidate_decision_enum == 'REQUIRE_HUMAN_REVIEW':
            candidate_review_count += 1
        if bool(item['override_required_original']) and not bool(item['override_required_candidate']):
            override_reduction_count += 1
        if divergence_type == 'MORE_PERMISSIVE':
            if str(item['action_class']) == 'CONSTRUCTIVE':
                constructive_more_permissive_count += 1
            else:
                protective_more_permissive_count += 1
        if original_decision == 'REQUIRE_HUMAN_REVIEW' and candidate_decision_enum in AUTO_DECISION_ENUMS:
            review_to_auto_count += 1
        if original_decision in BLOCK_DECISION_ENUMS and candidate_decision_enum in AUTO_DECISION_ENUMS:
            block_to_auto_count += 1
        if item['candidate_matches_human_resolution'] is True:
            candidate_matches_human_resolution_count += 1
        for rule in list(item.get('original_blocker_rules') or []):
            block_reasons_distribution[rule] = block_reasons_distribution.get(rule, 0) + 1
        for rule in list(item.get('candidate_blocker_rules') or []):
            candidate_block_reasons_distribution[rule] = candidate_block_reasons_distribution.get(rule, 0) + 1

    decision_changed_pct = 0.0 if proposal_count == 0 else round((decision_changed_count / proposal_count) * 100.0, 4)
    auto_execute_delta_count = candidate_auto_execute_count - original_auto_execute_count
    auto_execute_delta_pct = 0.0 if proposal_count == 0 else round((auto_execute_delta_count / proposal_count) * 100.0, 4)
    block_delta_count = candidate_block_count - original_block_count
    block_delta_pct = 0.0 if proposal_count == 0 else round((block_delta_count / proposal_count) * 100.0, 4)
    override_reduction_pct = 0.0 if original_review_count == 0 else round((override_reduction_count / original_review_count) * 100.0, 4)
    mismatch_frequency = 0.0 if proposal_count == 0 else round((int(historical_override_analysis.get('mismatch_count') or 0) / proposal_count) * 100.0, 4)
    permissive_divergence_pct = 0.0 if proposal_count == 0 else round((int(divergence_type_counts.get('MORE_PERMISSIVE', 0)) / proposal_count) * 100.0, 4)
    decision_changed_pct_text = _format_decimal_v1(decision_changed_pct)
    auto_execute_delta_pct_text = _format_decimal_v1(auto_execute_delta_pct)
    block_delta_pct_text = _format_decimal_v1(block_delta_pct)
    override_reduction_pct_text = _format_decimal_v1(override_reduction_pct)
    mismatch_frequency_text = _format_decimal_v1(mismatch_frequency)
    permissive_divergence_pct_text = _format_decimal_v1(permissive_divergence_pct)

    thresholds = dict(candidate_policy.get('thresholds') or {})
    risk_flags: List[Dict[str, Any]] = []
    if auto_execute_delta_pct > float(str(thresholds.get('auto_execute_increase_warn_pct') or '0')):
        risk_flags.append({
            'flag': 'AUTO_EXECUTE_INCREASE_THRESHOLD_EXCEEDED',
            'severity': 'HIGH',
            'detail': f'auto_execute_delta_pct={auto_execute_delta_pct_text}',
        })
    if block_delta_pct < -float(str(thresholds.get('block_rate_decrease_warn_pct') or '0')):
        risk_flags.append({
            'flag': 'BLOCK_RATE_DECREASE_THRESHOLD_EXCEEDED',
            'severity': 'HIGH',
            'detail': f'block_delta_pct={block_delta_pct_text}',
        })
    if decision_changed_pct > float(str(thresholds.get('decision_change_warn_pct') or '0')):
        risk_flags.append({
            'flag': 'DECISION_CHANGE_THRESHOLD_EXCEEDED',
            'severity': 'MEDIUM',
            'detail': f'decision_changed_pct={decision_changed_pct_text}',
        })
    if permissive_divergence_pct > float(str(thresholds.get('divergence_warn_pct') or '0')):
        risk_flags.append({
            'flag': 'PERMISSIVE_DIVERGENCE_THRESHOLD_EXCEEDED',
            'severity': 'MEDIUM',
            'detail': f'permissive_divergence_pct={permissive_divergence_pct_text}',
        })
    if constructive_more_permissive_count > 0:
        risk_flags.append({
            'flag': 'CONSTRUCTIVE_MORE_PERMISSIVE_DECISIONS_PRESENT',
            'severity': 'HIGH',
            'detail': f'constructive_more_permissive_count={constructive_more_permissive_count}',
        })

    report = {
        'schema_id': 'candidate_policy_replay_report',
        'schema_version': 1,
        'produced_utc': f'{day}T00:00:00Z',
        'day_utc': day,
        'source_truth_root': str(source_truth_root),
        'replay_truth_root': str(replay_truth_root),
        'submission_id': submission_id,
        'candidate_policy': {
            'policy_version': str(candidate_policy.get('policy_version') or ''),
            'policy_source': str(candidate_policy.get('policy_source') or ''),
            'policy_input_hash': str(candidate_policy.get('policy_input_hash') or ''),
            'thresholds': dict(candidate_policy.get('thresholds') or {}),
        },
        'historical_override_analysis': historical_override_analysis,
        'proposal_comparisons': comparisons,
        'aggregate_metrics': {
            'proposal_count': proposal_count,
            'decision_changed_count': decision_changed_count,
            'decision_changed_pct': decision_changed_pct_text,
            'original_decision_counts': dict(sorted(original_decision_counts.items())),
            'candidate_decision_counts': dict(sorted(candidate_decision_counts.items())),
            'divergence_type_counts': dict(sorted(divergence_type_counts.items())),
            'auto_execute_delta_count': auto_execute_delta_count,
            'auto_execute_delta_pct': auto_execute_delta_pct_text,
            'block_delta_count': block_delta_count,
            'block_delta_pct': block_delta_pct_text,
            'override_reduction_count': override_reduction_count,
            'override_reduction_pct': override_reduction_pct_text,
            'mismatch_frequency': mismatch_frequency_text,
            'human_system_divergence_count': int(historical_override_analysis.get('human_system_divergence_count') or 0),
            'candidate_matches_human_resolution_count': candidate_matches_human_resolution_count,
            'original_block_reasons_distribution': dict(sorted(block_reasons_distribution.items())),
            'candidate_block_reasons_distribution': dict(sorted(candidate_block_reasons_distribution.items())),
            'new_risk_exposure_indicators': {
                'constructive_more_permissive_count': constructive_more_permissive_count,
                'protective_more_permissive_count': protective_more_permissive_count,
                'review_to_auto_count': review_to_auto_count,
                'block_to_auto_count': block_to_auto_count,
            },
        },
        'risk_flags': risk_flags,
        'status': 'FAIL' if reason_codes else 'OK',
        'reason_codes': sorted(set(reason_codes)),
        'canonical_json_hash': '',
    }
    report['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1(report)
    validate_against_repo_schema_v1(report, REPO_ROOT, CANDIDATE_POLICY_REPLAY_SCHEMA)
    return report


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
    ap.add_argument('--candidate_policy_json')
    ap.add_argument('--candidate_policy_version')
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    source_truth_root = _require_truth_root(args.source_truth_root, label='source_truth_root')
    replay_truth_root = Path(str(args.replay_truth_root).strip()).expanduser().resolve()
    submission_id = str(args.submission_id).strip()
    candidate_policy = _load_candidate_policy_profile(
        str(args.candidate_policy_json).strip() if args.candidate_policy_json else None,
        str(args.candidate_policy_version).strip() if args.candidate_policy_version else None,
    )

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
    operator_source_dir = source_truth_root / 'reports' / 'constitutional_operator_decision_v1' / day
    operator_replay_dir = replay_truth_root / 'reports' / 'constitutional_operator_decision_v1' / day
    if operator_source_dir.exists() or operator_replay_dir.exists():
        comparisons.append(
            _compare_path(
                artifact_role='operator_decision_dir',
                source_path=operator_source_dir,
                replay_path=operator_replay_dir,
            )
        )
    reason_codes = sorted(set(source_completeness + replay_completeness + _reason_codes(comparisons)))
    source_override_analysis = _collect_override_analysis(source_truth_root, day)
    replay_override_analysis = _collect_override_analysis(replay_truth_root, day)

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
        'override_analysis': {
            'comparison_status': 'identical' if source_override_analysis == replay_override_analysis else 'different',
            'source': source_override_analysis,
            'replay': replay_override_analysis,
        },
        'canonical_json_hash': '',
    }
    manifest['canonical_json_hash'] = canonical_hash_for_c2_artifact_v1(manifest)
    validate_against_repo_schema_v1(manifest, REPO_ROOT, SCHEMA)
    out_path = (replay_truth_root / 'reports' / 'replay_manifest_v1' / day / f'{submission_id}.replay_manifest.v1.json').resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(canonical_json_bytes_v1(manifest) + b'\n')
    candidate_report = _evaluate_candidate_policy_replay(
        source_truth_root=source_truth_root,
        replay_truth_root=replay_truth_root,
        day=day,
        submission_id=submission_id,
        candidate_policy=candidate_policy,
    )
    candidate_report_path = (
        replay_truth_root
        / 'reports'
        / 'candidate_policy_replay_report_v1'
        / day
        / f'{submission_id}.candidate_policy_replay_report.v1.json'
    ).resolve()
    candidate_report_path.parent.mkdir(parents=True, exist_ok=True)
    candidate_report_path.write_bytes(canonical_json_bytes_v1(candidate_report) + b'\n')
    if reason_codes or list(candidate_report.get('reason_codes') or []):
        print(
            format_failure_line(
                'run_runtime_replay_day_v1',
                classify_failure('|'.join(sorted(set(reason_codes + list(candidate_report.get('reason_codes') or []))))),
                manifest=str(out_path),
                candidate_report=str(candidate_report_path),
                reason_codes=sorted(set(reason_codes + list(candidate_report.get('reason_codes') or []))),
            ),
            file=sys.stderr,
        )
        return 2
    print(f'OK: REPLAY_MANIFEST_WRITTEN path={out_path}')
    print(f'OK: CANDIDATE_POLICY_REPLAY_REPORT_WRITTEN path={candidate_report_path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
