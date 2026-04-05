from __future__ import annotations

import argparse
import json
import subprocess
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

SOURCE_ROOT = ensure_repo_root_on_sys_path(__file__)

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.common.advisor_execution.action_policy_pack_v1 import ActionPolicyPackV1, ReplanTriggerRuleV1
from constellation_2.common.advisor_execution.blocked_action_v1 import BlockedActionV1
from constellation_2.common.advisor_execution.decision_plan_v1 import DecisionPlanV1
from constellation_2.common.advisor_execution.official_recommendation_set_v1 import OfficialRecommendationSetV1
from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1

ADVISOR_RUNTIME_ROOT = advisor_runtime_root()
CANONICAL_TOOLS = canonical_tools_root(__file__)


def _write(path: Path, obj: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes_v1(obj) + b'\n'
    if path.exists() and path.read_bytes() != payload:
        raise ValueError(f'IMMUTABLE_CONFLICT: {path}')
    path.write_bytes(payload)
    return path


def _runtime_path(output_root: str, mode: str, family: str, day_utc: str, filename: str) -> Path:
    return advisor_runtime_path(output_root, mode, family, day_utc, filename)


def _metadata(args: Any, refs: list[str]) -> dict[str, Any]:
    return {'produced_utc': args.produced_utc, 'day_utc': args.day_utc, 'mode': args.mode, 'source_artifact_refs': refs}


def _read_json(path: str) -> dict[str, Any]:
    with Path(path).expanduser().resolve().open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


def _planning(path: str) -> PlanningSnapshotV1:
    return PlanningSnapshotV1.load_file(path)


def _official(path: str) -> OfficialRecommendationSetV1:
    return OfficialRecommendationSetV1.load_file(path)


def _plan(path: str) -> DecisionPlanV1:
    return DecisionPlanV1.load_file(path)


def _pack(path: str) -> ActionPolicyPackV1:
    return ActionPolicyPackV1.load_file(path)


def _trigger(path: str) -> ReplanTriggerRuleV1:
    obj = _read_json(path)
    if obj.get('schema_id') == 'replan_trigger_rule':
        return ReplanTriggerRuleV1.from_dict(obj)
    pack = ActionPolicyPackV1.from_dict(obj)
    if not pack.replan_trigger_rules:
        raise ValueError('REPLAN_TRIGGER_RULE_MISSING')
    return pack.replan_trigger_rules[0]


def _blocked(path: str) -> list[BlockedActionV1]:
    obj = _read_json(path)
    if obj.get('schema_id') == 'blocked_action':
        return [BlockedActionV1.from_dict(obj)]
    if 'blocked_actions' in obj:
        return [BlockedActionV1.from_dict(item) for item in obj['blocked_actions']]
    raise ValueError('BLOCKED_ACTIONS_JSON_INVALID')


def _publication_gate_results(path: str, cls: Any) -> list[Any]:
    obj = _read_json(path)
    if obj.get('schema_id') == 'publication_gate_result':
        return [cls.from_dict(obj)]
    if 'results' in obj:
        return [cls.from_dict(item) for item in obj['results']]
    raise ValueError('PUBLICATION_GATE_JSON_INVALID')


def _call(script: str, args: list[str]) -> None:
    completed = subprocess.run([sys.executable, str(CANONICAL_TOOLS / script)] + args, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        raise ValueError(f'CANONICAL_TOOL_FAILED: {script}: {completed.stderr or completed.stdout}')
from constellation_2.common.advisor_kernel.household_timeline_service import build_household_timeline


def main() -> int:
    parser = argparse.ArgumentParser(description='Emit household_timeline_v1')
    parser.add_argument('--planning_snapshot_json', required=True)
    parser.add_argument('--mode', required=True)
    parser.add_argument('--day_utc', required=True)
    parser.add_argument('--produced_utc', required=True)
    parser.add_argument('--output_root', required=True)
    args = parser.parse_args()
    planning = _planning(args.planning_snapshot_json)
    metadata = _metadata(args, [f'planning_snapshot_id:{planning.planning_snapshot_id}'])
    artifact = build_household_timeline(planning_snapshot=planning, metadata=metadata)
    out = _write(_runtime_path(args.output_root, args.mode, 'household_timeline_v1', args.day_utc, 'household_timeline.v1.json'), artifact.to_dict())
    print(f'OK: {out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
