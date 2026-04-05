from __future__ import annotations

import argparse
import sys
from pathlib import Path

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

from constellation_2.common.advisor_execution.decision_plan_delta_service import build_decision_plan_delta
from constellation_2.common.advisor_execution.decision_plan_v1 import DecisionPlanV1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(obj) + b'\n')


def main() -> int:
    parser = argparse.ArgumentParser(description='Build decision_plan_delta_v1 from two governed decision plans')
    parser.add_argument('--previous_plan_json', required=True)
    parser.add_argument('--current_plan_json', required=True)
    parser.add_argument('--out_json', required=True)
    args = parser.parse_args()

    previous_plan = DecisionPlanV1.load_file(args.previous_plan_json)
    current_plan = DecisionPlanV1.load_file(args.current_plan_json)
    delta = build_decision_plan_delta(previous_plan=previous_plan, current_plan=current_plan)
    out_path = Path(args.out_json).expanduser().resolve()
    _write_json(out_path, delta.to_dict())
    print(f'OK: DECISION_PLAN_DELTA_WRITTEN path={out_path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
