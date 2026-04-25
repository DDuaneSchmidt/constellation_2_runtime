from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

raise SystemExit('LEGACY_ADVISOR_TRADE_INTENT_PROPOSAL_DISABLED_USE_ADVISORY_KERNEL')

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
from constellation_2.common.advisor_execution.decision_plan_v1 import DecisionPlanV1
from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1
from constellation_2.common.advisor_bridge.advisor_trade_bridge_service import build_advisor_trade_intent_proposal

ADVISOR_RUNTIME_ROOT = advisor_runtime_root()


def _write(path: Path, obj: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes_v1(obj) + b"\n"
    if path.exists() and path.read_bytes() != payload:
        raise ValueError(f"IMMUTABLE_CONFLICT: {path}")
    path.write_bytes(payload)
    return path


def _runtime_path(output_root: str, mode: str, family: str, day_utc: str, filename: str) -> Path:
    return advisor_runtime_path(output_root, mode, family, day_utc, filename)


def _metadata(args: Any, planning_snapshot: PlanningSnapshotV1, decision_plan: DecisionPlanV1) -> dict[str, Any]:
    return {
        "produced_utc": args.produced_utc,
        "day_utc": args.day_utc,
        "mode": args.mode,
        "source_artifact_refs": [
            f"planning_snapshot_id:{planning_snapshot.planning_snapshot_id}",
            f"decision_plan_id:{decision_plan.plan_id}",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Emit advisor_trade_intent_proposal.v1.json")
    parser.add_argument("--decision_plan_json", required=True)
    parser.add_argument("--planning_snapshot_json", required=True)
    parser.add_argument("--mode", required=True)
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--produced_utc", required=True)
    parser.add_argument("--output_root", required=True)
    args = parser.parse_args()
    decision_plan = DecisionPlanV1.load_file(args.decision_plan_json)
    planning_snapshot = PlanningSnapshotV1.load_file(args.planning_snapshot_json)
    metadata = _metadata(args, planning_snapshot, decision_plan)
    artifact = build_advisor_trade_intent_proposal(decision_plan=decision_plan, planning_snapshot=planning_snapshot, metadata=metadata)
    out = _write(_runtime_path(args.output_root, args.mode, "advisor_trade_intent_proposal_v1", args.day_utc, "advisor_trade_intent_proposal.v1.json"), artifact.to_dict())
    print(f"OK: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
