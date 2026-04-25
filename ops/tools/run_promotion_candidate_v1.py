from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

raise SystemExit('LEGACY_PROMOTION_CANDIDATE_DISABLED_USE_ADVISORY_KERNEL')

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
from constellation_2.common.metadata_envelope_v1 import metadata_envelope_v1
from constellation_2.common.advisor_execution.decision_plan_v1 import DecisionPlanV1
from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1
from constellation_2.common.advisor_bridge.advisor_trade_intent_proposal_v1 import AdvisorTradeIntentProposalV1
from constellation_2.common.advisor_bridge.advisor_trade_translation_v1 import AdvisorTradeTranslationV1
from constellation_2.common.advisor_bridge.promotion_plane_service import build_promotion_candidate

ADVISOR_RUNTIME_ROOT = advisor_runtime_root()

def _write(path: Path, obj: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes_v1(obj) + b"\n"
    if path.exists() and path.read_bytes() != payload:
        raise ValueError(f'IMMUTABLE_CONFLICT: {path}')
    path.write_bytes(payload)
    return path

def _runtime_path(output_root: str, mode: str, day_utc: str) -> Path:
    return advisor_runtime_path(output_root, mode, 'promotion_candidate_v1', day_utc, 'promotion_candidate.v1.json')

def main() -> int:
    ap = argparse.ArgumentParser(description='Emit promotion_candidate.v1.json')
    ap.add_argument('--planning_snapshot_json', required=True)
    ap.add_argument('--decision_plan_json', required=True)
    ap.add_argument('--advisor_trade_translation_json', required=True)
    ap.add_argument('--advisor_trade_intent_proposal_json', required=True)
    ap.add_argument('--mode', required=True)
    ap.add_argument('--day_utc', required=True)
    ap.add_argument('--produced_utc', required=True)
    ap.add_argument('--output_root', required=True)
    args = ap.parse_args()
    planning = PlanningSnapshotV1.load_file(args.planning_snapshot_json)
    plan = DecisionPlanV1.load_file(args.decision_plan_json)
    translation = AdvisorTradeTranslationV1.load_file(args.advisor_trade_translation_json)
    proposal = AdvisorTradeIntentProposalV1.load_file(args.advisor_trade_intent_proposal_json)
    env = metadata_envelope_v1(produced_utc=args.produced_utc, day_utc=args.day_utc, mode=args.mode, source_artifact_refs=[f'planning_snapshot_id:{planning.planning_snapshot_id}', f'decision_plan_id:{plan.plan_id}', f'bridge_translation_id:{translation.run_id}', f'bridge_proposal_id:{proposal.proposal_id}'], artifact_family='promotion_candidate_v1')
    out = _write(_runtime_path(args.output_root, args.mode, args.day_utc), build_promotion_candidate(planning_snapshot=planning, decision_plan=plan, translation=translation, proposal=proposal, envelope=env).to_dict())
    print(f'OK: {out}')
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
