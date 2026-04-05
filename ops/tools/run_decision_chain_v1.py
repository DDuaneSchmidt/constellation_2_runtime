from __future__ import annotations

import argparse
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
from constellation_2.common.metadata_envelope_v1 import metadata_envelope_v1
from constellation_2.common.advisor_execution.decision_plan_delta_v1 import DecisionPlanDeltaV1
from constellation_2.common.advisor_execution.decision_plan_v1 import DecisionPlanV1
from constellation_2.common.advisor_execution.official_recommendation_set_v1 import OfficialRecommendationSetV1
from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1
from constellation_2.common.advisor_bridge.advisor_trade_intent_proposal_v1 import AdvisorTradeIntentProposalV1
from constellation_2.common.advisor_bridge.advisor_trade_translation_v1 import AdvisorTradeTranslationV1
from constellation_2.common.advisor_bridge.promotion_candidate_v1 import PromotionCandidateV1
from constellation_2.common.advisor_bridge.promotion_gate_result_v1 import PromotionGateResultV1
from constellation_2.common.advisor_bridge.promotion_manual_review_v1 import PromotionManualReviewV1
from constellation_2.common.advisor_bridge.promotion_review_v1 import PromotionReviewV1
from constellation_2.common.decision_chain_service import build_decision_chain

ADVISOR_RUNTIME_ROOT = advisor_runtime_root()


def _write(path: Path, obj: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes_v1(obj) + b"\n"
    if path.exists() and path.read_bytes() != payload:
        raise ValueError(f'IMMUTABLE_CONFLICT: {path}')
    path.write_bytes(payload)
    return path


def _runtime_path(output_root: str, mode: str, day_utc: str) -> Path:
    return advisor_runtime_path(output_root, mode, 'reports', 'decision_chain_v1', day_utc, 'decision_chain.v1.json')


def _opt(path: str, cls: Any):
    return None if path in {'', 'null', 'None'} else cls.load_file(path)


def main() -> int:
    ap = argparse.ArgumentParser(description='Emit decision_chain.v1.json')
    ap.add_argument('--planning_snapshot_json', required=True)
    ap.add_argument('--official_recommendation_set_json', required=True)
    ap.add_argument('--decision_plan_json', required=True)
    ap.add_argument('--decision_plan_delta_json', default='')
    ap.add_argument('--advisor_trade_translation_json', default='')
    ap.add_argument('--advisor_trade_intent_proposal_json', default='')
    ap.add_argument('--promotion_candidate_json', default='')
    ap.add_argument('--promotion_review_json', default='')
    ap.add_argument('--promotion_manual_review_json', default='')
    ap.add_argument('--promotion_gate_result_json', default='')
    ap.add_argument('--mode', required=True)
    ap.add_argument('--day_utc', required=True)
    ap.add_argument('--produced_utc', required=True)
    ap.add_argument('--output_root', required=True)
    args = ap.parse_args()
    planning = PlanningSnapshotV1.load_file(args.planning_snapshot_json)
    official = OfficialRecommendationSetV1.load_file(args.official_recommendation_set_json)
    plan = DecisionPlanV1.load_file(args.decision_plan_json)
    delta = _opt(args.decision_plan_delta_json, DecisionPlanDeltaV1)
    translation = _opt(args.advisor_trade_translation_json, AdvisorTradeTranslationV1)
    proposal = _opt(args.advisor_trade_intent_proposal_json, AdvisorTradeIntentProposalV1)
    candidate = _opt(args.promotion_candidate_json, PromotionCandidateV1)
    review = _opt(args.promotion_review_json, PromotionReviewV1)
    manual_review = _opt(args.promotion_manual_review_json, PromotionManualReviewV1)
    gate_result = _opt(args.promotion_gate_result_json, PromotionGateResultV1)
    refs = [f'planning_snapshot_id:{planning.planning_snapshot_id}', f'official_recommendation_set_id:{official.advisory_packet_id}', f'decision_plan_id:{plan.plan_id}']
    if delta is not None:
        refs.append(f'decision_plan_delta_id:{delta.plan_delta_id}')
    if translation is not None:
        refs.append(f'bridge_translation_id:{translation.run_id}')
    if proposal is not None:
        refs.append(f'bridge_proposal_id:{proposal.proposal_id}')
    if candidate is not None:
        refs.append(f'promotion_candidate_id:{candidate.candidate_id}')
    if review is not None:
        refs.append(f'promotion_review_id:{review.review_id}')
    if manual_review is not None:
        refs.append(f'promotion_manual_review_id:{manual_review.manual_review_id}')
    if gate_result is not None:
        refs.append(f'promotion_gate_result_id:{gate_result.gate_result_id}')
    env = metadata_envelope_v1(
        produced_utc=args.produced_utc,
        day_utc=args.day_utc,
        mode=args.mode,
        source_artifact_refs=refs,
        artifact_family='decision_chain_v1',
    )
    out = _write(
        _runtime_path(args.output_root, args.mode, args.day_utc),
        build_decision_chain(
            planning_snapshot=planning,
            official_recommendation_set=official,
            decision_plan=plan,
            envelope=env,
            decision_plan_delta=delta,
            bridge_translation=translation,
            bridge_proposal=proposal,
            promotion_candidate=candidate,
            promotion_review=review,
            promotion_manual_review=manual_review,
            promotion_gate_result=gate_result,
        ).to_dict(),
    )
    print(f'OK: {out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
