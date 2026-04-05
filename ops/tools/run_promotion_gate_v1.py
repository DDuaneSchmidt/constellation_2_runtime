from __future__ import annotations

import argparse
import json
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
from constellation_2.common.authority_registry_v1 import AuthorityRegistryV1
from constellation_2.common.metadata_envelope_v1 import metadata_envelope_v1
from constellation_2.common.advisor_bridge.promotion_candidate_v1 import PromotionCandidateV1
from constellation_2.common.advisor_bridge.promotion_gate_service import build_promotion_gate_result
from constellation_2.common.advisor_bridge.promotion_manual_review_v1 import PromotionManualReviewV1
from constellation_2.common.advisor_bridge.promotion_review_v1 import PromotionReviewV1

ADVISOR_RUNTIME_ROOT = advisor_runtime_root()


def _write(path: Path, obj: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes_v1(obj) + b"\n"
    if path.exists() and path.read_bytes() != payload:
        raise ValueError(f'IMMUTABLE_CONFLICT: {path}')
    path.write_bytes(payload)
    return path


def _runtime_path(output_root: str, mode: str, day_utc: str) -> Path:
    return advisor_runtime_path(output_root, mode, 'promotion_gate_result_v1', day_utc, 'promotion_gate_result.v1.json')


def _read_json(path: str | Path) -> dict[str, Any]:
    with Path(path).expanduser().resolve().open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


def _authority_registry_path(output_root: str, mode: str, day_utc: str) -> Path:
    return advisor_runtime_path(output_root, mode, 'reports', 'authority_registry_v1', day_utc, 'authority_registry.v1.json')


def _enforce_authority_registry(*, output_root: str, mode: str, day_utc: str) -> None:
    family = 'promotion_gate_result_v1'
    registry_path = _authority_registry_path(output_root, mode, day_utc)
    if not registry_path.exists():
        raise ValueError(f'AUTHORITY_REGISTRY_MISSING: {registry_path}')
    registry = AuthorityRegistryV1.from_dict(_read_json(registry_path))
    row = next((item for item in registry.rows if item.artifact_family == family), None)
    if row is None:
        raise ValueError(f'AUTHORITY_REGISTRY_ROW_MISSING:{family}')
    if row.owner_plane != 'promotion_plane':
        raise ValueError(f'AUTHORITY_REGISTRY_OWNER_PLANE_MISMATCH:{family}')
    if row.authority_class != 'recommendation_authority':
        raise ValueError(f'AUTHORITY_REGISTRY_AUTHORITY_CLASS_MISMATCH:{family}')
    if not row.publication_required:
        raise ValueError(f'AUTHORITY_REGISTRY_PUBLICATION_FORBIDDEN:{family}')


def main() -> int:
    ap = argparse.ArgumentParser(description='Emit promotion_gate_result.v1.json')
    ap.add_argument('--promotion_candidate_json', required=True)
    ap.add_argument('--promotion_review_json', required=True)
    ap.add_argument('--promotion_manual_review_json', required=True)
    ap.add_argument('--mode', required=True)
    ap.add_argument('--day_utc', required=True)
    ap.add_argument('--produced_utc', required=True)
    ap.add_argument('--output_root', required=True)
    args = ap.parse_args()
    candidate = PromotionCandidateV1.from_dict(_read_json(args.promotion_candidate_json))
    review = PromotionReviewV1.from_dict(_read_json(args.promotion_review_json))
    manual_review = PromotionManualReviewV1.load_file(args.promotion_manual_review_json)
    if review.review_status != 'review_required':
        raise ValueError('PROMOTION_REVIEW_NOT_REVIEW_REQUIRED')
    env = metadata_envelope_v1(
        produced_utc=args.produced_utc,
        day_utc=args.day_utc,
        mode=args.mode,
        source_artifact_refs=[f'promotion_candidate_id:{candidate.candidate_id}', f'promotion_review_id:{review.review_id}', f'promotion_manual_review_id:{manual_review.manual_review_id}'],
        artifact_family='promotion_gate_result_v1',
        selection_basis=manual_review.manual_review_status,
    )
    _enforce_authority_registry(output_root=args.output_root, mode=args.mode, day_utc=args.day_utc)
    out = _write(
        _runtime_path(args.output_root, args.mode, args.day_utc),
        build_promotion_gate_result(candidate=candidate, review=review, manual_review=manual_review, envelope=env).to_dict(),
    )
    print(f'OK: {out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
