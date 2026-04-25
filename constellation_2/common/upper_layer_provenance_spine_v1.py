from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Sequence

from constellation_2.common.upper_layer_shared_v1 import (
    REPO_ROOT,
    artifact_ref_from_path_v1,
    core2_refs_v1,
    day_utc_from_timestamp_v1,
    repo_artifact_ref_v1,
    resolve_upper_provenance_dir_v1,
    stable_unique_codes_v1,
    trade_identity_pointer_v1,
    write_validated_payload_v1,
    UpperCore2BundleV1,
)


UPPER_LAYER_PROVENANCE_SCHEMA_RELPATH = 'governance/04_DATA/SCHEMAS/C2/TRADE_STATE/upper_layer_provenance_spine.v1.schema.json'

RC_LOWER_CORE_REF_MISSING = 'LOWER_CORE_REF_MISSING'
RC_EXECUTABLE_REENTRY_PATH_MISSING = 'EXECUTABLE_REENTRY_PATH_MISSING'
RC_PROVENANCE_INCOMPLETE = 'PROVENANCE_INCOMPLETE'
RC_CROSS_PLANE_REF_INCONSISTENT = 'CROSS_PLANE_REF_INCONSISTENT'

CORE3_CONTRACT_RELPATH = 'governance/05_CONTRACTS/C2/lifecycle_action_authority_v1.contract.md'
CORE4_CONTRACT_RELPATH = 'governance/05_CONTRACTS/C2/post_entry_submit_boundary_v1.contract.md'


@dataclass(frozen=True)
class UpperLayerProvenanceMaterializationV1:
    path: Path
    payload: Dict[str, Any]


def _ref_has_payload(ref: Dict[str, Any]) -> bool:
    return bool(str(ref.get('artifact_path') or '').strip() and str(ref.get('artifact_sha256') or '').strip())


def build_upper_layer_provenance_spine_payload_v1(
    *,
    artifact_family: str,
    artifact_id: str,
    core2_bundle: UpperCore2BundleV1,
    evaluated_at_utc: str,
    core3_refs: Sequence[Dict[str, str]] | None = None,
    core4_refs: Sequence[Dict[str, str]] | None = None,
    core5_refs: Sequence[Dict[str, str]] | None = None,
    cross_upper_plane_refs: Sequence[Dict[str, str]] | None = None,
    rule_versions: Dict[str, str] | None = None,
) -> Dict[str, Any]:
    lower_core_refs = core2_refs_v1(core2_bundle)
    blocker_codes: list[str] = []
    degraded_codes: list[str] = []

    if not all(_ref_has_payload(ref) for ref in lower_core_refs.values()):
        blocker_codes.append(RC_LOWER_CORE_REF_MISSING)
    if not rule_versions:
        degraded_codes.append(RC_PROVENANCE_INCOMPLETE)

    cross_refs = list(cross_upper_plane_refs or [])
    for ref in cross_refs:
        if not _ref_has_payload(ref):
            degraded_codes.append(RC_CROSS_PLANE_REF_INCONSISTENT)
            break

    current_core3_refs = list(core3_refs or [])
    current_core4_refs = list(core4_refs or [])
    if not current_core3_refs or not current_core4_refs:
        degraded_codes.append(RC_EXECUTABLE_REENTRY_PATH_MISSING)

    if blocker_codes:
        provenance_status = 'BLOCKED'
    elif degraded_codes:
        provenance_status = 'DEGRADED'
    else:
        provenance_status = 'COMPLETE'

    return {
        'schema_id': 'upper_layer_provenance_spine',
        'schema_version': 'v1',
        'authority_owner': 'upper_layer_provenance_spine_v1',
        'artifact_family': str(artifact_family),
        'artifact_id': str(artifact_id),
        'day_utc': day_utc_from_timestamp_v1(evaluated_at_utc),
        'evaluated_at_utc': str(evaluated_at_utc),
        'trade_identity_ref': trade_identity_pointer_v1(core2_bundle),
        'core2_refs': lower_core_refs,
        'core3_refs': list(current_core3_refs),
        'core4_refs': list(current_core4_refs),
        'core5_refs': list(core5_refs or []),
        'cross_upper_plane_refs': cross_refs,
        'executable_reentry_path_refs': {
            'reentry_required': True,
            'reentry_status': 'CURRENT_PATH_COMPLETE' if current_core3_refs and current_core4_refs else 'REQUIRED_BEFORE_EXECUTION',
            'core3_contract_ref': repo_artifact_ref_v1(CORE3_CONTRACT_RELPATH),
            'core4_contract_ref': repo_artifact_ref_v1(CORE4_CONTRACT_RELPATH),
            'current_core3_refs': list(current_core3_refs),
            'current_core4_refs': list(current_core4_refs),
        },
        'rule_versions': dict(rule_versions or {'upper_layer_provenance_spine': 'v1'}),
        'provenance_status': provenance_status,
        'blocker_codes': stable_unique_codes_v1(blocker_codes),
        'degraded_codes': stable_unique_codes_v1(degraded_codes),
    }


def materialize_upper_layer_provenance_spine_v1(
    *,
    execution_root: Path,
    artifact_family: str,
    artifact_id: str,
    core2_bundle: UpperCore2BundleV1,
    evaluated_at_utc: str,
    core3_refs: Sequence[Dict[str, str]] | None = None,
    core4_refs: Sequence[Dict[str, str]] | None = None,
    core5_refs: Sequence[Dict[str, str]] | None = None,
    cross_upper_plane_refs: Sequence[Dict[str, str]] | None = None,
    rule_versions: Dict[str, str] | None = None,
) -> UpperLayerProvenanceMaterializationV1:
    payload = build_upper_layer_provenance_spine_payload_v1(
        artifact_family=artifact_family,
        artifact_id=artifact_id,
        core2_bundle=core2_bundle,
        evaluated_at_utc=evaluated_at_utc,
        core3_refs=core3_refs,
        core4_refs=core4_refs,
        core5_refs=core5_refs,
        cross_upper_plane_refs=cross_upper_plane_refs,
        rule_versions=rule_versions,
    )
    out_dir = resolve_upper_provenance_dir_v1(
        execution_root=execution_root,
        day_utc=payload['day_utc'],
        artifact_family=artifact_family,
        artifact_id=artifact_id,
    )
    ref = write_validated_payload_v1(
        path=out_dir / 'upper_layer_provenance_spine.v1.json',
        payload=payload,
        schema_relpath=UPPER_LAYER_PROVENANCE_SCHEMA_RELPATH,
    )
    return UpperLayerProvenanceMaterializationV1(path=ref.path, payload=payload)
