from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

SOURCE_ROOT = Path('/home/node/constellation_2_clean')
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1, canonical_sha256_hex_v1


def compute_run_id(*, artifact_family: str, metadata: dict[str, Any]) -> str:
    payload = {
        'day_utc': metadata['day_utc'],
        'mode': metadata['mode'],
        'artifact_family': artifact_family,
        'source_artifact_refs': sorted(metadata['source_artifact_refs']),
    }
    return canonical_sha256_hex_v1(payload)


def build_base(schema_id: str, authority_class: str, metadata: dict[str, Any], artifact_family: str) -> dict[str, Any]:
    return {
        'schema_id': schema_id,
        'schema_version': 'v1',
        'authority_class': authority_class,
        'support_status': 'fully_supported',
        'produced_utc': metadata['produced_utc'],
        'run_id': compute_run_id(artifact_family=artifact_family, metadata=metadata),
    }
from constellation_2.common.advisor_kernel.publication_gate_result_v1 import PublicationGateResultV1


def evaluate_publication(*, artifact_family: str, artifact_ref: str, authority_class: str, support_status: str, semantic_status: str, upstream_publication_passed: bool, lane: str, metadata: dict[str, Any]) -> PublicationGateResultV1:
    if lane == 'decision_lane' and authority_class == 'view_authority':
        raise ValueError('VIEW_ARTIFACT_NOT_DECISION_AUTHORITATIVE')
    publication_status = 'publishable' if semantic_status == 'pass' and upstream_publication_passed and support_status != 'blocked' else 'blocked'
    obj = build_base('publication_gate_result', 'policy_authority', metadata, 'publication_gate_result_v1')
    obj.update({'artifact_family': artifact_family, 'artifact_ref': artifact_ref, 'publication_status': publication_status, 'publication_class': 'view_only' if authority_class == 'view_authority' else 'decision_authoritative', 'reason_codes': [] if publication_status == 'publishable' else ['PUBLICATION_GATE_DENIED']})
    return PublicationGateResultV1.from_dict(obj)
