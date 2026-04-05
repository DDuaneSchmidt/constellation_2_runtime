from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

SOURCE_ROOT = Path('/home/node/constellation')
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
from constellation_2.common.advisor_execution.official_recommendation_set_v1 import OfficialRecommendationSetV1
from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1
from constellation_2.common.advisor_kernel.decision_comparison_set_v1 import DecisionComparisonSetV1


def build_decision_comparison_set(*, planning_snapshot: PlanningSnapshotV1, official_recommendation_set: OfficialRecommendationSetV1, metadata: dict[str, Any]) -> DecisionComparisonSetV1:
    option_ids = [item.recommendation_id for item in sorted(official_recommendation_set.recommendations, key=lambda row: (row.priority, row.recommendation_id))]
    obj = build_base('decision_comparison_set', 'recommendation_authority', metadata, 'decision_comparison_set_v1')
    obj.update({'planning_snapshot_id': planning_snapshot.planning_snapshot_id, 'advisory_packet_id': official_recommendation_set.advisory_packet_id, 'option_ids': option_ids, 'support_basis': 'official_recommendation_set_v1', 'reason_codes': ['DETERMINISTIC_ORDERING_APPLIED']})
    return DecisionComparisonSetV1.from_dict(obj)
