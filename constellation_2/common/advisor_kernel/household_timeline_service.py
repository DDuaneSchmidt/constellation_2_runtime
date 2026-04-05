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
from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1
from constellation_2.common.advisor_kernel.household_timeline_v1 import HouseholdTimelineV1


def build_household_timeline(*, planning_snapshot: PlanningSnapshotV1, metadata: dict[str, Any]) -> HouseholdTimelineV1:
    obj = build_base('household_timeline', 'view_authority', metadata, 'household_timeline_v1')
    obj.update({'planning_snapshot_id': planning_snapshot.planning_snapshot_id, 'milestones': [f"annuity:{item.annuity_id}:{item.phase}" for item in planning_snapshot.annuities], 'phase_rules_applied': ['timeline_v1']})
    return HouseholdTimelineV1.from_dict(obj)
