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
from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1
from constellation_2.common.advisor_kernel.capability_schedule_v1 import CapabilityScheduleV1


def build_capability_schedule(*, planning_snapshot: PlanningSnapshotV1, module_maturity_map: dict[str, str], prerequisite_map: dict[str, tuple[str, ...]], metadata: dict[str, Any]) -> CapabilityScheduleV1:
    capabilities = []
    for index, capability_id in enumerate(sorted(module_maturity_map), start=1):
        maturity = module_maturity_map[capability_id]
        if maturity not in {'observational_only', 'recommendation_support_only', 'action_policy_allowed', 'monitored_plan_allowed'}:
            raise ValueError(f'INVALID_MATURITY_LEVEL: {capability_id}')
        lane = 'view_lane' if maturity == 'observational_only' else 'decision_lane'
        capabilities.append({'capability_id': capability_id, 'lane': lane, 'maturity_level': maturity, 'prerequisites': list(prerequisite_map.get(capability_id, ())), 'eligible': True, 'blocked_reasons': [], 'execution_order': index})
    obj = build_base('capability_schedule', 'policy_authority', metadata, 'capability_schedule_v1')
    obj.update({'planning_snapshot_id': planning_snapshot.planning_snapshot_id, 'advisory_packet_id': planning_snapshot.advisory_packet_id, 'day_utc': metadata['day_utc'], 'mode': metadata['mode'], 'capabilities': capabilities, 'publication_ready': True})
    return CapabilityScheduleV1.from_dict(obj)
