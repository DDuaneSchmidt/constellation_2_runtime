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
from constellation_2.common.advisor_kernel.income_floor_timeline_v1 import IncomeFloorTimelineV1


def build_income_floor_timeline(*, planning_snapshot: PlanningSnapshotV1, metadata: dict[str, Any]) -> IncomeFloorTimelineV1:
    gap = max(0, planning_snapshot.spending.monthly_spending_cents - planning_snapshot.income.guaranteed_monthly_income_cents)
    obj = build_base('income_floor_timeline', 'fact_authority', metadata, 'income_floor_timeline_v1')
    obj.update({'planning_snapshot_id': planning_snapshot.planning_snapshot_id, 'guaranteed_monthly_income_cents': planning_snapshot.income.guaranteed_monthly_income_cents, 'required_monthly_spending_cents': planning_snapshot.spending.monthly_spending_cents, 'floor_gap_monthly_cents': gap, 'timeline': [f'gap:{gap}'], 'reason_codes': ['INCOME_FLOOR_GAP_PRESENT'] if gap > 0 else ['INCOME_FLOOR_COVERED']})
    return IncomeFloorTimelineV1.from_dict(obj)
