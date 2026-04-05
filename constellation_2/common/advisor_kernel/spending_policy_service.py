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
from constellation_2.common.advisor_kernel.spending_policy_v1 import SpendingPolicyV1


def build_spending_policy(*, planning_snapshot: PlanningSnapshotV1, metadata: dict[str, Any]) -> SpendingPolicyV1:
    required = 12
    status = 'active' if planning_snapshot.liquidity.cash_cents < planning_snapshot.spending.minimum_monthly_spending_cents * required else 'satisfied'
    obj = build_base('spending_policy', 'policy_authority', metadata, 'spending_policy_v1')
    obj.update({'planning_snapshot_id': planning_snapshot.planning_snapshot_id, 'liquidity_months_required': required, 'minimum_spending_cents': planning_snapshot.spending.minimum_monthly_spending_cents, 'cash_available_cents': planning_snapshot.liquidity.cash_cents, 'status': status, 'reason_codes': ['LIQUIDITY_BELOW_REQUIRED_MONTHS'] if status == 'active' else ['LIQUIDITY_REQUIREMENT_MET']})
    return SpendingPolicyV1.from_dict(obj)
