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
from constellation_2.common.advisor_execution.decision_plan_v1 import DecisionPlanV1
from constellation_2.common.advisor_kernel.quarterly_action_summary_v1 import QuarterlyActionSummaryV1


def build_quarterly_action_summary(*, decision_plan: DecisionPlanV1, metadata: dict[str, Any]) -> QuarterlyActionSummaryV1:
    month = int(metadata['day_utc'][5:7])
    quarter = ((month - 1) // 3) + 1
    obj = build_base('quarterly_action_summary', 'view_authority', metadata, 'quarterly_action_summary_v1')
    obj.update({'decision_plan_id': decision_plan.plan_id, 'quarter_label': f"{metadata['day_utc'][:4]}-Q{quarter}", 'action_count': len(decision_plan.actions), 'blocked_action_count': len(decision_plan.blocked_actions), 'summary_lines': [f"actions:{len(decision_plan.actions)}", f"blocked:{len(decision_plan.blocked_actions)}"]})
    return QuarterlyActionSummaryV1.from_dict(obj)
