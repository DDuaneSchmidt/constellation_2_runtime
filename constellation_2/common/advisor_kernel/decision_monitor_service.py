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
from constellation_2.common.advisor_execution.action_policy_pack_v1 import ReplanTriggerRuleV1
from constellation_2.common.advisor_execution.decision_plan_v1 import DecisionPlanV1
from constellation_2.common.advisor_kernel.decision_monitor_state_v1 import DecisionMonitorStateV1


def build_decision_monitor_state(*, decision_plan: DecisionPlanV1, replan_trigger_rule: ReplanTriggerRuleV1, metadata: dict[str, Any]) -> DecisionMonitorStateV1:
    obj = build_base('decision_monitor_state', 'monitor_authority', metadata, 'decision_monitor_state_v1')
    obj.update({'decision_plan_id': decision_plan.plan_id, 'monitored_trigger_rule_version': 'v1', 'monitor_status': 'monitoring', 'reason_codes': [replan_trigger_rule.trigger_type]})
    return DecisionMonitorStateV1.from_dict(obj)
