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
from constellation_2.common.advisor_kernel.plan_health_snapshot_v1 import PlanHealthSnapshotV1
from constellation_2.common.advisor_kernel.semantic_reconciliation_report_v1 import SemanticReconciliationReportV1


def build_plan_health_snapshot(*, decision_plan: DecisionPlanV1, semantic_report: SemanticReconciliationReportV1, metadata: dict[str, Any]) -> PlanHealthSnapshotV1:
    issues = [item.check_id for item in semantic_report.checks if item.status == 'fail']
    obj = build_base('plan_health_snapshot', 'view_authority', metadata, 'plan_health_snapshot_v1')
    obj.update({'support_status': semantic_report.support_status, 'decision_plan_id': decision_plan.plan_id, 'health_status': 'healthy' if semantic_report.overall_status == 'pass' else 'attention_required', 'top_issues': issues, 'reason_codes': ['SEMANTIC_OK'] if not issues else ['SEMANTIC_ATTENTION_REQUIRED']})
    return PlanHealthSnapshotV1.from_dict(obj)
