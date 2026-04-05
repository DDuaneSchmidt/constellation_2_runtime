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
from constellation_2.common.advisor_execution.official_recommendation_set_v1 import OfficialRecommendationSetV1
from constellation_2.common.advisor_execution.planning_snapshot_v1 import PlanningSnapshotV1
from constellation_2.common.advisor_kernel.advice_completeness_report_v1 import AdviceCompletenessReportV1
from constellation_2.common.advisor_kernel.domain_readiness_score_v1 import DomainReadinessScoreV1
from constellation_2.common.advisor_kernel.missing_input_priority_v1 import MissingInputPriorityV1


def build_readiness_outputs(*, planning_snapshot: PlanningSnapshotV1, official_recommendation_set: OfficialRecommendationSetV1 | None, metadata: dict[str, Any]) -> tuple[DomainReadinessScoreV1, MissingInputPriorityV1, AdviceCompletenessReportV1]:
    missing = []
    if not planning_snapshot.tax_profile.present:
        missing.append('tax_profile.present')
    score = 100 if planning_snapshot.liquidity.cash_cents >= planning_snapshot.spending.minimum_monthly_spending_cents * 12 else 50
    support = 'fully_supported' if official_recommendation_set is not None else 'provisional'
    one = build_base('domain_readiness_score', 'view_authority', metadata, 'domain_readiness_score_v1')
    one.update({'support_status': support, 'planning_snapshot_id': planning_snapshot.planning_snapshot_id, 'domain_id': 'liquidity', 'readiness_score': score, 'missing_inputs': missing, 'reason_codes': ['LIQUIDITY_DOMAIN_SELECTED']})
    two = build_base('missing_input_priority', 'view_authority', metadata, 'missing_input_priority_v1')
    two.update({'support_status': support, 'planning_snapshot_id': planning_snapshot.planning_snapshot_id, 'prioritized_inputs': [{'input_id': 'tax_profile.present', 'domain_id': 'tax', 'priority': 1, 'reason_codes': ['REQUIRED_FOR_TAXABLE_WITHDRAWAL']}] if missing else []})
    three = build_base('advice_completeness_report', 'view_authority', metadata, 'advice_completeness_report_v1')
    three.update({'support_status': support, 'planning_snapshot_id': planning_snapshot.planning_snapshot_id, 'overall_completeness_status': 'complete' if not missing else 'incomplete', 'missing_input_refs': missing, 'reason_codes': ['READINESS_COMPLETE'] if not missing else ['READINESS_INCOMPLETE']})
    return DomainReadinessScoreV1.from_dict(one), MissingInputPriorityV1.from_dict(two), AdviceCompletenessReportV1.from_dict(three)
