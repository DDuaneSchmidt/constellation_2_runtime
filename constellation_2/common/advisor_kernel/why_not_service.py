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
from constellation_2.common.advisor_execution.blocked_action_v1 import BlockedActionV1
from constellation_2.common.advisor_execution.official_recommendation_set_v1 import OfficialRecommendationSetV1
from constellation_2.common.advisor_kernel.alternative_rejection_ledger_v1 import AlternativeRejectionLedgerV1
from constellation_2.common.advisor_kernel.why_not_reason_v1 import WhyNotReasonV1


def _selected(blocked_actions: list[BlockedActionV1]) -> BlockedActionV1:
    if not blocked_actions:
        raise ValueError('NO_BLOCKED_RECOMMENDATION')
    return sorted(blocked_actions, key=lambda item: (item.priority if hasattr(item, 'priority') else 999999, item.recommendation_id))[0]


def build_why_not_outputs(*, official_recommendation_set: OfficialRecommendationSetV1, blocked_actions: list[BlockedActionV1], metadata: dict[str, Any]) -> tuple[WhyNotReasonV1, AlternativeRejectionLedgerV1]:
    selected = _selected(blocked_actions)
    one = build_base('why_not_reason', 'view_authority', metadata, 'why_not_reason_v1')
    one.update({'support_status': selected.support_status, 'recommendation_id': selected.recommendation_id, 'why_not_reasons': list(selected.blockers)})
    two = build_base('alternative_rejection_ledger', 'view_authority', metadata, 'alternative_rejection_ledger_v1')
    two.update({'support_status': selected.support_status, 'recommendation_id': selected.recommendation_id, 'rejected_alternatives': [{'alternative_id': item.recommendation_id, 'reason_codes': list(item.blockers)} for item in blocked_actions if item.recommendation_id != selected.recommendation_id]})
    return WhyNotReasonV1.from_dict(one), AlternativeRejectionLedgerV1.from_dict(two)
