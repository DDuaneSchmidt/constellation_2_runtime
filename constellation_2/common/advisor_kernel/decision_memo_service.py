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
from constellation_2.common.advisor_execution.blocked_action_v1 import BlockedActionV1
from constellation_2.common.advisor_execution.decision_plan_v1 import DecisionPlanV1
from constellation_2.common.advisor_execution.official_recommendation_set_v1 import OfficialRecommendationSetV1
from constellation_2.common.advisor_kernel.alternative_rejection_ledger_v1 import AlternativeRejectionLedgerV1
from constellation_2.common.advisor_kernel.decision_memo_v1 import DecisionMemoV1
from constellation_2.common.advisor_kernel.publication_gate_result_v1 import PublicationGateResultV1
from constellation_2.common.advisor_kernel.semantic_reconciliation_report_v1 import SemanticReconciliationReportV1
from constellation_2.common.advisor_kernel.why_not_reason_v1 import WhyNotReasonV1
from constellation_2.common.advisor_kernel.why_not_service import build_why_not_outputs


def build_decision_memo(*, decision_plan: DecisionPlanV1, semantic_reconciliation_report: SemanticReconciliationReportV1, publication_gate_results: list[PublicationGateResultV1], official_recommendation_set: OfficialRecommendationSetV1, blocked_actions: list[BlockedActionV1], metadata: dict[str, Any]) -> tuple[WhyNotReasonV1, AlternativeRejectionLedgerV1, DecisionMemoV1]:
    why_not, ledger = build_why_not_outputs(official_recommendation_set=official_recommendation_set, blocked_actions=blocked_actions, metadata=metadata)
    memo = build_base('decision_memo', 'view_authority', metadata, 'decision_memo_v1')
    memo.update({'support_status': semantic_reconciliation_report.support_status, 'decision_plan_id': decision_plan.plan_id, 'memo_sections': [f"plan:{decision_plan.plan_id}", f"semantic:{semantic_reconciliation_report.overall_status}", f"why_not:{why_not.recommendation_id}"], 'source_refs': [item.artifact_family for item in publication_gate_results] + [why_not.recommendation_id]})
    return why_not, ledger, DecisionMemoV1.from_dict(memo)
