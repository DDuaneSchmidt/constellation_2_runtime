from __future__ import annotations

import json
from pathlib import Path

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.common.advisory.assumption_manifest_v1 import AssumptionManifestV1
from constellation_2.common.advisory.execution_intent_v1 import ExecutionIntentV1
from constellation_2.common.advisory.household_snapshot_v1 import HouseholdSnapshotV1
from constellation_2.common.advisory.investor_intent_v1 import InvestorIntentV1
from constellation_2.common.advisory.kernel_run_envelope_v1 import KernelRunEnvelopeV1
from constellation_2.common.advisory.policy_v1 import PolicyV1
from constellation_2.common.advisory.portfolio_intent_v1 import PortfolioIntentV1
from constellation_2.common.advisory.promotion_decision_v1 import PromotionDecisionV1
from constellation_2.common.advisor_bridge.promotion_record_v2 import PromotionRecordV2


def advisory_authority_root_v1(output_root: str | Path) -> Path:
    return Path(output_root).expanduser().resolve()


def investor_intent_dir_v1(output_root: str | Path, household_id: str) -> Path:
    return advisory_authority_root_v1(output_root) / 'authorities' / 'investor_intent_v1' / 'households' / str(household_id)


def investor_intent_path_v1(output_root: str | Path, household_id: str, intent_id: str) -> Path:
    return investor_intent_dir_v1(output_root, household_id) / f'{intent_id}.investor_intent.v1.json'


def policy_dir_v1(output_root: str | Path, household_id: str) -> Path:
    return advisory_authority_root_v1(output_root) / 'authorities' / 'policy_v1' / 'households' / str(household_id)


def policy_path_v1(output_root: str | Path, household_id: str, policy_id: str) -> Path:
    return policy_dir_v1(output_root, household_id) / f'{policy_id}.policy.v1.json'


def assumption_manifest_dir_v1(output_root: str | Path) -> Path:
    return advisory_authority_root_v1(output_root) / 'authorities' / 'assumption_manifest_v1' / 'manifests'


def assumption_manifest_path_v1(output_root: str | Path, assumption_manifest_id: str) -> Path:
    return assumption_manifest_dir_v1(output_root) / f'{assumption_manifest_id}.assumption_manifest.v1.json'


def household_snapshot_dir_v1(output_root: str | Path, household_id: str) -> Path:
    return advisory_authority_root_v1(output_root) / 'authorities' / 'household_snapshot_v1' / 'households' / str(household_id)


def household_snapshot_path_v1(output_root: str | Path, household_id: str, household_snapshot_id: str) -> Path:
    return household_snapshot_dir_v1(output_root, household_id) / f'{household_snapshot_id}.household_snapshot.v1.json'


def snapshot_validation_decision_dir_v1(output_root: str | Path, household_id: str) -> Path:
    return advisory_authority_root_v1(output_root) / 'artifacts' / 'snapshot_validation_decision_v1' / 'households' / str(household_id)


def snapshot_validation_decision_path_v1(output_root: str | Path, household_id: str, snapshot_validation_decision_id: str) -> Path:
    return snapshot_validation_decision_dir_v1(output_root, household_id) / f'{snapshot_validation_decision_id}.snapshot_validation_decision.v1.json'


def snapshot_run_envelope_dir_v1(output_root: str | Path, household_id: str) -> Path:
    return advisory_authority_root_v1(output_root) / 'artifacts' / 'snapshot_run_envelope_v1' / 'households' / str(household_id)


def snapshot_run_envelope_path_v1(output_root: str | Path, household_id: str, snapshot_run_id: str) -> Path:
    return snapshot_run_envelope_dir_v1(output_root, household_id) / f'{snapshot_run_id}.snapshot_run_envelope.v1.json'


def portfolio_intent_dir_v1(output_root: str | Path, household_id: str) -> Path:
    return advisory_authority_root_v1(output_root) / 'authorities' / 'portfolio_intent_v1' / 'households' / str(household_id)


def portfolio_intent_path_v1(output_root: str | Path, household_id: str, portfolio_intent_id: str) -> Path:
    return portfolio_intent_dir_v1(output_root, household_id) / f'{portfolio_intent_id}.portfolio_intent.v1.json'


def promotion_decision_dir_v1(output_root: str | Path, household_id: str) -> Path:
    return advisory_authority_root_v1(output_root) / 'artifacts' / 'promotion_decision_v1' / 'households' / str(household_id)


def promotion_decision_path_v1(output_root: str | Path, household_id: str, promotion_decision_id: str) -> Path:
    return promotion_decision_dir_v1(output_root, household_id) / f'{promotion_decision_id}.promotion_decision.v1.json'


def promotion_record_dir_v2(output_root: str | Path, household_id: str) -> Path:
    return advisory_authority_root_v1(output_root) / 'authorities' / 'promotion_record_v2' / 'households' / str(household_id)


def promotion_record_path_v2(output_root: str | Path, household_id: str, promotion_record_id: str) -> Path:
    return promotion_record_dir_v2(output_root, household_id) / f'{promotion_record_id}.promotion_record.v2.json'


def execution_intent_dir_v1(output_root: str | Path, household_id: str) -> Path:
    return advisory_authority_root_v1(output_root) / 'artifacts' / 'execution_intent_v1' / 'households' / str(household_id)


def execution_intent_path_v1(output_root: str | Path, household_id: str, execution_intent_id: str) -> Path:
    return execution_intent_dir_v1(output_root, household_id) / f'{execution_intent_id}.execution_intent.v1.json'


def kernel_run_envelope_dir_v1(output_root: str | Path, household_id: str) -> Path:
    return advisory_authority_root_v1(output_root) / 'artifacts' / 'kernel_run_envelope_v1' / 'households' / str(household_id)


def kernel_run_envelope_path_v1(output_root: str | Path, household_id: str, kernel_run_id: str) -> Path:
    return kernel_run_envelope_dir_v1(output_root, household_id) / f'{kernel_run_id}.kernel_run_envelope.v1.json'


def write_immutable_json_v1(path: str | Path, obj: dict[str, object]) -> Path:
    resolved = Path(path).expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes_v1(obj) + b'\n'
    if resolved.exists() and resolved.read_bytes() != payload:
        raise ValueError(f'IMMUTABLE_CONFLICT: {resolved}')
    resolved.write_bytes(payload)
    return resolved


def _read_json_obj(path: str | Path) -> dict:
    with Path(path).expanduser().resolve().open('r', encoding='utf-8') as handle:
        obj = json.load(handle)
    if not isinstance(obj, dict):
        raise ValueError(f'TOP_LEVEL_NOT_OBJECT: {path}')
    return obj


def load_investor_intent_by_id_v1(output_root: str | Path, household_id: str, intent_id: str) -> InvestorIntentV1:
    return InvestorIntentV1.from_dict(_read_json_obj(investor_intent_path_v1(output_root, household_id, intent_id)))


def load_latest_complete_investor_intent_by_household_v1(output_root: str | Path, household_id: str) -> InvestorIntentV1 | None:
    root = investor_intent_dir_v1(output_root, household_id)
    if not root.exists():
        return None
    intents = []
    for path in sorted(root.glob('*.investor_intent.v1.json')):
        intent = InvestorIntentV1.from_dict(_read_json_obj(path))
        if intent.completeness_status == 'COMPLETE':
            intents.append(intent)
    if not intents:
        return None
    intents.sort(key=lambda item: (item.effective_at, item.created_at, item.intent_version, item.intent_id))
    return intents[-1]


def load_policy_by_id_v1(output_root: str | Path, household_id: str, policy_id: str) -> PolicyV1:
    return PolicyV1.from_dict(_read_json_obj(policy_path_v1(output_root, household_id, policy_id)))


def load_assumption_manifest_by_id_v1(output_root: str | Path, assumption_manifest_id: str) -> AssumptionManifestV1:
    return AssumptionManifestV1.from_dict(_read_json_obj(assumption_manifest_path_v1(output_root, assumption_manifest_id)))


def load_household_snapshot_by_id_v1(output_root: str | Path, household_id: str, household_snapshot_id: str) -> HouseholdSnapshotV1:
    return HouseholdSnapshotV1.from_dict(_read_json_obj(household_snapshot_path_v1(output_root, household_id, household_snapshot_id)))


def load_snapshot_validation_decision_by_id_v1(output_root: str | Path, household_id: str, snapshot_validation_decision_id: str) -> SnapshotValidationDecisionV1:
    from constellation_2.common.advisory.snapshot_validation_decision_v1 import SnapshotValidationDecisionV1

    return SnapshotValidationDecisionV1.from_dict(_read_json_obj(snapshot_validation_decision_path_v1(output_root, household_id, snapshot_validation_decision_id)))


def load_snapshot_run_envelope_by_id_v1(output_root: str | Path, household_id: str, snapshot_run_id: str) -> SnapshotRunEnvelopeV1:
    from constellation_2.common.advisory.snapshot_run_envelope_v1 import SnapshotRunEnvelopeV1

    return SnapshotRunEnvelopeV1.from_dict(_read_json_obj(snapshot_run_envelope_path_v1(output_root, household_id, snapshot_run_id)))


def load_portfolio_intent_by_id_v1(output_root: str | Path, household_id: str, portfolio_intent_id: str) -> PortfolioIntentV1:
    return PortfolioIntentV1.from_dict(_read_json_obj(portfolio_intent_path_v1(output_root, household_id, portfolio_intent_id)))


def load_promotion_decision_by_id_v1(output_root: str | Path, household_id: str, promotion_decision_id: str) -> PromotionDecisionV1:
    return PromotionDecisionV1.from_dict(_read_json_obj(promotion_decision_path_v1(output_root, household_id, promotion_decision_id)))


def load_promotion_record_by_id_v2(output_root: str | Path, household_id: str, promotion_record_id: str) -> PromotionRecordV2:
    return PromotionRecordV2.from_dict(_read_json_obj(promotion_record_path_v2(output_root, household_id, promotion_record_id)))


def load_execution_intent_by_id_v1(output_root: str | Path, household_id: str, execution_intent_id: str) -> ExecutionIntentV1:
    return ExecutionIntentV1.from_dict(_read_json_obj(execution_intent_path_v1(output_root, household_id, execution_intent_id)))


def load_kernel_run_envelope_by_id_v1(output_root: str | Path, household_id: str, kernel_run_id: str) -> KernelRunEnvelopeV1:
    return KernelRunEnvelopeV1.from_dict(_read_json_obj(kernel_run_envelope_path_v1(output_root, household_id, kernel_run_id)))


def load_policies_by_parent_intent_v1(output_root: str | Path, household_id: str, parent_intent_id: str) -> tuple[PolicyV1, ...]:
    root = policy_dir_v1(output_root, household_id)
    if not root.exists():
        return ()
    policies = []
    for path in sorted(root.glob('*.policy.v1.json')):
        policy = PolicyV1.from_dict(_read_json_obj(path))
        if policy.parent_intent_id == parent_intent_id:
            policies.append(policy)
    policies.sort(key=lambda item: (item.effective_at, item.created_at, item.policy_version, item.policy_id))
    return tuple(policies)
