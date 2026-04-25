from __future__ import annotations

from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_sha256_hex_v1
from constellation_2.common.advisory.advisory_domain_constants_v1 import (
    KERNEL_RUN_ENVELOPE_CONTRACT_VERSION_V1,
    KERNEL_RUN_ENVELOPE_VERSION_V1,
)
from constellation_2.common.advisory.advisory_storage_v1 import (
    kernel_run_envelope_path_v1,
    write_immutable_json_v1,
)
from constellation_2.common.advisory.execution_intent_service_v1 import write_execution_intent_v1
from constellation_2.common.advisory.execution_package_builder_from_execution_intent_v1 import (
    build_execution_package_from_execution_intent_v1,
    handoff_to_existing_paper_trading_v1,
)
from constellation_2.common.advisory.household_snapshot_v1 import HouseholdSnapshotV1
from constellation_2.common.advisory.investor_intent_v1 import InvestorIntentV1
from constellation_2.common.advisory.kernel_run_envelope_v1 import KernelRunEnvelopeV1
from constellation_2.common.advisory.policy_v1 import PolicyV1
from constellation_2.common.advisory.portfolio_intent_service_v1 import write_portfolio_intent_v1
from constellation_2.common.advisory.promotion_decision_service_v1 import write_promotion_decision_v1
from constellation_2.common.advisor_bridge.promotion_record_service_v2 import write_promotion_record_v2


def emit_kernel_run_envelope_v1(
    *,
    kernel_run_id: str,
    household_id: str,
    produced_utc: str,
    day_utc: str,
    run_outcome: str,
    stage_status: dict[str, Any],
    artifact_refs: dict[str, Any],
    handoff_status: dict[str, Any],
    reason_codes: list[str] | tuple[str, ...],
    output_root: str = '',
) -> tuple[KernelRunEnvelopeV1, str]:
    obj = {
        'schema_id': 'kernel_run_envelope',
        'schema_version': 'v1',
        'record_id': canonical_sha256_hex_v1(
            {
                'kernel_run_id': kernel_run_id,
                'household_id': household_id,
                'run_outcome': run_outcome,
                'artifact_refs': artifact_refs,
            }
        ),
        'kernel_run_id': kernel_run_id,
        'household_id': household_id,
        'produced_utc': produced_utc,
        'day_utc': day_utc,
        'contract_version': KERNEL_RUN_ENVELOPE_CONTRACT_VERSION_V1,
        'envelope_version': KERNEL_RUN_ENVELOPE_VERSION_V1,
        'run_outcome': run_outcome,
        'stage_status': stage_status,
        'artifact_refs': artifact_refs,
        'handoff_status': handoff_status,
        'reason_codes': sorted(set(str(item) for item in reason_codes)),
    }
    envelope = KernelRunEnvelopeV1.from_dict(obj)
    path = kernel_run_envelope_path_v1(output_root, household_id, kernel_run_id)
    written = write_immutable_json_v1(path, envelope.to_dict())
    return envelope, str(written)


def run_advisory_kernel_v1(
    *,
    repo_root: Path,
    output_root: str,
    kernel_run_id: str,
    investor_intent: InvestorIntentV1,
    policy: PolicyV1,
    household_snapshot: HouseholdSnapshotV1,
    portfolio_created_at: str,
    decision_created_at: str,
    promotion_produced_utc: str,
    execution_created_at_utc: str,
    actor_source: str,
    execution_profile: dict[str, Any],
    approval_confirmed: bool,
    risk_budget_path: Path | None = None,
    dry_run_submit: bool = True,
    submit_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    artifact_refs: dict[str, Any] = {
        'investor_intent_id': investor_intent.intent_id,
        'policy_id': policy.policy_id,
        'household_snapshot_id': household_snapshot.household_snapshot_id,
        'portfolio_intent_id': None,
        'promotion_decision_id': None,
        'promotion_record_id': None,
        'execution_intent_id': None,
        'execution_package_path': None,
        'execution_package_sha256': None,
    }
    stage_status = {
        'investor_intent': 'PRESENT',
        'policy': 'PRESENT',
        'household_snapshot': 'PRESENT',
        'portfolio_intent': 'SKIPPED',
        'promotion_decision': 'SKIPPED',
        'promotion_record': 'SKIPPED',
        'execution_intent': 'SKIPPED',
    }
    handoff_status = {
        'package_build': 'SKIPPED',
        'paper_submit': 'SKIPPED',
    }
    reason_codes: list[str] = []

    portfolio_intent, _ = write_portfolio_intent_v1(
        policy=policy,
        household_snapshot=household_snapshot,
        created_at=portfolio_created_at,
        effective_at=portfolio_created_at,
        actor_source=actor_source,
        classification_refs=list(household_snapshot.classification_refs),
        output_root=output_root,
    )
    artifact_refs['portfolio_intent_id'] = portfolio_intent.portfolio_intent_id
    stage_status['portfolio_intent'] = portfolio_intent.execution_eligibility

    promotion_decision, _ = write_promotion_decision_v1(
        policy=policy,
        household_snapshot=household_snapshot,
        portfolio_intent=portfolio_intent,
        created_at=decision_created_at,
        effective_at=decision_created_at,
        actor_source=actor_source,
        output_root=output_root,
    )
    artifact_refs['promotion_decision_id'] = promotion_decision.promotion_decision_id
    stage_status['promotion_decision'] = promotion_decision.outcome.upper()
    reason_codes.extend(promotion_decision.reason_codes)

    promotion_record, _ = write_promotion_record_v2(
        policy=policy,
        portfolio_intent=portfolio_intent,
        promotion_decision=promotion_decision,
        produced_utc=promotion_produced_utc,
        run_id=kernel_run_id,
        execution_profile=execution_profile,
        approval_confirmed=approval_confirmed,
        output_root=output_root,
    )
    artifact_refs['promotion_record_id'] = promotion_record.promotion_record_id
    stage_status['promotion_record'] = promotion_record.status
    reason_codes.extend(promotion_record.reason_codes)

    execution_intent = None
    package_result = None
    if promotion_record.status == 'AUTHORIZED':
        execution_intent, _ = write_execution_intent_v1(
            promotion_record=promotion_record,
            created_at_utc=execution_created_at_utc,
            effective_at_utc=execution_created_at_utc,
            actor_source=actor_source,
            output_root=output_root,
        )
        artifact_refs['execution_intent_id'] = execution_intent.execution_intent_id
        stage_status['execution_intent'] = 'AUTHORIZED'
        package_result = build_execution_package_from_execution_intent_v1(
            repo_root=repo_root,
            execution_intent=execution_intent,
        )
        handoff_status['package_build'] = package_result['build_obj']['closure_status']
        if package_result['package_path'] is not None:
            artifact_refs['execution_package_path'] = str(Path(package_result['package_path']).resolve())
            artifact_refs['execution_package_sha256'] = str(package_result['package_obj']['canonical_json_hash'])
        if risk_budget_path is not None and submit_config is not None:
            rc = handoff_to_existing_paper_trading_v1(
                repo_root=repo_root,
                execution_intent=execution_intent,
                execution_package_path=Path(package_result['package_path']),
                submission_record_path=Path(package_result['package_path']).resolve().parent / 'submission_record.v1.json',
                eval_time_utc=submit_config['eval_time_utc'],
                risk_budget_path=risk_budget_path,
                ib_host=submit_config['ib_host'],
                ib_port=int(submit_config['ib_port']),
                ib_client_id=int(submit_config['ib_client_id']),
                dry_run=bool(dry_run_submit),
                submissions_root_override=submit_config.get('submissions_root_override'),
            )
            handoff_status['paper_submit'] = 'DRY_RUN_ACCEPTED' if dry_run_submit and rc == 0 else f'RC_{rc}'
        else:
            handoff_status['paper_submit'] = 'NOT_ATTEMPTED'
        run_outcome = 'promote'
    elif promotion_record.status == 'NO_ACTION':
        run_outcome = 'no_action'
    else:
        run_outcome = 'blocked'

    envelope, envelope_path = emit_kernel_run_envelope_v1(
        kernel_run_id=kernel_run_id,
        household_id=policy.household_id,
        produced_utc=promotion_produced_utc,
        day_utc=promotion_produced_utc[:10],
        run_outcome=run_outcome,
        stage_status=stage_status,
        artifact_refs=artifact_refs,
        handoff_status=handoff_status,
        reason_codes=reason_codes or [f'KERNEL_RUN_{run_outcome.upper()}'],
        output_root=output_root,
    )
    return {
        'portfolio_intent': portfolio_intent,
        'promotion_decision': promotion_decision,
        'promotion_record': promotion_record,
        'execution_intent': execution_intent,
        'execution_package_result': package_result,
        'kernel_run_envelope': envelope,
        'kernel_run_envelope_path': envelope_path,
    }
