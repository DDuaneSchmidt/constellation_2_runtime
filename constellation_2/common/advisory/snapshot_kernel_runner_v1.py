from __future__ import annotations

from pathlib import Path
from typing import Any

from constellation_2.common.advisory.household_snapshot_service_v1 import write_household_snapshot_v1
from constellation_2.common.advisory.policy_v1 import PolicyV1
from constellation_2.common.advisory.snapshot_run_envelope_v1 import write_snapshot_run_envelope_v1
from constellation_2.common.advisory.snapshot_validation_decision_v1 import write_snapshot_validation_decision_v1


def run_snapshot_kernel_v1(
    *,
    repo_root: Path,
    output_root: str,
    snapshot_run_id: str,
    policy: PolicyV1,
    produced_utc: str,
    effective_at: str,
    actor_source: str,
    account_registry_snapshot: dict[str, Any],
    verified_positions_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    verified_cash_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    external_holdings_inputs: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
    classification_refs: list[str] | tuple[str, ...] = (),
) -> dict[str, Any]:
    del repo_root
    validation_decision, validation_path = write_snapshot_validation_decision_v1(
        policy=policy,
        produced_utc=produced_utc,
        effective_at=effective_at,
        actor_source=actor_source,
        account_registry_snapshot=account_registry_snapshot,
        verified_positions_inputs=verified_positions_inputs,
        verified_cash_inputs=verified_cash_inputs,
        external_holdings_inputs=external_holdings_inputs,
        classification_refs=classification_refs,
        output_root=output_root,
    )

    household_snapshot = None
    household_snapshot_path = None
    run_outcome = 'blocked'
    reason_codes = list(validation_decision.reason_codes)
    if validation_decision.outcome == 'valid':
        household_snapshot, household_snapshot_path = write_household_snapshot_v1(
            policy=policy,
            created_at=produced_utc,
            effective_at=effective_at,
            actor_source=actor_source,
            account_registry_snapshot=account_registry_snapshot,
            verified_positions_inputs=verified_positions_inputs,
            verified_cash_inputs=verified_cash_inputs,
            external_holdings_inputs=external_holdings_inputs,
            classification_refs=classification_refs,
            output_root=output_root,
        )
        run_outcome = 'success'

    envelope, envelope_path = write_snapshot_run_envelope_v1(
        snapshot_run_id=snapshot_run_id,
        household_id=policy.household_id,
        produced_utc=produced_utc,
        effective_at=effective_at,
        run_outcome=run_outcome,
        artifact_refs={
            'snapshot_validation_decision_id': validation_decision.snapshot_validation_decision_id,
            'snapshot_validation_decision_path': validation_path,
            'input_assembly_id': validation_decision.input_assembly_id,
            'household_snapshot_id': None if household_snapshot is None else household_snapshot.household_snapshot_id,
            'household_snapshot_path': household_snapshot_path,
        },
        reason_codes=reason_codes or [f'SNAPSHOT_RUN_{run_outcome.upper()}'],
        output_root=output_root,
    )
    return {
        'snapshot_validation_decision': validation_decision,
        'snapshot_validation_decision_path': validation_path,
        'household_snapshot': household_snapshot,
        'household_snapshot_path': household_snapshot_path,
        'snapshot_run_envelope': envelope,
        'snapshot_run_envelope_path': envelope_path,
    }
