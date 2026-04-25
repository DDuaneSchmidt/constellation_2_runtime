from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class ExecutionIntent:
    execution_intent_id: str
    candidate_action_ref: str
    action_plan_ref: str
    active_snapshot_id: str
    graph_hash: str
    interpreter_version: str
    target_account: str | None
    target_symbol: str | None
    target_side: str | None
    target_quantity: float | None
    target_order_type: str | None
    target_limit_price: float | None
    target_time_in_force: str | None
    rationale: str
    supporting_evidence_refs: tuple[str, ...]
    artifact_hash: str


@dataclass(frozen=True, slots=True)
class PreExecutionGateResult:
    execution_intent_id: str
    gate_id: str
    result: bool
    measured_value: Any
    required_condition: Any
    evidence_refs: tuple[str, ...]
    failure_reason: str = ""


@dataclass(frozen=True, slots=True)
class ExecutionSubmission:
    submission_id: str
    execution_intent_ref: str
    adapter_name: str
    submitted_at: str
    submission_payload_hash: str
    broker_request_ref: str
    status: str
    artifact_hash: str


@dataclass(frozen=True, slots=True)
class ExecutionReceipt:
    receipt_id: str
    submission_ref: str
    receipt_type: str
    received_at: str
    external_execution_ref: str | None
    order_status: str
    filled_quantity: float
    remaining_quantity: float
    average_fill_price: float | None
    fee_amount: float | None
    message: str
    artifact_hash: str


@dataclass(frozen=True, slots=True)
class ExecutionStateTransition:
    transition_id: str
    submission_ref: str
    prior_state: str
    new_state: str
    triggered_by: str
    receipt_ref: str | None
    occurred_at: str
    explanation: str
    artifact_hash: str


@dataclass(frozen=True, slots=True)
class ExecutionRecoveryRecord:
    recovery_id: str
    as_of: str
    open_submission_refs: tuple[str, ...]
    recovered_state_refs: tuple[str, ...]
    unresolved_refs: tuple[str, ...]
    recommended_actions: tuple[str, ...]
    artifact_hash: str


@dataclass(frozen=True, slots=True)
class DeploymentState:
    deployment_state_id: str
    as_of: str
    runtime_mode: str
    service_state: str
    startup_gate_status: str
    active_snapshot_id: str | None
    interpreter_version: str
    unhealthy_components: tuple[str, ...]
    blocked_execution_reasons: tuple[str, ...]
    artifact_hash: str


@dataclass(frozen=True, slots=True)
class HealthCheckResult:
    health_check_id: str
    component_name: str
    status: str
    checked_at: str
    details: dict[str, Any] = field(default_factory=dict)
    blocking: bool = False
    artifact_hash: str = ""


@dataclass(frozen=True, slots=True)
class ExecutionOperatorView:
    operator_view_id: str
    as_of: str
    pending_submissions: tuple[str, ...]
    blocked_intents: tuple[str, ...]
    active_receipts: tuple[str, ...]
    open_recovery_items: tuple[str, ...]
    deployment_state_ref: str
    top_execution_risks: tuple[str, ...]
    view_hash: str


@dataclass(frozen=True, slots=True)
class ExecutionDeploymentBundle:
    bundle_id: str
    intent_refs: tuple[str, ...]
    gate_result_refs: tuple[str, ...]
    submission_refs: tuple[str, ...]
    receipt_refs: tuple[str, ...]
    state_transition_refs: tuple[str, ...]
    recovery_refs: tuple[str, ...]
    deployment_state_ref: str
    health_check_refs: tuple[str, ...]
    operator_view_ref: str
    artifact_hash: str
