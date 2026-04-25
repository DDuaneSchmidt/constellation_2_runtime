from __future__ import annotations

from pathlib import Path
from typing import Any

from .broker_adapters import adapter_readiness
from .execution_state_machine import OPEN_STATES, current_state
from .types import PreExecutionGateResult
from ..meta_governance.api import _store
from ..meta_governance.startup_gate import validate_runtime_authority


def _result(
    execution_intent_id: str,
    gate_id: str,
    result: bool,
    measured_value: Any,
    required_condition: Any,
    evidence_refs: tuple[str, ...],
    failure_reason: str = "",
) -> PreExecutionGateResult:
    return PreExecutionGateResult(
        execution_intent_id=execution_intent_id,
        gate_id=gate_id,
        result=result,
        measured_value=measured_value,
        required_condition=required_condition,
        evidence_refs=evidence_refs,
        failure_reason=failure_reason,
    )


def _latest_internal_snapshot_for(store, runtime_snapshot_id: str) -> dict | None:
    matches: list[dict] = []
    for ref in store.list_ids("internal_reality_snapshots"):
        snapshot = store.read("internal_reality_snapshots", ref)
        if snapshot["record"]["runtime_snapshot_id"] == runtime_snapshot_id:
            matches.append(snapshot)
    if not matches:
        return None
    matches.sort(key=lambda item: (item["record"]["captured_at"], item["record"]["internal_snapshot_id"]))
    return matches[-1]


def _open_duplicate_exists(store, intent: dict) -> bool:
    for ref in store.list_ids("execution_submissions"):
        submission = store.read("execution_submissions", ref)["record"]
        state = current_state(store, ref)
        if state not in OPEN_STATES:
            continue
        other_intent = store.read("execution_intents", submission["execution_intent_ref"])["record"]
        fields = ("target_account", "target_symbol", "target_side", "target_quantity")
        if all(other_intent.get(field) == intent.get(field) for field in fields):
            return True
    return False


def evaluate_pre_execution_gate(
    execution_intent_id: str,
    *,
    runtime_mode: str,
    adapter_name: str,
    deployment_state_ref: str | None = None,
    strict_risk_budget: bool = False,
    risk_budget_remaining: float | None = None,
    strict_tax_check: bool = False,
    tax_uncertainty: float | None = None,
    tax_uncertainty_limit: float | None = None,
    store_root: str | Path | None = None,
) -> tuple[PreExecutionGateResult, ...]:
    store = _store(store_root)
    intent = store.read("execution_intents", execution_intent_id)["record"]
    action = store.read("candidate_actions", intent["candidate_action_ref"])["record"]
    eligibility = store.read("execution_eligibilities", intent["candidate_action_ref"])["record"]
    evidence_refs = tuple(sorted(set(tuple(intent["supporting_evidence_refs"]) + (intent["candidate_action_ref"], intent["action_plan_ref"]))))
    active_snapshot_id = ""
    authority_valid = True
    scope_allowed = True
    snapshot_failure = ""
    graph_match = True
    interpreter_match = True
    try:
        authority = validate_runtime_authority(store, actor="pre_execution_gate", event_type="pre_execution_gate_blocked")
        active_snapshot_id = authority["snapshot"]["snapshot_id"]
        graph_match = authority["snapshot"]["graph_hash"] == intent["graph_hash"]
        interpreter_match = authority["snapshot"]["interpreter_version"] == intent["interpreter_version"]
        scope_allowed = bool(authority["compiled_policy"].get("execution", {}).get("allowed_scopes"))
        authority_valid = graph_match and interpreter_match and scope_allowed
    except ValueError as exc:
        authority_valid = False
        scope_allowed = False
        snapshot_failure = str(exc)
    has_critical_reconciliation = False
    has_execution_freeze = False
    has_broker_conflict = False
    for ref in intent["supporting_evidence_refs"]:
        if store.exists("reconciliation_results", ref):
            if store.read("reconciliation_results", ref)["record"]["overall_severity"] == "critical":
                has_critical_reconciliation = True
        if store.exists("correction_recommendations", ref):
            recommendation = store.read("correction_recommendations", ref)["record"]
            if recommendation["action_type"] == "freeze_governed_execution":
                has_execution_freeze = True
            if recommendation["action_type"] == "broker_review_required":
                has_broker_conflict = True
        if store.exists("adaptation_recommendations", ref):
            recommendation = store.read("adaptation_recommendations", ref)["record"]
            if recommendation["recommendation_type"] == "investigate_broker_truth":
                has_broker_conflict = True
    internal_snapshot = _latest_internal_snapshot_for(store, intent["active_snapshot_id"])
    if internal_snapshot is None:
        account_mapping_resolved = False
        account_mapping_measurement: Any = "missing_internal_reality_snapshot"
        account_mapping_failure = "ACCOUNT_MAPPING_UNPROVEN"
    else:
        accounts = {record.get("account_id") for record in internal_snapshot["record"]["account_state"]}
        account_mapping_resolved = bool(intent["target_account"]) and intent["target_account"] in accounts
        account_mapping_measurement = tuple(sorted(account for account in accounts if account))
        account_mapping_failure = "ACCOUNT_MAPPING_UNRESOLVED"
    duplicate_absent = not _open_duplicate_exists(store, intent)
    adapter_status = adapter_readiness(adapter_name=adapter_name, runtime_mode=runtime_mode)
    if strict_risk_budget and risk_budget_remaining is None:
        risk_ok = False
        risk_failure = "RISK_BUDGET_REQUIRED"
    else:
        risk_ok = risk_budget_remaining is None or float(risk_budget_remaining) >= 0.0
        risk_failure = "RISK_BUDGET_EXCEEDED"
    if strict_tax_check and tax_uncertainty_limit is None:
        tax_ok = False
        tax_failure = "TAX_UNCERTAINTY_LIMIT_REQUIRED"
    else:
        tax_ok = tax_uncertainty is None or tax_uncertainty_limit is None or float(tax_uncertainty) <= float(tax_uncertainty_limit)
        tax_failure = "TAX_UNCERTAINTY_TOO_HIGH"
    deployment_ok = True
    deployment_measured: Any = "not_required"
    deployment_failure = ""
    if deployment_state_ref:
        deployment = store.read("deployment_states", deployment_state_ref)["record"]
        deployment_measured = {"runtime_mode": deployment["runtime_mode"], "service_state": deployment["service_state"]}
        deployment_ok = deployment["service_state"] == "healthy" and deployment["runtime_mode"] in {"development", "paper"}
        deployment_failure = "DEPLOYMENT_STATE_BLOCKS_SUBMISSION" if not deployment_ok else ""
    results = (
        _result(execution_intent_id, "active_snapshot_valid", bool(active_snapshot_id) and not snapshot_failure, active_snapshot_id or snapshot_failure, "active snapshot present", evidence_refs, snapshot_failure if snapshot_failure else ""),
        _result(execution_intent_id, "action_still_eligible", bool(eligibility["eligible_for_execution"]), eligibility["blocking_reasons"], True, evidence_refs, "" if eligibility["eligible_for_execution"] else "ACTION_NOT_ELIGIBLE"),
        _result(execution_intent_id, "authority_still_valid", authority_valid, {"graph_match": graph_match, "interpreter_match": interpreter_match, "scope_allowed": scope_allowed}, True, evidence_refs, "" if authority_valid else "AUTHORITY_INVALID"),
        _result(execution_intent_id, "no_open_critical_reconciliation_break", not has_critical_reconciliation, has_critical_reconciliation, False, evidence_refs, "" if not has_critical_reconciliation else "CRITICAL_RECONCILIATION_BREAK"),
        _result(execution_intent_id, "no_open_execution_freeze_candidate", not has_execution_freeze, has_execution_freeze, False, evidence_refs, "" if not has_execution_freeze else "OPEN_EXECUTION_FREEZE_CANDIDATE"),
        _result(execution_intent_id, "no_unresolved_broker_truth_conflict", not has_broker_conflict, has_broker_conflict, False, evidence_refs, "" if not has_broker_conflict else "BROKER_TRUTH_CONFLICT"),
        _result(execution_intent_id, "quantity_and_symbol_complete", bool(intent["target_symbol"]) and bool(intent["target_side"]) and intent["target_quantity"] is not None and float(intent["target_quantity"]) > 0.0, {"target_symbol": intent["target_symbol"], "target_side": intent["target_side"], "target_quantity": intent["target_quantity"]}, "complete order coordinates", evidence_refs, "" if (bool(intent["target_symbol"]) and bool(intent["target_side"]) and intent["target_quantity"] is not None and float(intent["target_quantity"]) > 0.0) else "ORDER_COORDINATES_INCOMPLETE"),
        _result(execution_intent_id, "account_mapping_resolved", account_mapping_resolved, account_mapping_measurement, intent["target_account"], evidence_refs, "" if account_mapping_resolved else account_mapping_failure),
        _result(execution_intent_id, "execution_scope_allowed", scope_allowed and adapter_status["status"] == "healthy", adapter_status, True, evidence_refs, "" if (scope_allowed and adapter_status["status"] == "healthy") else adapter_status["reason"] if adapter_status["status"] != "healthy" else "EXECUTION_SCOPE_DISALLOWED"),
        _result(execution_intent_id, "tax_uncertainty_below_limit", tax_ok, tax_uncertainty, tax_uncertainty_limit if tax_uncertainty_limit is not None else "not_required", evidence_refs, "" if tax_ok else tax_failure),
        _result(execution_intent_id, "risk_budget_check_passed", risk_ok, risk_budget_remaining, "non-negative", evidence_refs, "" if risk_ok else risk_failure),
        _result(execution_intent_id, "duplicate_submission_absent", duplicate_absent, duplicate_absent, True, evidence_refs, "" if duplicate_absent else "DUPLICATE_SUBMISSION_PRESENT"),
        _result(execution_intent_id, "deployment_state_allows_submission", deployment_ok, deployment_measured, {"runtime_mode": runtime_mode, "service_state": "healthy"}, evidence_refs, deployment_failure),
    )
    return results
