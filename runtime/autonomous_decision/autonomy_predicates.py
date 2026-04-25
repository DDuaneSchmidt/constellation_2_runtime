from __future__ import annotations

from pathlib import Path
from typing import Any

from ..meta_governance.api import _store
from .types import AutonomyPredicateResult


AUTO_EXECUTION_FAMILIES = {"freeze_execution_family", "defer_change_family"}


def _result(
    candidate_action_id: str,
    predicate_id: str,
    result: bool,
    measured_value: Any,
    required_condition: Any,
    evidence_refs: tuple[str, ...],
    failure_reason: str = "",
) -> AutonomyPredicateResult:
    return AutonomyPredicateResult(
        candidate_action_id=candidate_action_id,
        predicate_id=predicate_id,
        result=result,
        measured_value=measured_value,
        required_condition=required_condition,
        evidence_refs=evidence_refs,
        failure_reason=failure_reason,
    )


def evaluate_predicates(
    candidate_action_id: str,
    *,
    authority_evaluation_ref: str,
    classification_ref: str,
    approval_refs: tuple[str, ...] = (),
    strict_portfolio_budget: bool = False,
    portfolio_risk_budget_remaining: float | None = None,
    tax_uncertainty: float | None = None,
    tax_uncertainty_limit: float | None = None,
    store_root: str | Path | None = None,
) -> tuple[AutonomyPredicateResult, ...]:
    store = _store(store_root)
    action = store.read("candidate_actions", candidate_action_id)["record"]
    authority = store.read("authority_evaluations", authority_evaluation_ref)["record"]
    classification = store.read("autonomy_classifications", classification_ref)["record"]
    evidence_refs = tuple(sorted(set(tuple(action["supporting_evidence_refs"]) + (authority_evaluation_ref, classification_ref))))
    has_severe_validation = False
    has_critical_reconciliation = False
    has_truth_gap = False
    has_execution_freeze = False
    has_broker_conflict = False
    for ref in action["supporting_evidence_refs"]:
        if store.exists("realized_validation_results", ref):
            validation = store.read("realized_validation_results", ref)["record"]
            if validation["drift_classification"] == "severe_drift":
                has_severe_validation = True
            if validation["recommended_action"] == "freeze_similar_changes":
                has_execution_freeze = True
        if store.exists("reconciliation_results", ref):
            reconciliation = store.read("reconciliation_results", ref)["record"]
            if reconciliation["overall_severity"] == "critical":
                has_critical_reconciliation = True
                has_truth_gap = True
        if store.exists("correction_recommendations", ref):
            recommendation = store.read("correction_recommendations", ref)["record"]
            if recommendation["action_type"] == "freeze_governed_execution":
                has_execution_freeze = True
            if recommendation["action_type"] == "broker_review_required":
                has_broker_conflict = True
                has_truth_gap = True
        if store.exists("adaptation_recommendations", ref):
            recommendation = store.read("adaptation_recommendations", ref)["record"]
            if recommendation["recommendation_type"] == "investigate_broker_truth":
                has_broker_conflict = True
                has_truth_gap = True
        if store.exists("regime_signals", ref):
            regime = store.read("regime_signals", ref)["record"]
            if regime["confidence_class"] == "insufficient_evidence":
                has_truth_gap = True
    scope_allows = True
    if action["action_family"] in AUTO_EXECUTION_FAMILIES:
        scope_allows = bool(authority["authority_scope"].get("allowed_scopes"))
    elif action["target_surface"] == "execution":
        scope_allows = bool(authority["authority_scope"].get("allowed_scopes"))
    elif action["target_surface"] in {"state", "broker_truth", "governance_change_family"}:
        scope_allows = False
    else:
        scope_allows = bool(authority["authority_scope"])
    tier_allows = action["action_family"] in AUTO_EXECUTION_FAMILIES
    approval_present = classification["autonomy_class"] != "approval_required" or bool(approval_refs)
    if strict_portfolio_budget and portfolio_risk_budget_remaining is None:
        portfolio_budget_ok = False
        portfolio_budget_failure = "PORTFOLIO_RISK_BUDGET_REQUIRED"
    else:
        portfolio_budget_ok = portfolio_risk_budget_remaining is None or float(portfolio_risk_budget_remaining) >= 0.0
        portfolio_budget_failure = "PORTFOLIO_RISK_BUDGET_EXCEEDED"
    if action["target_surface"] == "tax" and tax_uncertainty_limit is None:
        tax_ok = False
        tax_failure = "TAX_UNCERTAINTY_LIMIT_REQUIRED"
    else:
        tax_ok = tax_uncertainty is None or tax_uncertainty_limit is None or float(tax_uncertainty) <= float(tax_uncertainty_limit)
        tax_failure = "TAX_UNCERTAINTY_TOO_HIGH"
    results = (
        _result(candidate_action_id, "active_snapshot_valid", bool(authority["active_snapshot_id"]), authority["active_snapshot_id"], "active snapshot present", evidence_refs, "" if authority["active_snapshot_id"] else "ACTIVE_SNAPSHOT_REQUIRED"),
        _result(candidate_action_id, "interpreter_version_match", "INTERPRETER_VERSION_MISMATCH" not in authority["blocking_conditions"], authority["interpreter_version"], "current interpreter", evidence_refs, "" if "INTERPRETER_VERSION_MISMATCH" not in authority["blocking_conditions"] else "INTERPRETER_VERSION_MISMATCH"),
        _result(candidate_action_id, "verification_not_failed", not has_severe_validation, has_severe_validation, False, evidence_refs, "" if not has_severe_validation else "SEVERE_VERIFICATION_DRIFT"),
        _result(candidate_action_id, "no_critical_reconciliation_break", not has_critical_reconciliation, has_critical_reconciliation, False, evidence_refs, "" if not has_critical_reconciliation else "CRITICAL_RECONCILIATION_BREAK"),
        _result(candidate_action_id, "no_unresolved_truth_gap", not has_truth_gap, has_truth_gap, False, evidence_refs, "" if not has_truth_gap else "UNRESOLVED_TRUTH_GAP"),
        _result(candidate_action_id, "no_open_execution_freeze", not has_execution_freeze or action["action_family"] == "freeze_execution_family", has_execution_freeze, False, evidence_refs, "" if (not has_execution_freeze or action["action_family"] == "freeze_execution_family") else "OPEN_EXECUTION_FREEZE"),
        _result(candidate_action_id, "authority_scope_allows_action", scope_allows, authority["authority_scope"], action["target_surface"], evidence_refs, "" if scope_allows else "AUTHORITY_SCOPE_MISMATCH"),
        _result(candidate_action_id, "autonomy_tier_allows_action", tier_allows or classification["autonomy_class"] != "auto_executable", action["action_family"], AUTO_EXECUTION_FAMILIES, evidence_refs, "" if (tier_allows or classification["autonomy_class"] != "auto_executable") else "AUTONOMY_TIER_DISALLOWS_ACTION"),
        _result(candidate_action_id, "required_human_approval_present", approval_present, tuple(approval_refs), classification["autonomy_class"], evidence_refs, "" if approval_present else "HUMAN_APPROVAL_REQUIRED"),
        _result(candidate_action_id, "portfolio_risk_budget_allows_action", portfolio_budget_ok, portfolio_risk_budget_remaining, "non-negative", evidence_refs, "" if portfolio_budget_ok else portfolio_budget_failure),
        _result(candidate_action_id, "tax_uncertainty_below_limit", tax_ok, tax_uncertainty, tax_uncertainty_limit if tax_uncertainty_limit is not None else "not_required", evidence_refs, "" if tax_ok else tax_failure),
        _result(candidate_action_id, "broker_truth_conflict_absent", not has_broker_conflict, has_broker_conflict, False, evidence_refs, "" if not has_broker_conflict else "BROKER_TRUTH_CONFLICT"),
    )
    return results
