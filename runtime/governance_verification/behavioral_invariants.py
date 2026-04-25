from __future__ import annotations

from typing import Any

from ..meta_governance.store import ArtifactStore
from .decision_diff import replay_decisions
from .types import BehavioralInvariantResult


def _budget(scope: dict[str, Any], key: str, default: Any) -> Any:
    return scope.get("behavioral_budgets", {}).get(key, default)


def _autonomy_surface(policy: dict[str, Any], decisions: list[dict[str, Any]]) -> set[str]:
    surface = set(policy.get("autonomy", {}).get("allowed_actions", ()))
    if not surface:
        surface.update(decision.get("action") for decision in decisions if decision.get("action"))
    return {item for item in surface if item}


def _execution_surface(policy: dict[str, Any]) -> set[str]:
    scopes = policy.get("execution", {}).get("allowed_scopes", ("position",))
    if isinstance(scopes, list):
        return set(scopes)
    if isinstance(scopes, tuple):
        return set(scopes)
    if scopes:
        return {str(scopes)}
    return {"position"}


def evaluate_behavioral_invariants(store: ArtifactStore, verification_id: str) -> tuple[str, ...]:
    context = store.read("verification_contexts", verification_id)["record"]
    impact = store.read("impact_summaries", verification_id)["record"]
    diff = store.read("decision_diff_artifacts", verification_id)["record"]
    replay = replay_decisions(store, verification_id)
    baseline_snapshot = store.read("activation_snapshots", context["active_snapshot_id"])
    candidate_snapshot = store.read("activation_snapshots", context["candidate_snapshot_id"])
    baseline_policy = store.read("canonical_graphs", baseline_snapshot["record"]["graph_id"])["record"]["compiled_policy"]
    candidate_policy = store.read("canonical_graphs", candidate_snapshot["record"]["graph_id"])["record"]["compiled_policy"]
    scope = context.get("scope", {})
    decision_deltas = diff["decision_deltas"]
    churn = len(decision_deltas)
    baseline_autonomy = _autonomy_surface(baseline_policy, replay["baseline"]["decisions"])
    candidate_autonomy = _autonomy_surface(candidate_policy, replay["candidate"]["decisions"])
    baseline_execution = _execution_surface(baseline_policy)
    candidate_execution = _execution_surface(candidate_policy)
    checks = [
        (
            "max_allowed_risk_increase",
            float(impact["expected_risk_delta"]),
            float(_budget(scope, "max_allowed_risk_increase", 0.0)),
            float(impact["expected_risk_delta"]) <= float(_budget(scope, "max_allowed_risk_increase", 0.0)),
            "risk increase exceeds budget",
        ),
        (
            "max_allowed_turnover_increase",
            float(impact["expected_turnover_delta"]),
            float(_budget(scope, "max_allowed_turnover_increase", 0.0)),
            float(impact["expected_turnover_delta"]) <= float(_budget(scope, "max_allowed_turnover_increase", 0.0)),
            "turnover increase exceeds budget",
        ),
        (
            "max_allowed_tax_degradation",
            abs(float(impact["expected_tax_delta"])),
            float(_budget(scope, "max_allowed_tax_degradation", 0.0)),
            abs(float(impact["expected_tax_delta"])) <= float(_budget(scope, "max_allowed_tax_degradation", 0.0)),
            "tax degradation exceeds budget",
        ),
        (
            "max_allowed_decision_churn",
            float(churn),
            float(_budget(scope, "max_allowed_decision_churn", 0.0)),
            float(churn) <= float(_budget(scope, "max_allowed_decision_churn", 0.0)),
            "decision churn exceeds budget",
        ),
        (
            "no_forbidden_autonomy_broadening",
            sorted(candidate_autonomy - baseline_autonomy),
            bool(_budget(scope, "allow_new_autonomy_classes", False)),
            not (candidate_autonomy - baseline_autonomy) or bool(_budget(scope, "allow_new_autonomy_classes", False)),
            "new autonomous action classes appear without allowance",
        ),
        (
            "no_forbidden_execution_scope_increase",
            sorted(candidate_execution - baseline_execution),
            bool(_budget(scope, "allow_execution_scope_increase", False)),
            not (candidate_execution - baseline_execution) or bool(_budget(scope, "allow_execution_scope_increase", False)),
            "execution scope increases without allowance",
        ),
    ]
    artifact_ids: list[str] = []
    for invariant_id, measured, limit, passed, failure_reason in checks:
        artifact_id = f"{verification_id}__{invariant_id}"
        record = BehavioralInvariantResult(
            verification_id=verification_id,
            invariant_id=invariant_id,
            result=bool(passed),
            measured_value=measured,
            allowed_limit=limit,
            evidence_refs=(verification_id,),
            failure_reason="" if passed else failure_reason,
        )
        store.write_immutable("behavioral_invariant_results", artifact_id, record, artifact_type="BehavioralInvariantResult")
        artifact_ids.append(artifact_id)
    return tuple(artifact_ids)


def all_behavioral_invariants_pass(store: ArtifactStore, invariant_refs: tuple[str, ...]) -> bool:
    return all(store.read("behavioral_invariant_results", ref)["record"]["result"] for ref in invariant_refs)
