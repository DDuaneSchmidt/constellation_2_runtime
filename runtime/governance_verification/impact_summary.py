from __future__ import annotations

from typing import Any

from ..meta_governance.store import ArtifactStore
from .decision_diff import replay_decisions
from .schemas import content_hash
from .types import ImpactSummary


def _weighted_domain_total(decisions: list[dict[str, Any]], policy: dict[str, Any], domain: str) -> float:
    weight = policy.get("weights", {}).get(domain, 0.0)
    return float(sum(decision.get("scores", {}).get(domain, 0.0) * weight for decision in decisions))


def _capital_usage(decisions: list[dict[str, Any]], state: dict[str, Any]) -> float:
    total = 0.0
    for decision in decisions:
        position_id = decision.get("position_id")
        if not position_id:
            continue
        total += float(abs(state.get("positions", {}).get(position_id, {}).get("quantity", 0)))
    return total


def _policy_surface_count(policy: dict[str, Any], path: tuple[str, ...]) -> int:
    current: Any = policy
    for part in path:
        if not isinstance(current, dict):
            return 0
        current = current.get(part)
    if isinstance(current, list):
        return len(current)
    if isinstance(current, dict):
        return len(current)
    if current is None:
        return 0
    return 1


def build_impact_summary(store: ArtifactStore, verification_id: str) -> str:
    replay = replay_decisions(store, verification_id)
    baseline_doc = store.read("decision_diff_artifacts", verification_id)
    baseline_run = replay["baseline"]
    candidate_run = replay["candidate"]
    context = store.read("verification_contexts", verification_id)["record"]
    baseline_snapshot = store.read("activation_snapshots", context["active_snapshot_id"])
    candidate_snapshot = store.read("activation_snapshots", context["candidate_snapshot_id"])
    baseline_graph = store.read("canonical_graphs", baseline_snapshot["record"]["graph_id"])
    candidate_graph = store.read("canonical_graphs", candidate_snapshot["record"]["graph_id"])
    baseline_policy = baseline_graph["record"]["compiled_policy"]
    candidate_policy = candidate_graph["record"]["compiled_policy"]
    expected_risk_delta = _weighted_domain_total(candidate_run["decisions"], candidate_policy, "risk") - _weighted_domain_total(baseline_run["decisions"], baseline_policy, "risk")
    expected_tax_delta = _weighted_domain_total(candidate_run["decisions"], candidate_policy, "tax") - _weighted_domain_total(baseline_run["decisions"], baseline_policy, "tax")
    expected_turnover_delta = float(len(candidate_run["decisions"]) - len(baseline_run["decisions"]))
    expected_capital_usage_delta = _capital_usage(candidate_run["decisions"], candidate_run["state"]) - _capital_usage(baseline_run["decisions"], baseline_run["state"])
    expected_autonomy_delta = float(
        _policy_surface_count(candidate_policy, ("autonomy", "allowed_actions"))
        - _policy_surface_count(baseline_policy, ("autonomy", "allowed_actions"))
    )
    expected_execution_delta = float(
        _policy_surface_count(candidate_policy, ("execution", "allowed_scopes"))
        - _policy_surface_count(baseline_policy, ("execution", "allowed_scopes"))
    )
    changed_domains = sorted(
        {
            domain
            for delta in baseline_doc["record"]["decision_deltas"]
            for domain in delta["affected_domains"]
        }
    )
    if expected_autonomy_delta > 0 or expected_execution_delta > 0:
        summary_classification = "critical"
    elif baseline_doc["record"]["decision_count_changed"] > 0:
        summary_classification = "moderate"
    else:
        summary_classification = "safe"
    artifact_hash = content_hash(
        {
            "expected_risk_delta": expected_risk_delta,
            "expected_tax_delta": expected_tax_delta,
            "expected_turnover_delta": expected_turnover_delta,
            "expected_capital_usage_delta": expected_capital_usage_delta,
            "expected_autonomy_delta": expected_autonomy_delta,
            "expected_execution_delta": expected_execution_delta,
            "decision_surface_delta_count": baseline_doc["record"]["decision_count_changed"],
            "summary_classification": summary_classification,
            "major_changed_domains": changed_domains,
        }
    )
    summary = ImpactSummary(
        verification_id=verification_id,
        expected_risk_delta=float(expected_risk_delta),
        expected_tax_delta=float(expected_tax_delta),
        expected_turnover_delta=float(expected_turnover_delta),
        expected_capital_usage_delta=float(expected_capital_usage_delta),
        expected_autonomy_delta=float(expected_autonomy_delta),
        expected_execution_delta=float(expected_execution_delta),
        decision_surface_delta_count=int(baseline_doc["record"]["decision_count_changed"]),
        summary_classification=summary_classification,
        major_changed_domains=tuple(changed_domains),
        artifact_hash=artifact_hash,
    )
    store.write_immutable("impact_summaries", verification_id, summary, artifact_type="ImpactSummary")
    return verification_id
