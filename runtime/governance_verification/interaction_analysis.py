from __future__ import annotations

from itertools import combinations
from typing import Any

from ..meta_governance.store import ArtifactStore
from .behavioral_invariants import all_behavioral_invariants_pass, evaluate_behavioral_invariants
from .decision_diff import compute_decision_diff
from .impact_summary import build_impact_summary
from .schemas import content_hash
from .types import InteractionAnalysisResult
from .verification_context import create_verification_context


def _flatten(value: Any, prefix: str = "") -> dict[str, Any]:
    if not isinstance(value, dict):
        return {prefix: value}
    flattened: dict[str, Any] = {}
    for key, child in sorted(value.items()):
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(child, dict):
            flattened.update(_flatten(child, path))
        else:
            flattened[path] = child
    return flattened


def _component_result(
    store: ArtifactStore,
    *,
    base_context: dict[str, Any],
    component_name: str,
    component_snapshot_id: str,
) -> dict[str, Any]:
    verification_id = f"{base_context['verification_id']}__component__{component_name}"
    create_verification_context(
        store,
        active_snapshot_id=base_context["active_snapshot_id"],
        candidate_snapshot_id=component_snapshot_id,
        dataset_refs=tuple(base_context["dataset_refs"]),
        requested_by=base_context["requested_by"],
        scope=dict(base_context.get("scope", {})),
        verification_id=verification_id,
        status="completed",
    )
    compute_decision_diff(store, verification_id)
    build_impact_summary(store, verification_id)
    invariant_refs = evaluate_behavioral_invariants(store, verification_id)
    impact = store.read("impact_summaries", verification_id)["record"]
    candidate_snapshot = store.read("activation_snapshots", component_snapshot_id)
    candidate_graph = store.read("canonical_graphs", candidate_snapshot["record"]["graph_id"])
    return {
        "component": component_name,
        "snapshot_id": component_snapshot_id,
        "impact": impact,
        "clean": all_behavioral_invariants_pass(store, invariant_refs),
        "policy": candidate_graph["record"]["compiled_policy"],
    }


def analyze_interactions(store: ArtifactStore, verification_id: str, invariant_refs: tuple[str, ...]) -> str:
    context = store.read("verification_contexts", verification_id)["record"]
    component_snapshots = context.get("scope", {}).get("component_snapshot_ids", {})
    component_results = [
        _component_result(
            store,
            base_context=context,
            component_name=name,
            component_snapshot_id=snapshot_id,
        )
        for name, snapshot_id in sorted(component_snapshots.items())
    ]
    combined_impact = store.read("impact_summaries", verification_id)["record"]
    combined_clean = all_behavioral_invariants_pass(store, invariant_refs)
    pairwise_results: list[dict[str, Any]] = []
    nonlinear_effects_detected = False
    blocked_reasons: list[str] = []
    for left, right in combinations(component_results, 2):
        left_flat = _flatten(left["policy"])
        right_flat = _flatten(right["policy"])
        conflicting_paths = sorted(
            path
            for path in set(left_flat) & set(right_flat)
            if left_flat[path] != right_flat[path]
            and any(token in path for token in ("threshold", "limit", "allow", "permission", "scope", "action"))
        )
        isolated_total = abs(float(left["impact"]["expected_risk_delta"])) + abs(float(right["impact"]["expected_risk_delta"]))
        combined_total = abs(float(combined_impact["expected_risk_delta"]))
        nonlinear = combined_total > isolated_total
        safe_alone_unsafe_together = left["clean"] and right["clean"] and not combined_clean
        pair_blocked_reason = ""
        if conflicting_paths:
            pair_blocked_reason = f"conflicting_policy_paths:{','.join(conflicting_paths)}"
        elif safe_alone_unsafe_together:
            pair_blocked_reason = "safe_alone_unsafe_together"
        elif nonlinear:
            pair_blocked_reason = "combined_impact_exceeds_isolated_sum"
        pairwise_results.append(
            {
                "pair": [left["component"], right["component"]],
                "conflicting_paths": conflicting_paths,
                "isolated_risk_delta_sum": isolated_total,
                "combined_risk_delta": combined_total,
                "nonlinear_effect_detected": nonlinear,
                "safe_alone_unsafe_together": safe_alone_unsafe_together,
                "blocked_reason": pair_blocked_reason,
            }
        )
        if pair_blocked_reason:
            blocked_reasons.append(pair_blocked_reason)
        nonlinear_effects_detected = nonlinear_effects_detected or nonlinear or safe_alone_unsafe_together or bool(conflicting_paths)
    autonomy_execution_coupling = (
        component_results
        and float(combined_impact.get("expected_autonomy_delta", 0.0)) > 0
        and float(combined_impact.get("expected_execution_delta", 0.0)) > 0
        and any(float(result["impact"].get("expected_autonomy_delta", 0.0)) > 0 for result in component_results)
        and any(float(result["impact"].get("expected_execution_delta", 0.0)) > 0 for result in component_results)
    )
    if autonomy_execution_coupling:
        blocked_reasons.append("safe_alone_unsafe_together:autonomy_execution_coupling")
        nonlinear_effects_detected = True
    if component_results and all(result["clean"] for result in component_results) and not combined_clean:
        blocked_reasons.append("combined_result_blocks_clean_verification")
        nonlinear_effects_detected = True
    if blocked_reasons:
        combined_clean = False
    combined_result = {
        "clean_verification": combined_clean,
        "decision_surface_delta_count": combined_impact["decision_surface_delta_count"],
        "expected_risk_delta": combined_impact["expected_risk_delta"],
        "expected_turnover_delta": combined_impact["expected_turnover_delta"],
        "summary_classification": combined_impact["summary_classification"],
    }
    artifact_hash = content_hash(
        {
            "verification_id": verification_id,
            "component_proposals": tuple(sorted(component_snapshots.keys())),
            "pairwise_results": pairwise_results,
            "combined_result": combined_result,
            "nonlinear_effects_detected": nonlinear_effects_detected,
            "blocked_reason": "|".join(sorted(set(blocked_reasons))),
        }
    )
    record = InteractionAnalysisResult(
        verification_id=verification_id,
        component_proposals=tuple(sorted(component_snapshots.keys())),
        pairwise_results=tuple(pairwise_results),
        combined_result=combined_result,
        nonlinear_effects_detected=nonlinear_effects_detected,
        blocked_reason="|".join(sorted(set(blocked_reasons))),
        artifact_hash=artifact_hash,
    )
    store.write_immutable(
        "interaction_analysis_results",
        verification_id,
        record,
        artifact_type="InteractionAnalysisResult",
    )
    return verification_id
