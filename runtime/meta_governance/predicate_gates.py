from __future__ import annotations

from .approval_policy import approval_is_sufficient, determine_required_approval
from .interpreter_version import get_interpreter_version
from .schemas import canonical_json_bytes
from .store import ArtifactStore
from .types import PredicateEvaluation

REQUIRED_PREDICATES = (
    "invariant_preservation",
    "dependency_closure",
    "audit_completeness",
    "rollback_readiness",
    "interpreter_pinning",
    "activation_snapshot_completeness",
    "approval_sufficiency",
    "no_forbidden_scope_broadening",
    "bounds_validity",
    "schema_validity",
)


def _predicate(
    proposal_id: str,
    predicate_id: str,
    result: bool,
    *,
    evidence_refs: tuple[str, ...] = (),
    failure_reason: str = "",
) -> PredicateEvaluation:
    return PredicateEvaluation(
        proposal_id=proposal_id,
        predicate_id=predicate_id,
        result=result,
        evidence_refs=evidence_refs,
        failure_reason=failure_reason,
    )


def evaluate_predicates(
    store: ArtifactStore,
    proposal: dict,
    assessment: dict,
    *,
    proposal_diff: dict | None = None,
    approval: dict | None = None,
    graph: dict | None = None,
    snapshot: dict | None = None,
    rollback_snapshot_ref: str | None = None,
    evidence_refs: tuple[str, ...] = (),
    parameter_group: dict | None = None,
    full_invariant_review: bool = False,
) -> str:
    proposal_id = proposal["record"]["proposal_id"]
    required_level = determine_required_approval(assessment, proposal)
    results: list[PredicateEvaluation] = []
    schema_validity = True
    try:
        canonical_json_bytes(proposal["record"])
        if proposal_diff is not None:
            canonical_json_bytes(proposal_diff["record"])
        if graph is not None:
            canonical_json_bytes(graph["record"])
        if snapshot is not None:
            canonical_json_bytes(snapshot["record"])
    except Exception as exc:
        schema_validity = False
        schema_failure = str(exc)
    else:
        schema_failure = ""
    results.append(_predicate(proposal_id, "schema_validity", schema_validity, failure_reason=schema_failure))

    dependency_ok = True
    if graph is not None:
        module_ids = set(graph["record"]["active_module_versions"].keys())
        dependency_ok = all(target in module_ids for _, target in graph["record"]["dependency_edges"])
    results.append(_predicate(proposal_id, "dependency_closure", dependency_ok, evidence_refs=evidence_refs, failure_reason="" if dependency_ok else "DEPENDENCY_MISSING"))

    audit_ok = bool(evidence_refs)
    results.append(_predicate(proposal_id, "audit_completeness", audit_ok, evidence_refs=evidence_refs, failure_reason="" if audit_ok else "AUDIT_ARTIFACT_MISSING"))

    rollback_ok = bool(rollback_snapshot_ref and store.exists("activation_snapshots", rollback_snapshot_ref))
    results.append(_predicate(proposal_id, "rollback_readiness", rollback_ok, evidence_refs=((rollback_snapshot_ref,) if rollback_snapshot_ref else ()), failure_reason="" if rollback_ok else "ROLLBACK_TARGET_MISSING"))

    interpreter_ok = True
    expected = get_interpreter_version()
    for candidate in (graph, snapshot):
        if candidate is None:
            continue
        actual = candidate["record"].get("interpreter_version")
        if actual != expected:
            interpreter_ok = False
    results.append(_predicate(proposal_id, "interpreter_pinning", interpreter_ok, failure_reason="" if interpreter_ok else "INTERPRETER_VERSION_MISMATCH"))

    snapshot_ok = bool(
        snapshot is not None
        and snapshot["record"].get("graph_hash")
        and snapshot["record"].get("interpreter_version")
        and snapshot["record"].get("approval_refs")
        and snapshot["record"].get("evaluation_refs")
        and snapshot["record"].get("rollback_snapshot_ref")
    )
    results.append(_predicate(proposal_id, "activation_snapshot_completeness", snapshot_ok, failure_reason="" if snapshot_ok else "ACTIVATION_SNAPSHOT_INCOMPLETE"))

    approval_ok = approval is not None and approval_is_sufficient(
        required_level,
        approval["record"]["required_approval_level"],
    )
    results.append(_predicate(proposal_id, "approval_sufficiency", approval_ok, failure_reason="" if approval_ok else "APPROVAL_INSUFFICIENT"))

    forbidden_broadening = assessment["record"]["audit_surface_changed"]
    if "approval_semantics_changed" in assessment["record"]["rationale"]:
        forbidden_broadening = True
    if assessment["record"]["autonomy_surface_changed"] and not approval_ok:
        forbidden_broadening = True
    results.append(
        _predicate(
            proposal_id,
            "no_forbidden_scope_broadening",
            not forbidden_broadening,
            failure_reason="" if not forbidden_broadening else "FORBIDDEN_SCOPE_BROADENING",
        )
    )

    bounds_ok = True
    if parameter_group is not None:
        values = dict(parameter_group["record"]["ref"]["default_values"])
        values.update(parameter_group["record"]["ref"]["active_values"])
        for key, bounds in parameter_group["record"]["ref"]["bounds"].items():
            value = values.get(key)
            if value is None:
                continue
            if bounds.get("min") is not None and value < bounds["min"]:
                bounds_ok = False
            if bounds.get("max") is not None and value > bounds["max"]:
                bounds_ok = False
    results.append(_predicate(proposal_id, "bounds_validity", bounds_ok, failure_reason="" if bounds_ok else "BOUNDS_INVALID"))

    invariant_ok = full_invariant_review or proposal["record"]["target_tier"] not in {"tier_0", "tier_1"}
    results.append(_predicate(proposal_id, "invariant_preservation", invariant_ok, failure_reason="" if invariant_ok else "FULL_INVARIANT_REVIEW_REQUIRED"))

    artifact_id = f"predicate_evaluations__{proposal_id}"
    store.write_immutable(
        "evaluation_artifacts",
        artifact_id,
        {"proposal_id": proposal_id, "evaluations": results},
        artifact_type="PredicateEvaluations",
    )
    return artifact_id


def all_required_predicates_pass(evaluation_artifact: dict) -> bool:
    evaluations = evaluation_artifact["record"]["evaluations"]
    result_map = {item["predicate_id"]: bool(item["result"]) for item in evaluations}
    return all(result_map.get(predicate_id, False) for predicate_id in REQUIRED_PREDICATES)
