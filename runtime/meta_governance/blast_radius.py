from __future__ import annotations

from .approval_policy import APPROVAL_ORDER
from .module_registry import get_module_document, get_parameter_group_document
from .store import ArtifactStore
from .tiers import TIER_0, TIER_1
from .types import BlastRadiusAssessment


def _bool_from_paths(paths: list[str], *needles: str) -> bool:
    lowered = [path.lower() for path in paths]
    return any(any(needle in path for needle in needles) for path in lowered)


def _is_approval_weakening(changes: tuple[dict, ...]) -> bool:
    for change in changes:
        path = str(change["path"]).lower()
        if "approval" not in path:
            continue
        before = change["before"]
        after = change["after"]
        if isinstance(before, str) and isinstance(after, str):
            if before in APPROVAL_ORDER and after in APPROVAL_ORDER and APPROVAL_ORDER[after] < APPROVAL_ORDER[before]:
                return True
        if isinstance(before, bool) and isinstance(after, bool) and before and not after:
            return True
        if before is not None and after is None:
            return True
    return False


def _is_audit_weakening(changes: tuple[dict, ...]) -> bool:
    for change in changes:
        path = str(change["path"]).lower()
        if not any(token in path for token in ("audit", "lineage", "event", "snapshot")):
            continue
        before = change["before"]
        after = change["after"]
        if isinstance(before, bool) and isinstance(after, bool) and before and not after:
            return True
        if isinstance(before, list) and isinstance(after, list) and len(after) < len(before):
            return True
        if before is not None and after is None:
            return True
    return False


def _is_autonomy_broadening(diff: dict, paths: list[str], changes: tuple[dict, ...]) -> bool:
    if diff["new_permissions"]:
        return True
    for change in changes:
        path = str(change["path"]).lower()
        if not any(token in path for token in ("autonomy", "permission", "automation")):
            continue
        before = change["before"]
        after = change["after"]
        if isinstance(before, bool) and isinstance(after, bool) and not before and after:
            return True
        if isinstance(before, list) and isinstance(after, list) and len(after) > len(before):
            return True
    return _bool_from_paths(paths, "autonomy", "permission", "automation")


def _module_closure(store: ArtifactStore, module_id: str) -> tuple[set[str], set[str], set[str], set[str]]:
    queue = [module_id]
    visited: set[str] = set()
    module_ids: set[str] = set()
    domains: set[str] = set()
    tiers: set[str] = set()
    invariants: set[str] = set()
    while queue:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)
        try:
            document = get_module_document(store, current)
        except FileNotFoundError:
            continue
        ref = document["record"]["ref"]
        module_ids.add(ref["module_id"])
        domains.add(ref["semantic_domain"])
        tiers.add(ref["tier"])
        invariants.update(ref.get("invariant_ids", ()))
        queue.extend(ref.get("dependency_ids", ()))
    return module_ids, domains, tiers, invariants


def assess_blast_radius(
    store: ArtifactStore,
    proposal: dict,
    proposal_diff: dict,
    *,
    semantic_domain: str = "",
) -> str:
    record = proposal["record"]
    diff = proposal_diff["record"]
    paths = list(diff["semantic_diff"].get("changed_paths", []))
    domains = {semantic_domain or record["target_type"]}
    for domain in record.get("proposed_scope", {}).get("domains", []):
        domains.add(str(domain))
    transitive_module_ids: set[str] = set()
    transitive_tiers: set[str] = set()
    transitive_invariants: set[str] = set(record.get("invariants_touched", ()))
    transitive_only_domains: set[str] = set()
    target_module_id = None
    if record["target_type"] == "policy_module":
        target_module_id = record["target_id"]
    elif record["target_type"] == "parameter_group":
        try:
            parameter_document = get_parameter_group_document(store, record["target_id"])
            target_module_id = parameter_document["record"]["ref"]["parent_module_id"]
        except FileNotFoundError:
            target_module_id = None
    if target_module_id:
        closure_module_ids, closure_domains, closure_tiers, closure_invariants = _module_closure(store, target_module_id)
        transitive_module_ids.update(closure_module_ids)
        domains.update(closure_domains)
        transitive_only_domains.update(domain for domain in closure_domains if domain != semantic_domain)
        transitive_tiers.update(closure_tiers)
        transitive_invariants.update(closure_invariants)
    approval_semantics_changed = _bool_from_paths(paths, "approval")
    approval_semantics_weakened = _is_approval_weakening(diff["structural_diff"])
    autonomy_surface_changed = _is_autonomy_broadening(diff, paths, diff["structural_diff"])
    execution_surface_changed = _bool_from_paths(paths, "execution", "action") or "execution" in transitive_only_domains
    tax_surface_changed = _bool_from_paths(paths, "tax") or "tax" in transitive_only_domains
    risk_surface_changed = _bool_from_paths(paths, "risk", "drawdown")
    audit_surface_changed = _bool_from_paths(paths, "audit", "lineage", "snapshot", "event_log") or _is_audit_weakening(diff["structural_diff"])
    invariant_surface_changed = bool(transitive_invariants) or _bool_from_paths(paths, "invariant")
    capital_exposure_class = (
        record.get("dependency_impact", {}).get("capital_exposure_class")
        or record.get("proposed_scope", {}).get("capital_exposure_class")
        or "standard"
    )
    tiers_touched = set(record.get("proposed_scope", {}).get("tiers_touched", []))
    tiers_touched.add(record["target_tier"])
    tiers_touched.update(transitive_tiers)
    if record["target_tier"] in (TIER_0, TIER_1) or TIER_0 in tiers_touched or TIER_1 in tiers_touched or autonomy_surface_changed or audit_surface_changed or approval_semantics_weakened or approval_semantics_changed:
        classification = "critical"
    elif execution_surface_changed or tax_surface_changed or risk_surface_changed or invariant_surface_changed or capital_exposure_class in {"elevated", "high"}:
        classification = "moderate"
    else:
        classification = "safe"
    rationale_parts = []
    if record["target_tier"] in (TIER_0, TIER_1):
        rationale_parts.append("tier_0_or_tier_1_change")
    if autonomy_surface_changed:
        rationale_parts.append("autonomy_surface_changed")
    if audit_surface_changed:
        rationale_parts.append("audit_surface_changed")
    if approval_semantics_weakened:
        rationale_parts.append("approval_semantics_weakened")
    if approval_semantics_changed:
        rationale_parts.append("approval_semantics_changed")
    if execution_surface_changed:
        rationale_parts.append("execution_surface_changed")
    if tax_surface_changed:
        rationale_parts.append("tax_surface_changed")
    if risk_surface_changed:
        rationale_parts.append("risk_surface_changed")
    if invariant_surface_changed:
        rationale_parts.append("invariant_surface_changed")
    if transitive_module_ids:
        rationale_parts.append(f"transitive_modules:{','.join(sorted(transitive_module_ids))}")
    assessment = BlastRadiusAssessment(
        proposal_id=record["proposal_id"],
        tiers_touched=tuple(sorted(tiers_touched)),
        domains_touched=tuple(sorted(domains)),
        autonomy_surface_changed=autonomy_surface_changed,
        execution_surface_changed=execution_surface_changed,
        tax_surface_changed=tax_surface_changed,
        risk_surface_changed=risk_surface_changed,
        audit_surface_changed=audit_surface_changed,
        capital_exposure_class=str(capital_exposure_class),
        classification_result=classification,
        rationale=",".join(rationale_parts) or "bounded_change",
    )
    store.write_immutable(
        "evaluation_artifacts",
        f"blast_radius__{record['proposal_id']}",
        assessment,
        artifact_type="BlastRadiusAssessment",
    )
    return f"blast_radius__{record['proposal_id']}"
