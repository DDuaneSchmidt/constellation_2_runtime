from __future__ import annotations

from copy import deepcopy
from typing import Any

from .interpreter_version import get_interpreter_version
from .module_registry import get_module_document, get_parameter_group_document
from .schemas import content_hash, utc_now
from .store import ArtifactStore
from .types import CanonicalGovernanceGraph


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def compile_canonical_graph(
    store: ArtifactStore,
    *,
    active_modules: list[tuple[str, str]] | None = None,
    active_parameter_groups: list[tuple[str, str]] | None = None,
) -> str:
    latest_modules = store.read_pointer("latest_module_versions") or {}
    latest_parameters = store.read_pointer("latest_parameter_versions") or {}
    module_pairs = active_modules or sorted(latest_modules.items())
    parameter_pairs = active_parameter_groups or sorted(latest_parameters.items())
    module_docs = [get_module_document(store, module_id, version) for module_id, version in sorted(module_pairs)]
    parameter_docs = [get_parameter_group_document(store, group_id, version) for group_id, version in sorted(parameter_pairs)]
    active_module_versions = {
        doc["record"]["ref"]["module_id"]: doc["record"]["ref"]["version"] for doc in module_docs
    }
    for doc in module_docs:
        ref = doc["record"]["ref"]
        for dependency_id in ref["dependency_ids"]:
            if dependency_id not in active_module_versions:
                raise ValueError(f"MISSING_DEPENDENCY:{ref['module_id']}->{dependency_id}")
    resolved_modules = {
        doc["record"]["ref"]["module_id"]: {
            "tier": doc["record"]["ref"]["tier"],
            "semantic_domain": doc["record"]["ref"]["semantic_domain"],
            "version": doc["record"]["ref"]["version"],
            "interpreter_version": doc["record"]["ref"]["interpreter_version"],
            "content": doc["record"]["module_content"],
        }
        for doc in module_docs
    }
    resolved_parameter_values: dict[str, Any] = {}
    compiled_policy: dict[str, Any] = {}
    for doc in module_docs:
        compiled_policy = _deep_merge(compiled_policy, doc["record"]["module_content"])
    for doc in parameter_docs:
        ref = doc["record"]["ref"]
        resolved_values = dict(ref["default_values"])
        resolved_values.update(ref["active_values"])
        resolved_parameter_values[ref["group_id"]] = {
            "parent_module_id": ref["parent_module_id"],
            "values": resolved_values,
            "bounds": ref["bounds"],
            "version": ref["version"],
        }
        compiled_policy = _deep_merge(compiled_policy, resolved_values)
    dependency_edges = tuple(
        sorted(
            (doc["record"]["ref"]["module_id"], dependency_id)
            for doc in module_docs
            for dependency_id in doc["record"]["ref"]["dependency_ids"]
        )
    )
    invariants = tuple(
        sorted(
            {
                invariant_id
                for doc in module_docs
                for invariant_id in doc["record"]["ref"]["invariant_ids"]
            }
        )
    )
    compiled_at = utc_now()
    base_graph = {
        "interpreter_version": get_interpreter_version(),
        "active_module_versions": active_module_versions,
        "active_parameter_versions": {
            doc["record"]["ref"]["group_id"]: doc["record"]["ref"]["version"] for doc in parameter_docs
        },
        "dependency_edges": dependency_edges,
        "invariants": invariants,
        "resolved_modules": resolved_modules,
        "resolved_parameter_values": resolved_parameter_values,
        "compiled_policy": compiled_policy,
    }
    graph_hash = content_hash(base_graph)
    graph = CanonicalGovernanceGraph(
        graph_id=f"graph-{graph_hash[:12]}",
        graph_hash=graph_hash,
        interpreter_version=get_interpreter_version(),
        active_module_versions=base_graph["active_module_versions"],
        active_parameter_versions=base_graph["active_parameter_versions"],
        dependency_edges=dependency_edges,
        invariants=invariants,
        compiled_at=compiled_at,
        resolved_modules=resolved_modules,
        resolved_parameter_values=resolved_parameter_values,
        compiled_policy=compiled_policy,
    )
    store.write_immutable(
        "canonical_graphs",
        graph.graph_id,
        graph,
        artifact_type="CanonicalGovernanceGraph",
    )
    return graph.graph_id
