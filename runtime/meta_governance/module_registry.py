from __future__ import annotations

from typing import Any

from .interpreter_version import require_matching_interpreter
from .lifecycle import validate_state
from .schemas import content_hash
from .store import ArtifactStore
from .tiers import validate_tier
from .types import GovernanceModuleRef, ParameterGroupRef


def _versioned_id(entity_id: str, version: str) -> str:
    return f"{entity_id}__{version}"


def _assert_parameter_bounds(group: ParameterGroupRef) -> None:
    all_values = dict(group.default_values)
    all_values.update(group.active_values)
    for key, bounds in group.bounds.items():
        if key not in all_values:
            continue
        value = all_values[key]
        minimum = bounds.get("min")
        maximum = bounds.get("max")
        if minimum is not None and value < minimum:
            raise ValueError(f"PARAMETER_OUT_OF_BOUNDS:{group.group_id}:{key}:min")
        if maximum is not None and value > maximum:
            raise ValueError(f"PARAMETER_OUT_OF_BOUNDS:{group.group_id}:{key}:max")


def register_module(store: ArtifactStore, module_ref: GovernanceModuleRef, module_content: dict[str, Any]) -> str:
    validate_tier(module_ref.tier)
    validate_state(module_ref.lifecycle_state)
    require_matching_interpreter(module_ref.interpreter_version)
    if module_ref.content_hash != content_hash(module_content):
        raise ValueError(f"MODULE_CONTENT_HASH_MISMATCH:{module_ref.module_id}")
    artifact_id = _versioned_id(module_ref.module_id, module_ref.version)
    store.write_immutable(
        "governance_modules",
        artifact_id,
        {"ref": module_ref, "module_content": module_content},
        artifact_type="GovernanceModule",
    )
    latest = store.read_pointer("latest_module_versions") or {}
    latest[module_ref.module_id] = module_ref.version
    store.write_pointer("latest_module_versions", latest)
    return artifact_id


def register_parameter_group(store: ArtifactStore, group_ref: ParameterGroupRef) -> str:
    validate_state(group_ref.lifecycle_state)
    _assert_parameter_bounds(group_ref)
    if group_ref.content_hash != content_hash(
        {
            "bounds": group_ref.bounds,
            "default_values": group_ref.default_values,
            "active_values": group_ref.active_values,
        }
    ):
        raise ValueError(f"PARAMETER_GROUP_CONTENT_HASH_MISMATCH:{group_ref.group_id}")
    artifact_id = _versioned_id(group_ref.group_id, group_ref.version)
    store.write_immutable(
        "parameter_groups",
        artifact_id,
        {"ref": group_ref},
        artifact_type="ParameterGroup",
    )
    latest = store.read_pointer("latest_parameter_versions") or {}
    latest[group_ref.group_id] = group_ref.version
    store.write_pointer("latest_parameter_versions", latest)
    return artifact_id


def get_module_document(
    store: ArtifactStore,
    module_id: str,
    version: str | None = None,
) -> dict[str, Any]:
    latest = store.read_pointer("latest_module_versions") or {}
    resolved_version = version or latest.get(module_id)
    if not resolved_version:
        raise FileNotFoundError(f"MODULE_VERSION_UNKNOWN:{module_id}")
    return store.read("governance_modules", _versioned_id(module_id, resolved_version))


def get_parameter_group_document(
    store: ArtifactStore,
    group_id: str,
    version: str | None = None,
) -> dict[str, Any]:
    latest = store.read_pointer("latest_parameter_versions") or {}
    resolved_version = version or latest.get(group_id)
    if not resolved_version:
        raise FileNotFoundError(f"PARAMETER_GROUP_VERSION_UNKNOWN:{group_id}")
    return store.read("parameter_groups", _versioned_id(group_id, resolved_version))
