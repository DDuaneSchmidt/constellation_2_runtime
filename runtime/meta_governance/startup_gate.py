from __future__ import annotations

from .audit_log import emit_audit_event
from .interpreter_version import get_interpreter_version
from .lifecycle import STATE_ACTIVE, STATE_APPROVED
from .module_registry import get_module_document, get_parameter_group_document
from .store import ArtifactStore

ACTIVE_RUNTIME_SNAPSHOT_STATES = {STATE_APPROVED, STATE_ACTIVE}
ACTIVE_RUNTIME_COMPONENT_STATES = {STATE_ACTIVE}


def _fail(store: ArtifactStore, actor: str, event_type: str, reason: str, artifact_refs: tuple[str, ...]) -> None:
    emit_audit_event(store, event_type, actor, artifact_refs=artifact_refs, details={"reason": reason})
    raise ValueError(reason)


def validate_snapshot_authority(
    store: ArtifactStore,
    snapshot_id: str,
    *,
    actor: str,
    event_type: str,
) -> dict:
    if not snapshot_id or not store.exists("activation_snapshots", snapshot_id):
        _fail(store, actor, event_type, "ACTIVE_SNAPSHOT_POINTER_INVALID", ())
    snapshot = store.read("activation_snapshots", snapshot_id)
    snapshot_record = snapshot["record"]
    if snapshot_record.get("lifecycle_state") not in ACTIVE_RUNTIME_SNAPSHOT_STATES:
        _fail(store, actor, event_type, "SNAPSHOT_LIFECYCLE_INVALID_FOR_RUNTIME", (snapshot_id,))
    required_snapshot_fields = (
        "graph_id",
        "graph_hash",
        "interpreter_version",
        "approval_refs",
        "evaluation_refs",
        "rollback_snapshot_ref",
    )
    for field in required_snapshot_fields:
        if not snapshot_record.get(field):
            _fail(store, actor, event_type, f"SNAPSHOT_FIELD_MISSING:{field}", (snapshot_id,))
    if not store.exists("activation_snapshots", snapshot_record["rollback_snapshot_ref"]):
        _fail(store, actor, event_type, "ROLLBACK_TARGET_MISSING", (snapshot_id, snapshot_record["rollback_snapshot_ref"]))
    if snapshot_record.get("prior_snapshot_id") is None and snapshot_record.get("rollback_snapshot_ref") != snapshot_id:
        _fail(store, actor, event_type, "ROLLBACK_PREDECESSOR_REQUIRED", (snapshot_id,))
    if not store.exists("canonical_graphs", snapshot_record["graph_id"]):
        _fail(store, actor, event_type, "GRAPH_REQUIRED", (snapshot_id, snapshot_record["graph_id"]))
    graph = store.read("canonical_graphs", snapshot_record["graph_id"])
    graph_record = graph["record"]
    if graph_record["graph_hash"] != snapshot_record["graph_hash"]:
        _fail(store, actor, event_type, "GRAPH_HASH_MISMATCH", (snapshot_id, snapshot_record["graph_id"]))
    current_interpreter = get_interpreter_version()
    if not snapshot_record.get("interpreter_version"):
        _fail(store, actor, event_type, "INTERPRETER_VERSION_REQUIRED", (snapshot_id,))
    if graph_record.get("interpreter_version") != current_interpreter or snapshot_record.get("interpreter_version") != current_interpreter:
        _fail(store, actor, event_type, "INTERPRETER_VERSION_MISMATCH", (snapshot_id, snapshot_record["graph_id"]))
    module_versions = graph_record.get("active_module_versions", {})
    parameter_versions = graph_record.get("active_parameter_versions", {})
    for source_module, target_module in graph_record.get("dependency_edges", []):
        if source_module not in module_versions or target_module not in module_versions:
            _fail(store, actor, event_type, "UNRESOLVED_GRAPH_DEPENDENCY", (snapshot_id, snapshot_record["graph_id"]))
    for module_id, version in module_versions.items():
        try:
            module_document = get_module_document(store, module_id, version)
        except FileNotFoundError:
            _fail(store, actor, event_type, f"MODULE_DOCUMENT_MISSING:{module_id}:{version}", (snapshot_id, snapshot_record["graph_id"]))
        lifecycle_state = module_document["record"]["ref"]["lifecycle_state"]
        if lifecycle_state not in ACTIVE_RUNTIME_COMPONENT_STATES:
            _fail(
                store,
                actor,
                event_type,
                f"MODULE_LIFECYCLE_INVALID_FOR_RUNTIME:{module_id}:{lifecycle_state}",
                (snapshot_id, snapshot_record["graph_id"]),
            )
    for group_id, version in parameter_versions.items():
        try:
            parameter_document = get_parameter_group_document(store, group_id, version)
        except FileNotFoundError:
            _fail(store, actor, event_type, f"PARAMETER_GROUP_DOCUMENT_MISSING:{group_id}:{version}", (snapshot_id, snapshot_record["graph_id"]))
        lifecycle_state = parameter_document["record"]["ref"].get("lifecycle_state")
        if lifecycle_state not in ACTIVE_RUNTIME_COMPONENT_STATES:
            _fail(
                store,
                actor,
                event_type,
                f"PARAMETER_GROUP_LIFECYCLE_INVALID_FOR_RUNTIME:{group_id}:{lifecycle_state}",
                (snapshot_id, snapshot_record["graph_id"]),
            )
    return {
        "snapshot": snapshot_record,
        "graph": graph_record,
        "compiled_policy": graph_record["compiled_policy"],
    }


def validate_runtime_authority(
    store: ArtifactStore,
    *,
    actor: str,
    event_type: str,
) -> dict:
    pointer = store.read_pointer("active_snapshot_ref")
    if pointer is None:
        _fail(store, actor, event_type, "ACTIVE_SNAPSHOT_REQUIRED", ())
    return validate_snapshot_authority(
        store,
        pointer.get("snapshot_id"),
        actor=actor,
        event_type=event_type,
    )
