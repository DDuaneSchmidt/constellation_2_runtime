from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.projections.hypothesis_queue_projection import PROJECTION_TYPE, read_hypothesis_queue_projection
from research_lab.projections.operator_queue_projection import PROJECTION_TYPE as OPERATOR_PROJECTION_TYPE, read_operator_queue_projection
from research_lab.projections.projection_contracts import validate_queue_projection
from research_lab.storage.manifest_io import read_jsonl
from research_lab.storage.paths import ensure_store_layout


def validate_hypothesis_queue_projection(*, store_root: Path | None = None, strict: bool = False, actor: str = "AegisProjection") -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    warnings: list[str] = []
    errors: list[str] = []
    projection = read_hypothesis_queue_projection(store_root=store)
    if projection is None:
        errors.append("projection_latest_missing")
        payload = _payload(False, warnings, errors)
        _audit(store, actor, payload)
        if strict:
            raise RuntimeError("projection latest missing")
        return payload
    try:
        validate_queue_projection(projection)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"projection_latest_malformed:{exc}")
    proposal_registry_path = store / "registries" / "hypothesis_proposals.jsonl"
    proposal_rows = read_jsonl(proposal_registry_path) if proposal_registry_path.exists() else []
    source_ids = {str(row.get("hypothesis_proposal_id") or "") for row in proposal_rows if row.get("hypothesis_proposal_id")}
    artifact_ids = {path.stem for path in (store / "event_intake" / "hypothesis_proposals").glob("*.json")}
    projected_ids = {str(item.get("item_id") or "") for item in projection.get("items") or []}
    missing = sorted((source_ids | artifact_ids) - projected_ids)
    if missing:
        errors.append(f"source_proposals_missing_from_projection:{','.join(missing)}")
    for item in projection.get("items") or []:
        if "missing_artifact" in (item.get("display_flags") or []):
            continue
        for source in item.get("source_refs") or []:
            if source.get("kind") == "artifact" and source.get("path") and not Path(str(source["path"])).exists():
                errors.append(f"projected_item_references_missing_artifact:{item.get('item_id')}")
    empty_state = projection.get("empty_state") or {}
    if not projection.get("items") and not empty_state.get("trusted_empty"):
        errors.append("empty_projection_not_trusted")
    if empty_state.get("trusted_empty"):
        if proposal_rows or artifact_ids or projection.get("integrity_status") != "ok":
            errors.append("trusted_empty_rules_violated")
    payload = _payload(not errors, warnings, errors, projection=projection, source_ids=sorted(source_ids), artifact_ids=sorted(artifact_ids), projected_ids=sorted(projected_ids))
    _audit(store, actor, payload)
    if strict and errors:
        raise RuntimeError("projection validation failed")
    return payload



def validate_operator_queue_projection(*, store_root: Path | None = None, strict: bool = False, actor: str = "AegisProjection") -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    warnings: list[str] = []
    errors: list[str] = []
    projection = read_operator_queue_projection(store_root=store)
    if projection is None:
        errors.append("operator_projection_latest_missing")
        payload = _payload(False, warnings, errors)
        _audit(store, actor, payload, projection_type=OPERATOR_PROJECTION_TYPE)
        if strict:
            raise RuntimeError("operator projection latest missing")
        return payload
    required = ["projection_id", "projection_type", "built_at", "source_summary", "integrity_status", "queues", "empty_state", "content_hash"]
    missing_fields = [field for field in required if field not in projection]
    if missing_fields:
        errors.append(f"operator_projection_missing_fields:{','.join(missing_fields)}")
    queues = projection.get("queues") or {}
    for name in ["research_plans", "evidence", "paper_trials", "sleeve_reviews", "blocked_work", "research_backlog"]:
        if name not in queues:
            errors.append(f"operator_projection_queue_missing:{name}")
        if not queues.get(name) and not ((projection.get("empty_state") or {}).get(name) or {}).get("trusted_empty"):
            errors.append(f"operator_projection_empty_not_trusted:{name}")
    payload = _payload(not errors, warnings, errors, projection=projection)
    _audit(store, actor, payload, projection_type=OPERATOR_PROJECTION_TYPE)
    if strict and errors:
        raise RuntimeError("operator projection validation failed")
    return payload

def validate_research_projections(*, projection: str = "all", store_root: Path | None = None, strict: bool = False, actor: str = "AegisProjection") -> dict[str, Any]:
    if projection not in {"all", PROJECTION_TYPE, OPERATOR_PROJECTION_TYPE}:
        raise ValueError(f"unsupported projection: {projection}")
    result: dict[str, Any] = {}
    if projection in {"all", PROJECTION_TYPE}:
        result[PROJECTION_TYPE] = validate_hypothesis_queue_projection(store_root=store_root, strict=strict, actor=actor)
    if projection in {"all", OPERATOR_PROJECTION_TYPE}:
        result[OPERATOR_PROJECTION_TYPE] = validate_operator_queue_projection(store_root=store_root, strict=strict, actor=actor)
    return {"ok": all(item["ok"] for item in result.values()), "projections": result}


def _payload(ok: bool, warnings: list[str], errors: list[str], **extra: Any) -> dict[str, Any]:
    return {"ok": ok, "warnings": warnings, "errors": errors, **extra}


def _audit(store: Path, actor: str, payload: dict[str, Any], *, projection_type: str = PROJECTION_TYPE) -> None:
    write_audit_event(actor=actor, entity_type="research_projection", entity_id=projection_type, action="research_projection_validation_completed", previous_state_hash="", new_state_hash="", reason="Validated research projection.", metadata={"ok": payload["ok"], "warnings": payload.get("warnings") or [], "errors": payload.get("errors") or []}, store_root=store)
