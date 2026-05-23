from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.projections.projection_contracts import (
    HYPOTHESIS_LANES,
    PROJECTION_BUILD_SCHEMA_VERSION,
    QUEUE_ITEM_SCHEMA_VERSION,
    QUEUE_PROJECTION_SCHEMA_VERSION,
    RESEARCH_LABEL,
    validate_projection_build,
    validate_queue_projection,
)
from research_lab.projections.projection_store import projection_output_hash, read_latest_projection, safe_json_load, write_projection_artifacts
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout

PROJECTION_TYPE = "hypothesis_queue"
SOURCE_REGISTRIES = {
    "hypothesis_proposals": "hypothesis_proposals.jsonl",
    "hypothesis_proposal_reviews": "hypothesis_proposal_reviews.jsonl",
    "research_readiness_assessments": "research_readiness_assessments.jsonl",
    "proposal_priority_scores": "proposal_priority_scores.jsonl",
    "research_intake_dossiers": "research_intake_dossiers.jsonl",
}


def read_hypothesis_queue_projection(*, store_root: Path | None = None) -> dict[str, Any] | None:
    return read_latest_projection(PROJECTION_TYPE, store_root=store_root)


def build_hypothesis_queue_projection(*, store_root: Path | None = None, actor: str = "AegisProjection", strict: bool = False, write: bool = True) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    built_at = utc_now_iso()
    build_sequence = len(read_jsonl(store / "registries" / "projection_builds.jsonl")) + 1
    warnings: list[str] = []
    errors: list[str] = []
    write_audit_event(actor=actor, entity_type="research_projection", entity_id=PROJECTION_TYPE, action="research_projection_rebuild_started", previous_state_hash="", new_state_hash="", reason="Started hypothesis queue projection rebuild.", metadata={"strict": strict}, store_root=store)
    registry_data = _load_source_registries(store, warnings, errors, strict=strict)
    proposal_registry = registry_data["hypothesis_proposals"]["rows"]
    proposal_artifacts = _load_artifacts(store / "event_intake" / "hypothesis_proposals", "hypothesis_proposal_id", warnings)
    proposal_ids = sorted(set(proposal_registry) | set(proposal_artifacts))
    related = {
        "reviews": _latest_by_proposal_from_registry_and_artifacts(store, "hypothesis_proposal_reviews", "hypothesis_proposal_review_id", "research_intake/reviews", warnings),
        "readiness": _latest_by_proposal_from_registry_and_artifacts(store, "research_readiness_assessments", "research_readiness_assessment_id", "research_intake/readiness_assessments", warnings),
        "priority": _latest_by_proposal_from_registry_and_artifacts(store, "proposal_priority_scores", "proposal_priority_score_id", "research_intake/priority_scores", warnings),
        "dossiers": _latest_by_proposal_from_registry_and_artifacts(store, "research_intake_dossiers", "research_intake_dossier_id", "research_intake/dossiers", warnings),
    }
    items = [_queue_item_for_proposal(proposal_id, proposal_registry.get(proposal_id), proposal_artifacts.get(proposal_id), related, warnings) for proposal_id in proposal_ids]
    items.sort(key=lambda item: (HYPOTHESIS_LANES.index(item["lane"]) if item["lane"] in HYPOTHESIS_LANES else 99, str(item["title"]), str(item["item_id"])))
    lanes = {lane: [item for item in items if item["lane"] == lane] for lane in HYPOTHESIS_LANES}
    source_summary = _source_summary(store, registry_data, proposal_artifacts)
    trusted_empty = bool(
        source_summary["source_registries_checked"]
        and source_summary["hypothesis_proposal_registry_exists"]
        and source_summary["hypothesis_proposal_registry_row_count"] == 0
        and source_summary["event_intake_proposal_artifact_count"] == 0
        and not warnings
        and not errors
    )
    if not source_summary["hypothesis_proposal_registry_exists"]:
        integrity_status = "source_missing"
    elif errors:
        integrity_status = "error"
    elif warnings or any(item["lane"] == "needs_review" for item in items):
        integrity_status = "warning"
    else:
        integrity_status = "ok"
    if not items and not trusted_empty and integrity_status == "ok":
        integrity_status = "error"
        errors.append("false_empty_queue_detected")
    empty_state = {
        "is_empty": len(items) == 0,
        "trusted_empty": trusted_empty,
        "reason": "trusted_empty_sources" if trusted_empty else ("projection_has_items" if items else "not_trusted_empty"),
        "source_registries_checked": source_summary["source_registries_checked"],
        "hypothesis_proposal_registry_exists": source_summary["hypothesis_proposal_registry_exists"],
        "hypothesis_proposal_registry_row_count": source_summary["hypothesis_proposal_registry_row_count"],
        "event_intake_proposal_artifact_count": source_summary["event_intake_proposal_artifact_count"],
    }
    operator_message = _operator_message(integrity_status, items, trusted_empty)
    projection = {
        "projection_id": f"hqp_{short_hash(content_hash({'built_at': built_at, 'build_sequence': build_sequence, 'items': [item['item_id'] for item in items], 'warnings': warnings, 'errors': errors}), 16)}",
        "projection_type": PROJECTION_TYPE,
        "built_at": built_at,
        "source_summary": source_summary,
        "integrity_status": integrity_status,
        "integrity_warnings": sorted(set(warnings)),
        "integrity_errors": sorted(set(errors)),
        "lanes": lanes,
        "items": items,
        "empty_state": empty_state,
        "operator_message": operator_message,
        "research_label": RESEARCH_LABEL,
        "schema_version": QUEUE_PROJECTION_SCHEMA_VERSION,
    }
    projection["content_hash"] = projection_output_hash(projection)
    build_status = "failed" if errors and strict else ("success_with_warnings" if warnings or errors else "success")
    build = {
        "projection_build_id": f"pb_{short_hash(content_hash({'projection_type': PROJECTION_TYPE, 'built_at': built_at, 'build_sequence': build_sequence, 'output': projection['content_hash']}), 20)}",
        "projection_type": PROJECTION_TYPE,
        "built_at": built_at,
        "built_by": actor,
        "source_registries": {name: str(store / "registries" / filename) for name, filename in SOURCE_REGISTRIES.items()},
        "source_artifact_counts": source_summary["artifact_counts"],
        "source_hashes": source_summary["source_hashes"],
        "output_uri": "research://projections/hypothesis_queue/latest.json",
        "output_hash": projection["content_hash"],
        "status": build_status,
        "warnings": sorted(set(warnings)),
        "errors": sorted(set(errors)),
        "schema_version": PROJECTION_BUILD_SCHEMA_VERSION,
    }
    validate_queue_projection(projection)
    validate_projection_build(build)
    if strict and build_status == "failed":
        write_audit_event(actor=actor, entity_type="research_projection", entity_id=PROJECTION_TYPE, action="research_projection_rebuild_failed", previous_state_hash="", new_state_hash=projection["content_hash"], reason="Projection rebuild failed strict validation.", metadata={"errors": errors, "warnings": warnings}, store_root=store)
        raise RuntimeError("projection validation failed in strict mode")
    paths = {}
    if write:
        markdown = _projection_markdown(projection, build)
        paths = write_projection_artifacts(projection=projection, build=build, markdown=markdown, store_root=store)
        health_payload = {
            "projection_type": PROJECTION_TYPE,
            "latest_build_id": build["projection_build_id"],
            "latest_built_at": build["built_at"],
            "status": build["status"],
            "source_counts": projection["source_summary"].get("registry_counts", {}),
            "projected_item_count": len(projection.get("items") or []),
            "integrity_status": projection.get("integrity_status"),
            "warnings": projection.get("integrity_warnings") or [],
            "errors": projection.get("integrity_errors") or [],
            "stale": False,
        }
        health_path = store / "projections" / "projection_health" / "latest.json"
        existing_health = read_json(health_path) if health_path.exists() else {"projections": {}}
        projections = existing_health.get("projections") or {}
        projections[PROJECTION_TYPE] = health_payload
        existing_health = {"ok": True, **health_payload, "projections": projections}
        write_json(health_path, existing_health, overwrite=True)
        action = "research_projection_rebuild_completed" if build_status != "failed" else "research_projection_rebuild_failed"
        write_audit_event(actor=actor, entity_type="research_projection", entity_id=build["projection_build_id"], action=action, previous_state_hash="", new_state_hash=projection["content_hash"], reason="Completed hypothesis queue projection rebuild.", metadata={"projection_type": PROJECTION_TYPE, "status": build_status, "warnings": warnings, "errors": errors, "paths": paths}, store_root=store)
        if warnings:
            write_audit_event(actor=actor, entity_type="research_projection", entity_id=build["projection_build_id"], action="research_projection_integrity_warning", previous_state_hash="", new_state_hash=projection["content_hash"], reason="Projection built with integrity warnings.", metadata={"warnings": warnings}, store_root=store)
    return {"ok": build_status != "failed", "projection": projection, "build": build, "paths": paths}


def projection_api_payload(*, status: str | None = None, store_root: Path | None = None) -> dict[str, Any]:
    projection = read_hypothesis_queue_projection(store_root=store_root)
    if projection is None:
        return {
            "ok": False,
            "read_only": True,
            "projection_type": PROJECTION_TYPE,
            "integrity_status": "source_missing",
            "operator_message": "Hypothesis queue projection has not been built.",
            "next_allowed_action": "Rebuild research projections",
            "queue": [],
            "hypothesis_proposals": [],
            "lanes": {lane: [] for lane in HYPOTHESIS_LANES},
            "count": 0,
            "non_archived_count": 0,
            "archived_count": 0,
            "discoverable_count": 0,
            "counts_by_lifecycle": {},
            "empty_state": {"is_empty": True, "trusted_empty": False, "reason": "projection_not_built"},
            "projection_source": "latest.json",
        }
    items = list(projection.get("items") or [])
    if status:
        lane = _status_to_lane(status)
        items = [item for item in items if item.get("lane") == lane or item.get("status") == status]
    lanes = {lane: [item for item in items if item.get("lane") == lane] for lane in HYPOTHESIS_LANES}
    rows = [_ui_row(item) for item in items]
    non_archived_rows = [row for row in rows if not _is_archived_row(row)]
    counts_by_lifecycle: dict[str, int] = {}
    for row in rows:
        lifecycle = str(row.get("lifecycle_state") or "Ideas")
        counts_by_lifecycle[lifecycle] = counts_by_lifecycle.get(lifecycle, 0) + 1
    return {
        "ok": True,
        "read_only": True,
        "projection_source": "latest.json",
        "projection_id": projection.get("projection_id"),
        "projection_type": projection.get("projection_type"),
        "projection_built_at": projection.get("built_at"),
        "integrity_status": projection.get("integrity_status"),
        "integrity_warnings": projection.get("integrity_warnings") or [],
        "integrity_errors": projection.get("integrity_errors") or [],
        "source_summary": projection.get("source_summary") or {},
        "source_proposal_count": (projection.get("source_summary") or {}).get("hypothesis_proposal_registry_row_count", 0),
        "projected_item_count": len(projection.get("items") or []),
        "operator_message": projection.get("operator_message"),
        "empty_state": projection.get("empty_state") or {},
        "trusted_empty": bool((projection.get("empty_state") or {}).get("trusted_empty")),
        "items": items,
        "queue": rows,
        "hypothesis_proposals": rows,
        "lanes": {lane: [_ui_row(item) for item in lane_items] for lane, lane_items in lanes.items()},
        "count": len(rows),
        "non_archived_count": len(non_archived_rows),
        "archived_count": len(rows) - len(non_archived_rows),
        "discoverable_count": len(non_archived_rows),
        "counts_by_lifecycle": counts_by_lifecycle,
        "recovered_count": sum(1 for item in projection.get("items") or [] if (item.get("provenance") or {}).get("recovered")),
        "needs_operator_review_count": sum(1 for item in projection.get("items") or [] if (item.get("provenance") or {}).get("needs_operator_review") or "needs_operator_review" in (item.get("display_flags") or [])),
    }


def blocked_items_from_projection(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    projection = read_hypothesis_queue_projection(store_root=store_root)
    if projection is None:
        return [{"title": "Research queue projection needs rebuild/inspection", "item_type": "Projection", "what_is_blocked": PROJECTION_TYPE, "why_blocked": ["projection_not_built"], "missing": ["hypothesis_queue/latest.json"], "next_action": "Rebuild research projections"}]
    blocked = []
    for item in projection.get("items") or []:
        readiness = item.get("readiness") or {}
        if item.get("blocking_items") or readiness.get("ready_for_research") is False or "blocked" in (item.get("display_flags") or []):
            blocked.append({
                "title": item.get("title"),
                "item_type": item.get("item_type"),
                "what_is_blocked": item.get("item_id"),
                "why_blocked": item.get("blocking_items") or item.get("display_flags") or [item.get("status")],
                "missing": readiness.get("missing_symbols") or [],
                "next_action": item.get("recommended_next_action") or "Review projection item",
                "event_family_id": (item.get("provenance") or {}).get("event_family_id"),
            })
    return blocked


def _ui_row(item: dict[str, Any]) -> dict[str, Any]:
    readiness = item.get("readiness") or {}
    provenance = item.get("provenance") or {}
    dataset_candidates = readiness.get("dataset_snapshot_candidates") or []
    universe_candidates = readiness.get("universe_snapshot_candidates") or []
    default_dataset = dataset_candidates[0] if dataset_candidates else {}
    default_universe = universe_candidates[0] if universe_candidates else {}
    lifecycle_state = _lifecycle_state_for_item(item)
    operator_action_required = _operator_action_required(item)
    required_symbols = readiness.get("required_symbols") or []
    search_terms = [
        item.get("item_id"),
        item.get("title"),
        item.get("summary"),
        item.get("lane"),
        item.get("status"),
        lifecycle_state,
        item.get("recommended_next_action"),
        provenance.get("event_family_id"),
        provenance.get("governance_classification"),
        readiness.get("operator_data_status"),
        readiness.get("data_requirement_status"),
        readiness.get("required_data_mode"),
        item.get("priority_bucket"),
        *required_symbols,
        *(readiness.get("missing_symbols") or []),
        *(item.get("blocking_items") or []),
    ]
    search_text = " ".join(str(term) for term in search_terms if term).lower()
    return {
        "hypothesis_proposal_id": item.get("item_id"),
        "item_id": item.get("item_id"),
        "event_family_id": provenance.get("event_family_id"),
        "event_family": provenance.get("event_family_id"),
        "title": item.get("title"),
        "hypothesis_summary": item.get("summary"),
        "proposal_status": item.get("status"),
        "lane": item.get("lane"),
        "lifecycle_state": lifecycle_state,
        "created_at": item.get("created_at") or "",
        "updated_at": item.get("updated_at") or item.get("created_at") or "",
        "governance_classification": provenance.get("governance_classification", "data_required"),
        "data_requirement_status": readiness.get("data_requirement_status", "unknown"),
        "operator_data_status": readiness.get("operator_data_status") or _operator_data_status(readiness),
        "required_data_mode": readiness.get("required_data_mode") or "INTRADAY_OPERATIONAL",
        "intraday_market_data_available": bool(readiness.get("intraday_market_data_available")),
        "final_eod_required": bool(readiness.get("final_eod_required")),
        "final_eod_certification_status": readiness.get("final_eod_certification_status") or "",
        "priority_bucket": item.get("priority_bucket"),
        "priority": item.get("priority_bucket"),
        "tier": _tier_for_item(item),
        "ready_for_research": bool(readiness.get("ready_for_research")) if readiness else False,
        "operator_action_required": operator_action_required,
        "operator_attention_required": operator_action_required,
        "recent": bool(item.get("updated_at") or item.get("created_at")),
        "recently_updated": bool(item.get("updated_at")),
        "recommended_next_action": item.get("recommended_next_action"),
        "confidence_level": provenance.get("confidence_level", "unknown"),
        "latest_review_decision": provenance.get("latest_review_decision", ""),
        "next_allowed_actions": item.get("next_allowed_actions") or [],
        "blocking_items": item.get("blocking_items") or [],
        "blocker_reason": _blocker_reason_for_item(item),
        "required_symbols": required_symbols,
        "missing_symbols": readiness.get("missing_symbols") or [],
        "search_text": search_text,
        "research_label_present": bool(provenance.get("research_label_present")),
        "display_flags": item.get("display_flags") or [],
        "accept_for_research_enabled": bool(readiness.get("ready_for_research")),
        "dataset_snapshot_candidates": dataset_candidates,
        "universe_snapshot_candidates": universe_candidates,
        "default_dataset_snapshot_id": default_dataset.get("dataset_snapshot_id", ""),
        "default_universe_snapshot_id": default_universe.get("universe_snapshot_id", ""),
        "convert_start": default_dataset.get("start_date", ""),
        "convert_end": default_dataset.get("end_date", ""),
        "convert_to_research_plan_enabled": bool(
            item.get("status") == "accepted_for_research"
            and default_dataset.get("dataset_snapshot_id")
            and default_universe.get("universe_snapshot_id")
            and default_dataset.get("start_date")
            and default_dataset.get("end_date")
        ),
        "recovered": bool(provenance.get("recovered")),
        "needs_operator_review": bool(provenance.get("needs_operator_review")) or "needs_operator_review" in (item.get("display_flags") or []),
        "recovery_source_path": (provenance.get("recovery_metadata") or {}).get("recovery_source_path", ""),
    }



def _is_archived_row(row: dict[str, Any]) -> bool:
    return str(row.get("lifecycle_state") or row.get("lane") or row.get("proposal_status") or "").strip().lower() == "archived"


def _lifecycle_state_for_item(item: dict[str, Any]) -> str:
    lane = str(item.get("lane") or "").strip().lower()
    status = str(item.get("status") or "").strip().lower()
    flags = {str(flag).strip().lower() for flag in item.get("display_flags") or []}
    if lane == "archived" or status == "archived":
        return "Archived"
    if lane == "rejected" or status == "rejected":
        return "Archived"
    if lane == "needs_data" or "blocked" in flags:
        return "Blocked"
    if lane == "accepted_for_research" or status == "accepted_for_research":
        return "Researching"
    if lane == "watchlist":
        return "Ideas"
    if lane == "needs_review":
        return "Ideas"
    return "Ideas"


def _tier_for_item(item: dict[str, Any]) -> str:
    priority = str(item.get("priority_bucket") or "").strip().lower()
    if priority in {"critical", "high", "tier_1", "t1"}:
        return "High"
    if priority in {"medium", "tier_2", "t2"}:
        return "Medium"
    if priority in {"low", "tier_3", "t3"}:
        return "Low"
    if priority == "blocked":
        return "Blocked"
    return "Watchlist"


def _operator_action_required(item: dict[str, Any]) -> bool:
    flags = {str(flag).strip().lower() for flag in item.get("display_flags") or []}
    blockers = {str(blocker).strip().lower() for blocker in item.get("blocking_items") or []}
    next_action = str(item.get("recommended_next_action") or "").strip().lower()
    if "needs_operator_review" in flags or "needs_review" in flags:
        return True
    if any(token in blockers for token in {"missing_macro_event_calendar", "missing_external_dataset", "external_dataset_required"}):
        return True
    return next_action.startswith("upload_") or "operator" in next_action or "review" in next_action


def _blocker_reason_for_item(item: dict[str, Any]) -> str:
    blockers = [str(blocker) for blocker in item.get("blocking_items") or [] if blocker]
    if blockers:
        return ", ".join(blockers)
    readiness = item.get("readiness") or {}
    if readiness.get("ready_for_research") is True:
        return ""
    status = readiness.get("operator_data_status") or readiness.get("data_requirement_status") or ""
    return str(status)


def _operator_data_status(readiness: dict[str, Any]) -> str:
    blockers = {str(item) for item in readiness.get("blocking_items") or []}
    if "missing_macro_event_calendar" in blockers:
        return "Waiting for external macro dataset upload."
    if "missing_breadth_snapshot" in blockers:
        return "Waiting for breadth dataset build."
    if "stale_intraday_market_data" in blockers:
        return "Current intraday market data is stale."
    if "missing_required_symbols" in blockers:
        if readiness.get("final_eod_required") is True:
            return "Final EOD certification pending."
        return "Waiting for current intraday market data."
    if readiness.get("ready_for_research") is True:
        return "Research can proceed."
    return "Readiness check pending."


def _next_action_for_blockers(blocking_items: list[str]) -> str:
    blockers = {str(item) for item in blocking_items}
    if "missing_macro_event_calendar" in blockers:
        return "upload_external_dataset"
    if "missing_breadth_snapshot" in blockers:
        return "build_breadth_snapshot"
    if "missing_required_symbols" in blockers or "stale_intraday_market_data" in blockers:
        return "refresh_intraday_market_data"
    return "resolve_research_blocker"

def _load_source_registries(store: Path, warnings: list[str], errors: list[str], *, strict: bool) -> dict[str, dict[str, Any]]:
    out = {}
    for name, filename in SOURCE_REGISTRIES.items():
        path = store / "registries" / filename
        rows_by_id: dict[str, dict[str, Any]] = {}
        row_count = 0
        exists = path.exists()
        if not exists:
            msg = f"source_registry_missing:{filename}"
            if name == "hypothesis_proposals" or strict:
                errors.append(msg)
        else:
            try:
                rows = read_jsonl(path)
            except Exception as exc:  # noqa: BLE001
                msg = f"source_registry_unreadable:{filename}:{exc}"
                if name == "hypothesis_proposals" or strict:
                    errors.append(msg)
                else:
                    warnings.append(msg)
                rows = []
            row_count = len(rows)
            for row in rows:
                proposal_id = str(row.get("hypothesis_proposal_id") or "").strip()
                if proposal_id:
                    rows_by_id[proposal_id] = row
        out[name] = {"path": path, "exists": exists, "row_count": row_count, "rows": rows_by_id}
    return out


def _load_artifacts(directory: Path, id_field: str, warnings: list[str]) -> dict[str, dict[str, Any]]:
    artifacts: dict[str, dict[str, Any]] = {}
    if not directory.exists():
        return artifacts
    for path in sorted(directory.glob("*.json")):
        payload, error = safe_json_load(path)
        artifact_id = path.stem
        if error or not isinstance(payload, dict):
            warnings.append(f"artifact_parse_error:{path.name}:{error or 'not_object'}")
            artifacts[artifact_id] = {"_parse_error": error or "not_object", "_path": str(path), id_field: artifact_id}
            continue
        payload["_path"] = str(path)
        artifacts[str(payload.get(id_field) or artifact_id)] = payload
    return artifacts


def _latest_by_proposal_from_registry_and_artifacts(store: Path, registry_name: str, id_field: str, artifact_rel: str, warnings: list[str]) -> dict[str, dict[str, Any]]:
    rows = []
    registry_path = store / "registries" / f"{registry_name}.jsonl"
    try:
        rows = read_jsonl(registry_path)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"related_registry_unreadable:{registry_name}:{exc}")
    artifacts = _load_artifacts(store / artifact_rel, id_field, warnings)
    by_proposal: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        proposal_id = str(row.get("hypothesis_proposal_id") or "").strip()
        if not proposal_id:
            continue
        artifact_id = str(row.get(id_field) or "").strip()
        artifact = artifacts.get(artifact_id)
        payload = dict(row)
        if artifact is not None:
            if artifact.get("_parse_error"):
                warnings.append(f"related_artifact_parse_error:{artifact_rel}/{artifact_id}.json")
                payload["_parse_error"] = artifact.get("_parse_error")
            else:
                payload = artifact
        elif artifact_id:
            warnings.append(f"related_artifact_missing:{artifact_rel}/{artifact_id}.json")
            payload["_missing_artifact"] = True
        by_proposal.setdefault(proposal_id, []).append(payload)
    for artifact_id, artifact in artifacts.items():
        proposal_id = str(artifact.get("hypothesis_proposal_id") or "").strip()
        if proposal_id and not any(str(row.get(id_field) or "") == artifact_id for row in rows):
            warning = f"related_artifact_without_registry:{artifact_rel}/{artifact_id}.json"
            warnings.append(warning)
            clone = dict(artifact)
            clone["_artifact_without_registry"] = True
            by_proposal.setdefault(proposal_id, []).append(clone)
    return {proposal_id: sorted(items, key=_time_key)[-1] for proposal_id, items in by_proposal.items() if items}


def _queue_item_for_proposal(proposal_id: str, registry_row: dict[str, Any] | None, artifact: dict[str, Any] | None, related: dict[str, dict[str, dict[str, Any]]], warnings: list[str]) -> dict[str, Any]:
    flags: set[str] = set()
    source_refs = []
    if registry_row:
        source_refs.append({"kind": "registry", "name": "hypothesis_proposals", "id": proposal_id})
    else:
        flags.update({"needs_review", "integrity_warning"})
        warnings.append(f"proposal_artifact_without_registry:{proposal_id}")
    proposal = artifact or registry_row or {"hypothesis_proposal_id": proposal_id}
    if artifact is None:
        flags.update({"needs_review", "missing_artifact"})
        warnings.append(f"proposal_artifact_missing:{proposal_id}")
    elif artifact.get("_parse_error"):
        flags.update({"needs_review", "parse_error"})
    else:
        source_refs.append({"kind": "artifact", "name": "event_intake/hypothesis_proposals", "id": proposal_id, "path": artifact.get("_path", "")})
    readiness = related["readiness"].get(proposal_id) or {}
    priority = related["priority"].get(proposal_id) or {}
    review = related["reviews"].get(proposal_id) or {}
    dossier = related["dossiers"].get(proposal_id) or {}
    for name, payload in [("readiness", readiness), ("priority", priority), ("review", review), ("dossier", dossier)]:
        if payload.get("_parse_error") or payload.get("_missing_artifact") or payload.get("_artifact_without_registry"):
            flags.update({"needs_review", "integrity_warning"})
            if payload.get("_parse_error"):
                flags.add("parse_error")
            if payload.get("_missing_artifact"):
                flags.add("missing_artifact")
    recovery_metadata = proposal.get("recovery_metadata") or {}
    if recovery_metadata:
        flags.add("recovered")
    if recovery_metadata.get("needs_operator_review"):
        flags.add("needs_operator_review")
    blocking_items = list(readiness.get("blocking_items") or [])
    if blocking_items:
        flags.add("blocked")
    lane = _derive_lane(proposal, readiness, review, flags)
    if lane == "needs_review":
        flags.add("needs_review")
    status = _effective_status(proposal, review, lane)
    return {
        "item_id": proposal_id,
        "item_type": "Hypothesis Proposal",
        "title": str(proposal.get("title") or registry_row and registry_row.get("title") or f"Hypothesis proposal {proposal_id}"),
        "summary": str(proposal.get("hypothesis") or proposal.get("title") or dossier.get("hypothesis") or "Hypothesis proposal needs review."),
        "lane": lane,
        "status": status,
        "priority_bucket": str(priority.get("priority_bucket") or ("blocked" if blocking_items else "watchlist")),
        "readiness": {
            "ready_for_research": bool(readiness.get("ready_for_research")) if readiness else False,
            "data_requirement_status": readiness.get("data_requirement_status") or proposal.get("data_requirement_status") or "unknown",
            "missing_symbols": readiness.get("missing_symbols") or [],
            "required_symbols": readiness.get("required_symbols") or list(proposal.get("proposed_universe") or []),
            "operator_data_status": readiness.get("operator_data_status") or _operator_data_status(readiness),
            "required_data_mode": readiness.get("required_data_mode") or "INTRADAY_OPERATIONAL",
            "intraday_market_data_available": bool(readiness.get("intraday_market_data_available")),
            "final_eod_required": bool(readiness.get("final_eod_required")),
            "final_eod_certification_status": readiness.get("final_eod_certification_status") or "",
        },
        "blocking_items": blocking_items,
        "recommended_next_action": str(priority.get("recommended_next_action") or (_next_action_for_blockers(blocking_items) if blocking_items else "review_dossier" if lane == "needs_review" else "watchlist")),
        "next_allowed_actions": readiness.get("next_allowed_actions") or (["inspect_source_artifacts", "rebuild_projection"] if lane == "needs_review" else ["assess_readiness", "open_dossier", "review"]),
        "source_refs": source_refs,
        "provenance": {
            "event_family_id": proposal.get("event_family_id") or (registry_row or {}).get("event_family_id") or "unknown",
            "intent_candidate_id": proposal.get("intent_candidate_id") or (registry_row or {}).get("intent_candidate_id") or "",
            "event_cluster_id": proposal.get("event_cluster_id") or (registry_row or {}).get("event_cluster_id") or "",
            "governance_classification": proposal.get("governance_classification") or (registry_row or {}).get("governance_classification") or "data_required",
            "latest_review_decision": review.get("review_decision") or review.get("decision") or "",
            "confidence_level": dossier.get("confidence_level") or "unknown",
            "research_label_present": proposal.get("research_label") == "RESEARCH_ONLY" or True,
            "source_content_hash": proposal.get("content_hash") or (registry_row or {}).get("content_hash") or "",
            "recovery_metadata": recovery_metadata,
            "recovered": bool(recovery_metadata),
            "needs_operator_review": bool(recovery_metadata.get("needs_operator_review")),
        },
        "created_at": str(proposal.get("created_at") or (registry_row or {}).get("created_at") or ""),
        "updated_at": str(review.get("reviewed_at") or readiness.get("assessed_at") or priority.get("scored_at") or dossier.get("created_at") or proposal.get("created_at") or ""),
        "display_flags": sorted(flags),
        "schema_version": QUEUE_ITEM_SCHEMA_VERSION,
    }


def _derive_lane(proposal: dict[str, Any], readiness: dict[str, Any], review: dict[str, Any], flags: set[str]) -> str:
    if flags & {"needs_review", "missing_artifact", "parse_error"}:
        return "needs_review"
    latest_status = str(review.get("next_status") or review.get("review_decision") or review.get("decision") or "").strip().lower()
    if latest_status in {"rejected", "reject"}:
        return "rejected"
    if latest_status in {"archived", "archive"}:
        return "archived"
    if readiness.get("blocking_items"):
        return "needs_data"
    if latest_status in {"accepted_for_research", "accept_for_research"}:
        return "accepted_for_research"
    if latest_status in {"watchlist", "needs_data", "needs_revision"}:
        return "needs_data" if latest_status == "needs_data" else latest_status
    proposal_status = str(proposal.get("proposal_status") or "proposed").strip().lower()
    if proposal_status in HYPOTHESIS_LANES:
        return proposal_status
    return "needs_review"


def _effective_status(proposal: dict[str, Any], review: dict[str, Any], lane: str) -> str:
    if review.get("next_status"):
        return str(review["next_status"])
    if review.get("review_decision"):
        decision = str(review["review_decision"])
        return {"accept_for_research": "accepted_for_research", "reject": "rejected", "archive": "archived"}.get(decision, decision)
    return str(proposal.get("proposal_status") or lane)


def _status_to_lane(status: str) -> str:
    text = str(status or "").strip().lower()
    return {"accept_for_research": "accepted_for_research", "accepted": "accepted_for_research", "needs_review": "needs_review"}.get(text, text)


def _time_key(payload: dict[str, Any]) -> str:
    for key in ["reviewed_at", "assessed_at", "scored_at", "created_at"]:
        if payload.get(key):
            return str(payload[key])
    return ""


def _source_summary(store: Path, registry_data: dict[str, dict[str, Any]], proposal_artifacts: dict[str, dict[str, Any]]) -> dict[str, Any]:
    artifact_counts = {
        "event_intake_proposal_artifacts": len(proposal_artifacts),
        "readiness_assessment_artifacts": len(list((store / "research_intake" / "readiness_assessments").glob("*.json"))) if (store / "research_intake" / "readiness_assessments").exists() else 0,
        "priority_score_artifacts": len(list((store / "research_intake" / "priority_scores").glob("*.json"))) if (store / "research_intake" / "priority_scores").exists() else 0,
        "dossier_artifacts": len(list((store / "research_intake" / "dossiers").glob("*.json"))) if (store / "research_intake" / "dossiers").exists() else 0,
        "review_artifacts": len(list((store / "research_intake" / "reviews").glob("*.json"))) if (store / "research_intake" / "reviews").exists() else 0,
    }
    return {
        "source_registries_checked": True,
        "hypothesis_proposal_registry_exists": bool(registry_data["hypothesis_proposals"]["exists"]),
        "hypothesis_proposal_registry_row_count": int(registry_data["hypothesis_proposals"]["row_count"]),
        "event_intake_proposal_artifact_count": len(proposal_artifacts),
        "registry_counts": {name: int(data["row_count"]) for name, data in registry_data.items()},
        "artifact_counts": artifact_counts,
        "source_hashes": {name: _file_hash(data["path"]) for name, data in registry_data.items()},
    }


def _file_hash(path: Path) -> str:
    if not path.exists():
        return ""
    return content_hash({"path": str(path), "content": path.read_text(encoding="utf-8")})


def _operator_message(integrity_status: str, items: list[dict[str, Any]], trusted_empty: bool) -> str:
    if trusted_empty:
        return "No hypotheses are queued; projection build confirmed proposal registry and proposal artifacts are empty."
    if integrity_status == "source_missing":
        return "Source registry missing. Rebuild or inspect Research Store sources."
    if integrity_status == "warning":
        return "Queue integrity warning: some source artifacts could not be hydrated. They are shown under Needs Review."
    if integrity_status == "error":
        return "Queue integrity error: inspect projection validation before trusting this queue."
    return f"Hypothesis queue projection built successfully with {len(items)} item(s)."


def _projection_markdown(projection: dict[str, Any], build: dict[str, Any]) -> str:
    lane_counts = {lane: len(projection["lanes"].get(lane) or []) for lane in HYPOTHESIS_LANES}
    return "\n".join([
        "# Hypothesis Queue Projection",
        "",
        f"Build: {build['projection_build_id']}",
        f"Built at: {projection['built_at']}",
        f"Integrity: {projection['integrity_status']}",
        f"Items: {len(projection['items'])}",
        f"Lane counts: {json.dumps(lane_counts, sort_keys=True)}",
        "",
        projection["operator_message"],
        "",
    ])
