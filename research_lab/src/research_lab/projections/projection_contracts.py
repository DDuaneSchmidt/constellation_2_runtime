from __future__ import annotations

from typing import Any

PROJECTION_BUILD_SCHEMA_VERSION = "projection_build.v1"
QUEUE_PROJECTION_SCHEMA_VERSION = "queue_projection.v1"
QUEUE_ITEM_SCHEMA_VERSION = "queue_item.v1"

BUILD_STATUSES = {"success", "success_with_warnings", "failed"}
INTEGRITY_STATUSES = {"ok", "warning", "error", "source_missing", "stale"}
DISPLAY_FLAGS = {"needs_review", "blocked", "stale", "missing_artifact", "parse_error", "integrity_warning", "recovered", "needs_operator_review"}
HYPOTHESIS_LANES = ["proposed", "watchlist", "needs_data", "accepted_for_research", "rejected", "archived", "needs_review"]

RESEARCH_LABEL = "Research queue projection only. Derived from Research Store canonical artifacts. No broker execution. No live trading. No autonomous trading. No sleeve creation. No investment recommendation."


def validate_projection_build(payload: dict[str, Any]) -> None:
    required = [
        "projection_build_id",
        "projection_type",
        "built_at",
        "built_by",
        "source_registries",
        "source_artifact_counts",
        "source_hashes",
        "output_uri",
        "output_hash",
        "status",
        "warnings",
        "errors",
        "schema_version",
    ]
    _require(payload, required, "ProjectionBuild")
    if payload["status"] not in BUILD_STATUSES:
        raise ValueError(f"invalid projection build status: {payload['status']}")


def validate_queue_item(payload: dict[str, Any]) -> None:
    required = [
        "item_id",
        "item_type",
        "title",
        "summary",
        "lane",
        "status",
        "priority_bucket",
        "readiness",
        "blocking_items",
        "recommended_next_action",
        "next_allowed_actions",
        "source_refs",
        "provenance",
        "created_at",
        "updated_at",
        "display_flags",
    ]
    _require(payload, required, "QueueItem")
    unknown = set(payload.get("display_flags") or []) - DISPLAY_FLAGS
    if unknown:
        raise ValueError(f"invalid display flags: {sorted(unknown)}")


def validate_queue_projection(payload: dict[str, Any]) -> None:
    required = [
        "projection_id",
        "projection_type",
        "built_at",
        "source_summary",
        "integrity_status",
        "integrity_warnings",
        "integrity_errors",
        "lanes",
        "items",
        "empty_state",
        "operator_message",
        "research_label",
        "schema_version",
        "content_hash",
    ]
    _require(payload, required, "QueueProjection")
    if payload["integrity_status"] not in INTEGRITY_STATUSES:
        raise ValueError(f"invalid integrity status: {payload['integrity_status']}")
    for item in payload.get("items") or []:
        validate_queue_item(item)


def _require(payload: dict[str, Any], fields: list[str], name: str) -> None:
    missing = [field for field in fields if field not in payload]
    if missing:
        raise ValueError(f"{name} missing fields: {missing}")
