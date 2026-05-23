from __future__ import annotations

from research_lab.projections.projection_contracts import validate_projection_build, validate_queue_item, validate_queue_projection


def test_projection_contracts_validate() -> None:
    item = {
        "item_id": "ehp_test",
        "item_type": "Hypothesis Proposal",
        "title": "Test",
        "summary": "Summary",
        "lane": "proposed",
        "status": "proposed",
        "priority_bucket": "watchlist",
        "readiness": {},
        "blocking_items": [],
        "recommended_next_action": "review",
        "next_allowed_actions": ["open_dossier"],
        "source_refs": [],
        "provenance": {},
        "created_at": "2026-05-20T00:00:00Z",
        "updated_at": "2026-05-20T00:00:00Z",
        "display_flags": [],
    }
    validate_queue_item(item)
    projection = {
        "projection_id": "hqp_test",
        "projection_type": "hypothesis_queue",
        "built_at": "2026-05-20T00:00:00Z",
        "source_summary": {},
        "integrity_status": "ok",
        "integrity_warnings": [],
        "integrity_errors": [],
        "lanes": {"proposed": [item]},
        "items": [item],
        "empty_state": {"is_empty": False, "trusted_empty": False},
        "operator_message": "ok",
        "research_label": "research only",
        "schema_version": "queue_projection.v1",
        "content_hash": "abc",
    }
    validate_queue_projection(projection)
    validate_projection_build({
        "projection_build_id": "pb_test",
        "projection_type": "hypothesis_queue",
        "built_at": "2026-05-20T00:00:00Z",
        "built_by": "test",
        "source_registries": {},
        "source_artifact_counts": {},
        "source_hashes": {},
        "output_uri": "research://projections/hypothesis_queue/latest.json",
        "output_hash": "abc",
        "status": "success",
        "warnings": [],
        "errors": [],
        "schema_version": "projection_build.v1",
    })
