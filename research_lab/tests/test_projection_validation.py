from __future__ import annotations

from pathlib import Path

import pytest

from research_lab.projections.hypothesis_queue_projection import build_hypothesis_queue_projection
from research_lab.projections.projection_validator import validate_hypothesis_queue_projection
from research_lab.storage.manifest_io import append_jsonl, read_json, write_json
from research_lab.storage.paths import ensure_store_layout


def test_empty_queue_allowed_only_when_sources_empty(tmp_path: Path) -> None:
    store = ensure_store_layout(tmp_path)
    (store / "registries" / "hypothesis_proposals.jsonl").write_text("", encoding="utf-8")
    projection = build_hypothesis_queue_projection(store_root=store)["projection"]
    assert projection["empty_state"]["trusted_empty"] is True
    assert projection["integrity_status"] == "ok"


def test_false_empty_queue_triggers_integrity_error(tmp_path: Path) -> None:
    store = ensure_store_layout(tmp_path)
    proposal = {"hypothesis_proposal_id": "ehp_test", "title": "Test", "hypothesis": "Test", "proposal_status": "proposed", "event_family_id": "gap_event", "created_at": "2026-05-20T00:00:00Z", "content_hash": "h"}
    write_json(store / "event_intake" / "hypothesis_proposals" / "ehp_test.json", proposal, overwrite=True)
    append_jsonl(store / "registries" / "hypothesis_proposals.jsonl", {"hypothesis_proposal_id": "ehp_test", "proposal_status": "proposed", "created_at": "2026-05-20T00:00:00Z"})
    build_hypothesis_queue_projection(store_root=store)
    latest_path = store / "projections" / "hypothesis_queue" / "latest.json"
    projection = read_json(latest_path)
    projection["items"] = []
    projection["lanes"] = {key: [] for key in projection["lanes"]}
    projection["empty_state"] = {"is_empty": True, "trusted_empty": False}
    write_json(latest_path, projection, overwrite=True)
    result = validate_hypothesis_queue_projection(store_root=store)
    assert result["ok"] is False
    assert "source_proposals_missing_from_projection:ehp_test" in result["errors"]
    with pytest.raises(RuntimeError):
        validate_hypothesis_queue_projection(store_root=store, strict=True)
