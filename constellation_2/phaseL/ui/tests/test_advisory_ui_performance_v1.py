from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from constellation_2.phaseL.ui_api import advisory_read_model


def test_advisory_summary_omits_evidence_refs(monkeypatch) -> None:
    ref = SimpleNamespace(
        path=Path("/tmp/advisory_decision_state.v1.json"),
        payload={
            "decision_id": "decision-1",
            "advisory_item_id": "item-1",
            "advisory_surface_label": "Surface",
            "decision_state": "allowed",
            "actionability_state": "actionable",
            "freshness_state": "fresh",
            "visibility_state": "visible",
            "promotion_eligibility_state": "eligible",
            "primary_explanation": {"short_message": "ok"},
            "generated_at_utc": "2026-04-29T14:30:00Z",
            "evidence_refs": [
                {"artifact_path": "/tmp/evidence.json", "artifact_id": "evidence"},
            ],
        },
    )
    monkeypatch.setattr(advisory_read_model, "resolve_ui_day", lambda _day=None: "2026-04-29")
    monkeypatch.setattr(advisory_read_model, "list_advisory_decision_states_v1", lambda **_kwargs: [ref])

    summary = advisory_read_model.build_advisory_view("2026-04-29", include_evidence=False)
    full = advisory_read_model.build_advisory_view("2026-04-29", include_evidence=True)

    assert summary["source_refs"] == []
    assert summary["decisions"][0]["evidence_refs"] == []
    assert full["source_refs"]
    assert full["decisions"][0]["evidence_refs"]
