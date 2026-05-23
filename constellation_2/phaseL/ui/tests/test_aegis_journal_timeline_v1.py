from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.journal.journal_event_v1 import build_journal_timeline_v1, write_journal_timeline_v1
from ops.aegis.research_lab.research_pipeline_v1 import build_research_pipeline_v1, write_research_pipeline_v1


DAY = "2026-05-17"


def _write_hypothesis(root: Path, hypothesis_id: str, *, status: str = "IDEA", title: str | None = None) -> None:
    path = root.parent / "research_lab" / "research_hypothesis_v1" / "2026-05-15" / hypothesis_id / "research_hypothesis.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "hypothesis_id": hypothesis_id,
                "title": title or f"{hypothesis_id} title",
                "status": status,
                "source": "CHATGPT_SEED",
                "edge_family": "MEAN_REVERSION",
                "created_at_utc": "2026-05-15T00:00:00Z",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _seed_journal_sources(truth: Path) -> None:
    _write_hypothesis(truth, "rh-edge-2026-0101", title="Volatility overshoot mean reversion")
    _write_hypothesis(truth, "rh-edge-2026-0103", title="Volatility compression next-session edge")
    pipeline = build_research_pipeline_v1(truth_root=truth, day_utc=DAY)
    write_research_pipeline_v1(truth_root=truth, day_utc=DAY, payload=pipeline)
    runtime_dir = truth / "reports" / "aegis_runtime_truth_kernel_v1" / DAY
    write_json_v1(
        runtime_dir / "runtime_truth_kernel.v1.json",
        {
            "generated_at": "2026-05-17T12:00:00Z",
            "day_utc": DAY,
            "runtime_truth_classification": "PARTIAL_CONTEXT",
            "highest_readiness_layer": "ADVISORY_ONLY",
            "missing_or_stale_source_count": 1,
            "blocked_capabilities": ["TRADE_ADVICE_ALLOWED"],
        },
    )
    write_json_v1(
        truth / "reports" / "aegis_canonical_operator_state_v1" / DAY / "canonical_operator_state.v1.json",
        {"generated_at_utc": "2026-05-17T12:01:00Z", "day_utc": DAY},
    )
    audit_path = truth / "reports" / "aegis_audit_handoff_v1" / DAY / "aegis_audit_handoff.txt"
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text("audit handoff", encoding="utf-8")


def test_journal_timeline_report_is_generated_with_required_events(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _seed_journal_sources(truth)

    payload = build_journal_timeline_v1(truth_root=truth, day_utc=DAY)
    paths = write_journal_timeline_v1(truth_root=truth, day_utc=DAY, payload=payload)
    events = payload["recent_events"]
    event_ids = {event["entity_id"] for event in events}
    event_types = {event["event_type"] for event in events}

    assert Path(paths["json"]).exists()
    assert Path(paths["summary"]).exists()
    assert Path(paths["matrix"]).exists()
    assert "rh-edge-2026-0101" in event_ids
    assert "rh-edge-2026-0103" in event_ids
    assert "runtime.truth_generated" in event_types
    assert "canonical_state.generated" in event_types
    assert "audit_handoff.generated" in event_types
    assert payload["timeline_summary"]["edge_events"] >= 2
    assert payload["timeline_summary"]["runtime_events"] >= 1


def test_journal_timeline_events_are_audit_ready_and_exclude_fixtures(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _seed_journal_sources(truth)
    pipeline = build_research_pipeline_v1(truth_root=truth, day_utc=DAY)
    pipeline["items"].append(
        {
            "hypothesis_id": "fixture_idea_v1",
            "title": "Fixture idea",
            "classification": "TEST_FIXTURE",
            "current_gate": "REJECTED_ARCHIVED",
            "gate_status": "ARCHIVED",
            "source_artifacts": ["fixture"],
        }
    )
    pipeline["pipeline"]["rejected_archived"].append(pipeline["items"][-1])
    write_research_pipeline_v1(truth_root=truth, day_utc=DAY, payload=pipeline)

    payload = build_journal_timeline_v1(truth_root=truth, day_utc=DAY)
    events = payload["recent_events"]

    assert "fixture_idea_v1" not in {event["entity_id"] for event in events}
    assert any(row.get("hypothesis_id") == "fixture_idea_v1" for row in payload["diagnostics"])
    for event in events:
        assert event["event_id"]
        assert event["event_type"]
        assert event["entity_id"]
        assert event["source_artifact_path"]
        assert event["source_hash"]
        assert event["broker_execution_allowed"] is False
        assert event["autonomous_execution_allowed"] is False
