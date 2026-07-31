from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from ops.aegis.research_lab.research_console_v1 import build_hypothesis_view_model_v1, research_console_v1, start_research_v1
from ops.aegis.research_lab.research_run_ledger_v1 import (
    append_research_run_v1,
    read_research_run_ledger_v1,
    research_run_projection_for_hypothesis_v1,
)
from research_lab.storage.manifest_io import append_jsonl


def test_legacy_active_hypothesis_without_run_ledger_does_not_show_researching() -> None:
    projection = research_run_projection_for_hypothesis_v1(
        {"hypothesis_id": "hyp_legacy", "lifecycle_state": "Researching", "source_type": "Governed Research"},
        [],
    )

    assert projection["user_facing_status"] == "Monitoring"
    assert "not started AI research" in projection["user_facing_explanation"]
    assert projection["has_active_run"] is False


def test_system_monitor_does_not_show_researching_without_running_run() -> None:
    with TemporaryDirectory() as tmp:
        store = Path(tmp)
        append_research_run_v1(
            hypothesis_id="hyp_system",
            trigger_source="SYSTEM_MONITOR",
            trigger_reason="Daily system refresh.",
            run_status="SUCCEEDED",
            store_root=store,
        )
        runs = read_research_run_ledger_v1(store_root=store)
        projection = research_run_projection_for_hypothesis_v1({"hypothesis_id": "hyp_system"}, runs)

        assert projection["user_facing_status"] == "Monitoring"


def test_running_system_or_user_run_is_actual_researching() -> None:
    with TemporaryDirectory() as tmp:
        store = Path(tmp)
        append_research_run_v1(
            hypothesis_id="hyp_running",
            trigger_source="USER_INITIATED",
            trigger_reason="User started research.",
            run_status="RUNNING",
            started_at="2026-05-22T14:15:00Z",
            store_root=store,
        )
        projection = research_run_projection_for_hypothesis_v1(
            {"hypothesis_id": "hyp_running"},
            read_research_run_ledger_v1(store_root=store),
        )

        assert projection["user_facing_status"] == "Researching"
        assert projection["started_by_user"] is True
        assert "Started by you" in projection["user_facing_explanation"]


def test_queued_scheduled_run_shows_scheduled() -> None:
    run = {
        "research_run_id": "rrun_scheduled",
        "hypothesis_id": "hyp_scheduled",
        "trigger_source": "SCHEDULED_RUN",
        "trigger_reason": "Overnight queue.",
        "run_status": "QUEUED",
        "requested_at": "2026-05-22T21:59:00Z",
        "queued_at": "2026-05-22T22:00:00Z",
    }
    projection = research_run_projection_for_hypothesis_v1({"hypothesis_id": "hyp_scheduled"}, [run])

    assert projection["user_facing_status"] == "Scheduled"
    assert projection["primary_action_label"] == "View Queue"


def test_completed_run_with_findings_shows_complete() -> None:
    run = {
        "research_run_id": "rrun_complete",
        "hypothesis_id": "hyp_complete",
        "trigger_source": "USER_INITIATED",
        "trigger_reason": "Research completed.",
        "run_status": "SUCCEEDED",
        "requested_at": "2026-05-22T20:00:00Z",
        "completed_at": "2026-05-22T20:30:00Z",
        "output_artifact_ids": ["research_result_hyp_complete"],
    }
    projection = research_run_projection_for_hypothesis_v1({"hypothesis_id": "hyp_complete"}, [run])

    assert projection["user_facing_status"] == "Complete"
    assert projection["primary_action_label"] == "View Findings"


def test_manual_ib_output_shows_recommendation_ready() -> None:
    run = {
        "research_run_id": "rrun_recommendation",
        "hypothesis_id": "hyp_recommendation",
        "trigger_source": "USER_INITIATED",
        "trigger_reason": "Research completed with recommendation.",
        "run_status": "SUCCEEDED",
        "requested_at": "2026-05-22T20:00:00Z",
        "completed_at": "2026-05-22T20:30:00Z",
        "output_artifact_ids": ["manual_ib_capture_recommendation_hyp_recommendation"],
    }
    projection = research_run_projection_for_hypothesis_v1({"hypothesis_id": "hyp_recommendation"}, [run])

    assert projection["user_facing_status"] == "Recommendation Ready"
    assert projection["primary_action_label"] == "View Recommendation"


def test_blocked_hypothesis_shows_blocked_without_active_run() -> None:
    projection = research_run_projection_for_hypothesis_v1(
        {"hypothesis_id": "hyp_blocked", "blocker_summary": "Missing earnings calendar", "next_action": "Provide event dates."},
        [],
    )

    assert projection["user_facing_status"] == "Blocked"
    assert projection["primary_action_label"] == "View Blocker"


def test_start_research_creates_user_initiated_run_ledger_event() -> None:
    with TemporaryDirectory() as tmp:
        store = Path(tmp)
        append_jsonl(store / "registries" / "cost_model_snapshots.jsonl", {"cost_model_snapshot_id": "cm_default", "schema_version": "cost_model_snapshot.v1", "content_hash": "abc123abc123"})
        result = start_research_v1({"idea": "Index gap reversals", "symbols": "SPY QQQ", "priority": "watchlist"}, store_root=store)
        ledger = read_research_run_ledger_v1(store_root=store)

        assert result["research_run_id"]
        assert result["research_run_trigger_source"] == "USER_INITIATED"
        assert result["research_run_status"] == "QUEUED"
        assert any(run["research_run_id"] == result["research_run_id"] for run in ledger)
        console = research_console_v1(store_root=store)
        row = next(item for item in console["all_hypotheses"] if item["hypothesis_id"] == result["hypothesis_proposal_id"])
        assert row["research_run_state"]["user_facing_status"] == "Queued"
        assert row["trigger_source"] == "USER_INITIATED"


def test_projection_is_deterministic_from_ledger() -> None:
    runs = [
        {
            "research_run_id": "rrun_1",
            "hypothesis_id": "hyp_deterministic",
            "trigger_source": "USER_INITIATED",
            "run_status": "QUEUED",
            "requested_at": "2026-05-22T10:00:00Z",
            "queued_at": "2026-05-22T10:00:00Z",
        },
        {
            "research_run_id": "rrun_2",
            "hypothesis_id": "hyp_deterministic",
            "trigger_source": "USER_INITIATED",
            "run_status": "RUNNING",
            "requested_at": "2026-05-22T10:01:00Z",
            "started_at": "2026-05-22T10:02:00Z",
        },
    ]

    first = research_run_projection_for_hypothesis_v1({"hypothesis_id": "hyp_deterministic"}, list(reversed(runs)))
    second = research_run_projection_for_hypothesis_v1({"hypothesis_id": "hyp_deterministic"}, runs)

    assert first["user_facing_status"] == second["user_facing_status"] == "Researching"
    assert first["active_research_run_id"] == second["active_research_run_id"] == "rrun_2"
    assert first["content_hash"] == second["content_hash"]



def test_hypothesis_view_model_summary_top_item_matches_first_visible_item() -> None:
    rows = [
        {"hypothesis_id": "hyp_b", "title": "Breadth collapse recovery", "user_facing_status": "Ready to Start", "rank": 2, "tier": "High", "symbols": ["SPY"]},
        {"hypothesis_id": "hyp_nvda", "title": "NVIDIA Earnings Event Dislocation", "user_facing_status": "Monitoring", "rank": 1, "tier": "TIER_1_ACTIVE", "symbols": ["NVDA"]},
        {"hypothesis_id": "hyp_blocked", "title": "Blocked credit stress", "user_facing_status": "Blocked", "rank": 3, "tier": "High", "blocker_summary": "Missing data"},
    ]

    model = build_hypothesis_view_model_v1(rows)
    ready = model["sections"]["ready_to_start"]
    ready_summary = next(card for card in model["summary_cards"] if card["section_id"] == "ready_to_start")

    assert ready["top_item"] == ready["visible_items"][0]
    assert ready_summary["top_item"] == ready["visible_items"][0]
    assert ready["visible_items"][0]["title"] == "NVIDIA Earnings Event Dislocation"
    assert ready["visible_items"][0]["user_facing_status"] == "Ready to Start"
    assert model["primary_cta"]["label"] == "Start NVIDIA Earnings Event Dislocation"
    assert model["raw_hypothesis_count"] == 3
    assert model["rendered_hypothesis_count"] == 3
    assert model["unmapped_hypothesis_ids"] == []
    assert ready["count"] == len(ready["visible_items"])


def test_hypothesis_view_model_zero_count_sections_collapse() -> None:
    model = build_hypothesis_view_model_v1([
        {"hypothesis_id": "hyp_ready", "title": "Ready idea", "user_facing_status": "Ready to Start", "rank": 1, "symbols": ["SPY"]},
    ])

    assert model["section_order"].index("ready_to_start") < model["section_order"].index("researching")
    assert model["sections"]["researching"]["count"] == 0
    assert model["sections"]["researching"]["collapsed_by_default"] is True
    assert model["sections"]["blocked"]["collapsed_by_default"] is True


def test_live_nvidia_ready_summary_matches_first_ready_card_when_present() -> None:
    payload = research_console_v1()
    model = payload["hypothesis_view_model_v1"]
    ready = model["sections"]["ready_to_start"]
    nvidia_titles = [item["title"] for item in ready["visible_items"] if "NVIDIA Earnings Event Dislocation" in item["title"]]
    if nvidia_titles:
        assert ready["top_item"]["title"] == "NVIDIA Earnings Event Dislocation"
        assert ready["visible_items"][0]["title"] == "NVIDIA Earnings Event Dislocation"
        assert model["primary_cta"]["label"] == "Start NVIDIA Earnings Event Dislocation"
    assert model["raw_hypothesis_count"] == model["rendered_hypothesis_count"]
    assert model["unmapped_hypothesis_ids"] == []
    assert all(section["count"] == len(section["visible_items"]) for section in model["sections"].values())



def test_start_existing_hypothesis_creates_user_initiated_run_without_new_proposal_chain() -> None:
    with TemporaryDirectory() as tmp:
        store = Path(tmp)
        result = start_research_v1({"hypothesis_id": "rh-nvidia-earnings-event-dislocation-v1", "idea": "NVIDIA Earnings Event Dislocation"}, store_root=store)
        ledger = read_research_run_ledger_v1(store_root=store)

        assert result["ok"] is True
        assert result["message"] == "Research started for NVIDIA Earnings Event Dislocation."
        assert result["operator_message"] == "Research started for NVIDIA Earnings Event Dislocation."
        assert result["hypothesis_proposal_id"] == "rh-nvidia-earnings-event-dislocation-v1"
        assert result["research_run_trigger_source"] == "USER_INITIATED"
        assert result["research_run_status"] == "QUEUED"
        projection = research_run_projection_for_hypothesis_v1({"hypothesis_id": "rh-nvidia-earnings-event-dislocation-v1"}, ledger)
        assert projection["user_facing_status"] == "Queued"
        assert len(ledger) == 1
        assert ledger[0]["hypothesis_id"] == "rh-nvidia-earnings-event-dislocation-v1"
        assert ledger[0]["input_artifact_ids"] == ["rh-nvidia-earnings-event-dislocation-v1"]



def test_every_raw_hypothesis_maps_to_exactly_one_visible_card() -> None:
    rows = [
        {"hypothesis_id": "hyp_ready", "title": "Ready idea", "user_facing_status": "Ready to Start"},
        {"hypothesis_id": "hyp_ambiguous", "title": "Ambiguous legacy idea", "user_facing_status": ""},
        {"hypothesis_id": "hyp_blocked", "title": "Blocked idea", "user_facing_status": "Blocked", "blocker_summary": "Missing data"},
        {"hypothesis_id": "hyp_complete", "title": "Complete idea", "user_facing_status": "Complete", "output_artifact_ids": ["result"]},
    ]
    model = build_hypothesis_view_model_v1(rows)
    rendered_ids = [
        item["hypothesis_id"]
        for section in model["sections"].values()
        for item in section["visible_items"]
    ]

    assert model["raw_hypothesis_count"] == len(rows)
    assert model["rendered_hypothesis_count"] == len(rendered_ids) == len(rows)
    assert sorted(rendered_ids) == sorted(row["hypothesis_id"] for row in rows)
    assert model["unmapped_hypothesis_ids"] == []
    assert all(section["count"] == len(section["visible_items"]) for section in model["sections"].values())
    assert any(item["hypothesis_id"] == "hyp_ambiguous" for item in model["sections"]["ready_to_start"]["visible_items"])


def test_pipeline_result_review_without_run_ledger_shows_recommendation_ready() -> None:
    projection = research_run_projection_for_hypothesis_v1(
        {
            "hypothesis_id": "hyp_result_review",
            "current_gate": "RESULT_REVIEW",
            "current_status": "NEEDS_OPERATOR",
            "latest_result_summary": "Evidence-backed findings are ready.",
        },
        [],
    )

    assert projection["user_facing_status"] == "Recommendation Ready"
    assert projection["primary_action_label"] == "View Recommendation"
    assert "operator review" in projection["user_facing_explanation"]


def test_hypothesis_view_items_preserve_command_contracts() -> None:
    rows = [
        {
            "hypothesis_id": "hyp_ready",
            "title": "Ready idea",
            "user_facing_status": "Ready to Start",
            "primary_command": {"command_id": "START_RESEARCH", "label": "Start Research", "enabled": True},
            "secondary_commands": [{"command_id": "VIEW_BLOCKER", "label": "View Blocker"}],
        }
    ]

    model = build_hypothesis_view_model_v1(rows)
    item = model["sections"]["ready_to_start"]["visible_items"][0]

    assert item["primary_command"]["command_id"] == "START_RESEARCH"
    assert item["primary_command"]["enabled"] is True
    assert item["secondary_commands"][0]["command_id"] == "VIEW_BLOCKER"
