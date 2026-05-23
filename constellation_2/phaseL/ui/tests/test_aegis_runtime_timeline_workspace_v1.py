from __future__ import annotations

import json
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import pytest
from constellation_2.phaseL.ui.tests.operator_shell_test_sources import pages_source_v1

from ops.tools.research_hypotheses_clickthrough_qa_v1 import CdpClient, wait_for_target

from ops.aegis.operator_state.runtime_timeline_projection_v1 import build_runtime_timeline_projection_v1
from ops.aegis.operator_state.schedule_event_ledger_v1 import build_schedule_event_ledger_v1


ROOT = Path(__file__).resolve().parents[4]


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return path


def _write_calendar(root: Path, day: str, *, open_session: bool = True) -> None:
    path = root / "market_calendar_v1" / "NYSE" / f"{day[:4]}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "dataset_version": "v1",
                "day_utc": day,
                "exchange": "NYSE",
                "is_trading_session": open_session,
                "source_name": "test_calendar",
                "source_hash": "a" * 64,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _base_artifacts(root: Path, day: str) -> dict:
    _write_calendar(root, day)
    _write_json(
        root / "reports" / "market_data_intraday_operational_v1" / day / "market_data_intraday_operational.v1.json",
        {
            "schema_id": "aegis_market_data",
            "day_utc": day,
            "generated_at_utc": f"{day}T14:50:00Z",
            "status": "CURRENT",
            "market_data_mode": "INTRADAY_OPERATIONAL",
            "final_eod_certification_status": "PENDING",
        },
    )
    _write_json(
        root / "reports" / "market_data_readiness_v1" / day / "market_data_readiness.v1.json",
        {"schema_id": "market_data_readiness", "generated_at_utc": f"{day}T14:51:00Z", "status": "VALID"},
    )
    _write_json(
        root / "reports" / "candidate_generation_manifest_v1" / day / "run-1" / "candidate_generation_manifest.v1.json",
        {"schema_id": "candidate_generation_manifest", "generated_at_utc": f"{day}T14:52:00Z", "status": "READY"},
    )
    _write_json(
        root / "reports" / "portfolio_scoring_v1" / day / "portfolio_scoring.v1.json",
        {"schema_id": "portfolio_scoring", "generated_at_utc": f"{day}T14:53:00Z", "status": "PASS"},
    )
    _write_json(
        root / "reports" / "intent_arbitration_v1" / day / "intent_arbitration.v1.json",
        {"schema_id": "intent_arbitration", "generated_at_utc": f"{day}T14:54:00Z", "status": "SELECTED"},
    )
    _write_json(
        root / "reports" / "aegis_selected_intent_promotion_v1" / day / "selected_intent_promotion.v1.json",
        {"schema_id": "selected_intent_promotion", "generated_at_utc": f"{day}T14:55:00Z", "status": "PROMOTED_TO_OPERATOR_REVIEW"},
    )
    _write_json(
        root / "reports" / "candidate_promotion_map_v1" / day / "candidate_promotion_map.v1.json",
        {"schema_id": "candidate_promotion_map", "generated_at_utc": f"{day}T14:56:00Z", "selected_candidate_count": 1, "blocked_selected_count": 1},
    )
    return {
        "generated_at_utc": f"{day}T14:57:00Z",
        "operator_today_projection": {
            "intraday_operational_ready": True,
            "final_eod_certification_pending": True,
            "final_eod_certification_status": "PENDING",
            "blocked_selected_candidate_count": 1,
            "capture_ready_ticket_count": 0,
            "market_data_last_updated_at": f"{day}T14:50:00Z",
            "candidate_snapshot_generated_at": f"{day}T14:52:00Z",
        },
        "current_day_status": {
            "intraday_operational_ready": True,
            "final_eod_certification_pending": True,
            "final_eod_certification_status": "PENDING",
            "retry_action_available": True,
            "retry_job": {"job_id": "market_data_refresh", "status": "QUEUED", "attempt_count": 2, "next_retry_utc": f"{day}T15:05:00Z"},
        },
    }


def test_runtime_timeline_models_early_close_vendor_lag_retry_and_deadlock(tmp_path: Path) -> None:
    day = "2026-11-27"
    snapshot = _base_artifacts(tmp_path, day)
    payload = build_runtime_timeline_projection_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot=snapshot,
        now_utc=f"{day}T21:30:00Z",
        repo_root=ROOT,
    )

    assert payload["market_session_timeline"]["early_close"] is True
    assert payload["market_session_timeline"]["vendor_lag_window"].endswith("minutes")
    assert payload["retry_visibility"]["retry_queue_depth"] == 1
    assert payload["retry_visibility"]["active_retries"][0]["retry_count"] == 2
    assert payload["current_pipeline_state"]["blocked_stage"] == "manual_capture_ready"
    assert any(row["alert_type"] == "pipeline_stage_blocked" for row in payload["alerts"])
    assert "Data as of" not in json.dumps(payload)
    assert payload["runtime_clock_semantics"]["market_data_timestamp"] == f"{day}T14:50:00Z"


def test_runtime_timeline_handles_holiday_and_certification_complete(tmp_path: Path) -> None:
    day = "2026-05-25"
    _write_calendar(tmp_path, day, open_session=False)
    snapshot = {
        "generated_at_utc": f"{day}T12:00:00Z",
        "operator_today_projection": {
            "intraday_operational_ready": False,
            "final_eod_certification_pending": False,
            "final_eod_certification_status": "VALID",
            "capture_ready_ticket_count": 0,
            "blocked_selected_candidate_count": 0,
        },
        "current_day_status": {},
    }
    payload = build_runtime_timeline_projection_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot=snapshot,
        now_utc=f"{day}T16:00:00Z",
        repo_root=ROOT,
    )

    assert payload["market_session_timeline"]["current_operational_phase"] == "MARKET_CLOSED_NON_TRADING_DAY"
    assert payload["market_session_timeline"]["market_open_utc"] == ""
    assert payload["certification_timing"]["certified_complete"] is True


def _runtime_workspace_block() -> str:
    pages = pages_source_v1(ROOT)
    return pages[pages.index("function runtimeParseMs"):pages.index("function renderAegisCandidatesWorkflow")]



def test_next_scheduled_events_are_chronological_and_separate_from_pipeline(tmp_path: Path) -> None:
    day = "2026-05-21"
    snapshot = _base_artifacts(tmp_path, day)
    payload = build_runtime_timeline_projection_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot=snapshot,
        now_utc=f"{day}T21:30:00Z",
        repo_root=ROOT,
    )

    events = payload["next_scheduled_events"]
    times = [row["scheduled_at_utc"] for row in events]
    assert times == sorted(times)
    assert all(row["scheduled_at_utc"] > f"{day}T21:30:00Z" for row in events)
    assert all(row["event_category"] == "OPERATOR_WORKFLOW_EVENT" for row in payload["next_scheduled_events"])
    assert all(row["visibility_surface"] == "PRIMARY_OPERATOR_TIMELINE" for row in payload["next_scheduled_events"])
    assert payload["next_event"]["operator_status"] in {"Missed", "Waiting on data", "Running", "Scheduled"}
    assert payload["next_event"].get("event_group") == "UPCOMING"
    assert payload["next_event"].get("event_key") == "final_eod_certification"
    assert payload["timeline_semantics"]["status_authority"] == "schedule_event_ledger_v1"
    assert payload["timeline_semantics"]["event_grouping"] == "operator_meaning"
    assert payload["timeline_semantics"]["next_scheduled_events_order"] == "schedule_event_ledger_chronological_datetime_ascending_future_only"
    assert payload["timeline_semantics"]["pipeline_stages_order"] == "logical_workflow_order"
    assert [row["stage_id"] for row in payload["current_pipeline_state"]["stages"]][:4] == [
        "market_data",
        "candidate_generation",
        "qualification",
        "scoring",
    ]


def test_runtime_timeline_groups_past_waiting_on_data_under_needs_attention(tmp_path: Path) -> None:
    day = "2026-05-21"
    snapshot = _base_artifacts(tmp_path, day)
    payload = build_runtime_timeline_projection_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot=snapshot,
        now_utc=f"{day}T22:45:00Z",
        repo_root=ROOT,
    )

    attention_keys = {row["event_key"] for row in payload["needs_attention_events"]}
    upcoming_keys = {row["event_key"] for row in payload["upcoming_events"]}
    assert "final_eod_certification" in attention_keys
    assert "final_eod_certification" not in upcoming_keys
    assert all(row["scheduled_at_utc"] > f"{day}T21:30:00Z" for row in payload["upcoming_events"])


def test_runtime_timeline_escalation_within_grace_is_normal_wait(tmp_path: Path) -> None:
    day = "2026-05-21"
    snapshot = _base_artifacts(tmp_path, day)
    payload = build_runtime_timeline_projection_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot=snapshot,
        now_utc=f"{day}T22:15:00Z",
        repo_root=ROOT,
    )

    row = next(row for row in payload["needs_attention_events"] if row["event_key"] == "final_eod_certification")
    assert row["escalation_state"] == "NORMAL_WAIT"
    assert row["operator_next_action"] == "No action required"


def test_runtime_timeline_escalation_past_grace_is_overdue(tmp_path: Path) -> None:
    day = "2026-05-21"
    snapshot = _base_artifacts(tmp_path, day)
    payload = build_runtime_timeline_projection_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot=snapshot,
        now_utc=f"{day}T22:45:00Z",
        repo_root=ROOT,
    )

    row = next(row for row in payload["needs_attention_events"] if row["event_key"] == "final_eod_certification")
    assert row["escalation_state"] == "OVERDUE"
    assert row["operator_next_action"] == "Diagnostics recommended"
    assert row["minutes_past_grace"] > 0


def test_runtime_timeline_escalation_retry_exhaustion_is_escalated(tmp_path: Path) -> None:
    day = "2026-05-21"
    snapshot = _base_artifacts(tmp_path, day)
    snapshot["current_day_status"]["active_jobs"] = [{"job_id": "final_eod_vendor_data", "status": "WAITING_ON_DATA", "reason": "VENDOR_DELAY"}]
    payload = build_runtime_timeline_projection_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot=snapshot,
        now_utc=f"{day}T22:15:00Z",
        repo_root=ROOT,
    )

    row = next(row for row in payload["needs_attention_events"] if row["event_key"] == "final_eod_certification")
    assert row["escalation_state"] == "ESCALATED"
    assert row["retry_exhausted"] is True
    assert row["operator_next_action"] == "Operator review recommended"


def test_runtime_timeline_escalation_terminal_failure_is_failed(tmp_path: Path) -> None:
    day = "2026-05-21"
    snapshot = _base_artifacts(tmp_path, day)
    _write_json(
        tmp_path / "reports" / "market_data_final_eod_v1" / day / "market_data_final_eod.v1.json",
        {"schema_id": "market_data_final_eod_v1", "generated_at_utc": f"{day}T22:05:00Z", "status": "FAILED"},
    )
    payload = build_runtime_timeline_projection_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot=snapshot,
        now_utc=f"{day}T22:10:00Z",
        repo_root=ROOT,
    )

    row = next(row for row in payload["needs_attention_events"] if row["event_key"] == "final_eod_certification")
    assert row["escalation_state"] == "FAILED"
    assert row["operator_next_action"] == "Repair required"


def test_runtime_timeline_groups_past_missed_under_needs_attention(tmp_path: Path) -> None:
    day = "2026-05-21"
    _write_calendar(tmp_path, day)
    payload = build_runtime_timeline_projection_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot={"current_day_status": {}},
        now_utc=f"{day}T22:45:00Z",
        repo_root=ROOT,
    )

    assert any(row["operator_status"] == "Missed" for row in payload["needs_attention_events"])
    assert not any(row["operator_status"] == "Missed" for row in payload["upcoming_events"])


def test_runtime_timeline_groups_completed_past_under_completed_today(tmp_path: Path) -> None:
    day = "2026-05-21"
    snapshot = _base_artifacts(tmp_path, day)
    payload = build_runtime_timeline_projection_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot=snapshot,
        now_utc=f"{day}T15:10:00Z",
        repo_root=ROOT,
    )

    assert any(row["operator_status"] in {"Completed", "Superseded"} for row in payload["completed_today_events"])
    assert not any(row["operator_status"] == "Completed" for row in payload["upcoming_events"])


def test_runtime_timeline_hero_uses_future_event_when_no_attention_items(tmp_path: Path) -> None:
    day = "2026-05-21"
    snapshot = _base_artifacts(tmp_path, day)
    snapshot["operator_today_projection"]["final_eod_certification_pending"] = False
    snapshot["current_day_status"] = {"final_eod_certification_pending": False}
    payload = build_runtime_timeline_projection_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot=snapshot,
        now_utc=f"{day}T14:00:00Z",
        repo_root=ROOT,
    )

    assert payload["needs_attention_events"] == []
    assert payload["upcoming_events"]
    assert payload["next_event"].get("event_group") == "UPCOMING"
    assert payload["next_event"]["scheduled_at_utc"] == payload["upcoming_events"][0]["scheduled_at_utc"]


def test_future_scheduled_events_are_not_marked_completed(tmp_path: Path) -> None:
    day = "2026-05-21"
    snapshot = _base_artifacts(tmp_path, day)
    payload = build_runtime_timeline_projection_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot=snapshot,
        now_utc=f"{day}T21:30:00Z",
        repo_root=ROOT,
    )

    assert payload["next_scheduled_events"]
    future_rows = [row for row in payload["next_scheduled_events"] if row["scheduled_at_utc"] > f"{day}T21:30:00Z"]
    assert future_rows
    assert all(row["operator_status"] == "Scheduled" for row in future_rows)
    overnight_research = [row for row in future_rows if row["event_key"] == "overnight_research"]
    assert overnight_research
    assert overnight_research[0]["run_status"] == "PLANNED"


def test_runtime_timeline_no_past_event_renders_scheduled_or_upcoming(tmp_path: Path) -> None:
    day = "2026-05-21"
    snapshot = _base_artifacts(tmp_path, day)
    payload = build_runtime_timeline_projection_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot=snapshot,
        now_utc=f"{day}T21:30:00Z",
        repo_root=ROOT,
    )

    for row in payload["schedule_event_ledger_events"]:
        if row["scheduled_at_utc"] <= f"{day}T21:30:00Z":
            assert row["operator_status"] != "Scheduled"
    assert all(row["scheduled_at_utc"] > f"{day}T21:30:00Z" for row in payload["upcoming_events"])


def test_historical_replay_mode_labels_past_schedule_explicitly(tmp_path: Path) -> None:
    day = "2026-05-21"
    snapshot = _base_artifacts(tmp_path, day)
    snapshot["runtime_mode"] = "HISTORICAL_FALLBACK"
    payload = build_runtime_timeline_projection_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot=snapshot,
        now_utc=f"2026-05-22T15:00:00Z",
        repo_root=ROOT,
    )

    assert payload["timeline_semantics"]["status_clock"] == "historical_replay_clock"
    assert payload["schedule_event_ledger"]["event_count"] > 0
    assert any(row["operator_status"] in {"Historical status unavailable", "Superseded", "Completed"} for row in payload["schedule_event_ledger_events"])


def test_schedule_event_ledger_marks_past_artifact_completed(tmp_path: Path) -> None:
    day = "2026-05-21"
    snapshot = _base_artifacts(tmp_path, day)
    ledger = build_schedule_event_ledger_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot=snapshot,
        now_utc=f"{day}T15:10:00Z",
        repo_root=ROOT,
        write_artifact=True,
    )

    candidate_events = [row for row in ledger["events"] if row["event_key"] == "morning_ai_run" and row["scheduled_at_utc"] <= f"{day}T15:10:00Z"]
    assert candidate_events
    assert any(row["operator_status"] in {"Completed", "Superseded"} for row in candidate_events)
    assert (tmp_path / "reports" / "schedule_event_ledger_v1" / day / "schedule_event_ledger.v1.json").exists()


def test_schedule_event_ledger_marks_past_without_artifact_missed(tmp_path: Path) -> None:
    day = "2026-05-21"
    _write_calendar(tmp_path, day)
    ledger = build_schedule_event_ledger_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot={"current_day_status": {}},
        now_utc=f"{day}T22:45:00Z",
        repo_root=ROOT,
        write_artifact=False,
    )

    final_event = next(row for row in ledger["events"] if row["event_key"] == "final_eod_certification")
    assert final_event["run_status"] == "MISSED"
    assert final_event["operator_status"] == "Missed"


def test_schedule_event_ledger_keeps_past_with_dependency_waiting_on_data(tmp_path: Path) -> None:
    day = "2026-05-21"
    _write_calendar(tmp_path, day)
    snapshot = {"current_day_status": {"final_eod_certification_pending": True, "retry_job": {"job_id": "market_data_refresh", "status": "QUEUED", "failure_reason": "PENDING_VENDOR_DATA"}}}
    ledger = build_schedule_event_ledger_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot=snapshot,
        now_utc=f"{day}T22:45:00Z",
        repo_root=ROOT,
        write_artifact=False,
    )

    final_event = next(row for row in ledger["events"] if row["event_key"] == "final_eod_certification")
    assert final_event["operator_status"] == "Waiting on data"
    assert final_event["status_reason"] == "ACTIVE_DEPENDENCY_RECORDED_AFTER_GRACE_WINDOW"


def test_schedule_event_ledger_hard_rule_no_past_waiting_without_dependency(tmp_path: Path) -> None:
    day = "2026-05-21"
    _write_calendar(tmp_path, day)
    ledger = build_schedule_event_ledger_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot={"current_day_status": {}},
        now_utc=f"{day}T23:00:00Z",
        repo_root=ROOT,
        write_artifact=False,
    )

    for row in ledger["events"]:
        if row["scheduled_at_utc"] < f"{day}T22:30:00Z":
            assert row["operator_status"] not in {"Scheduled", "Waiting"}
            if row["operator_status"] == "Waiting on data":
                assert ledger["active_dependencies"]


def test_schedule_event_ledger_hero_prefers_overdue_unresolved(tmp_path: Path) -> None:
    day = "2026-05-21"
    _write_calendar(tmp_path, day)
    ledger = build_schedule_event_ledger_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot={"current_day_status": {}},
        now_utc=f"{day}T22:45:00Z",
        repo_root=ROOT,
        write_artifact=False,
    )

    assert ledger["next_event"]["operator_status"] == "Missed"
    assert ledger["next_event"]["scheduled_at_utc"] < f"{day}T22:45:00Z"


def test_runtime_timeline_ui_has_no_primary_raw_event_bypass_paths() -> None:
    runtime_block = _runtime_workspace_block()
    primary_start = runtime_block.index("function runtimeBuildEventGroups")
    primary_end = runtime_block.index("function renderRuntimeDiagnostics")
    primary_block = runtime_block[primary_start:primary_end]

    assert "runtimeBuildFallbackOperatorSchedule" not in runtime_block
    assert "runtimeIsOperatorWorkflowRow" not in runtime_block
    assert "timeline.today_events" not in primary_block
    assert "timeline.tomorrow_events" not in primary_block
    assert "timeline.next_scheduled_events" not in primary_block
    assert "timeline.schedule_event_ledger_events" not in primary_block
    assert "visibility_surface" in runtime_block


def test_runtime_timeline_primary_excludes_internal_infrastructure_events(tmp_path: Path) -> None:
    day = "2026-05-21"
    snapshot = _base_artifacts(tmp_path, day)
    payload = build_runtime_timeline_projection_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot=snapshot,
        now_utc=f"{day}T12:00:00Z",
        repo_root=ROOT,
    )

    primary_rows = payload["needs_attention_events"] + payload["upcoming_events"] + payload["completed_today_events"]
    primary_keys = {row["event_key"] for row in primary_rows}
    assert primary_rows
    assert all(row["event_category"] == "OPERATOR_WORKFLOW_EVENT" for row in primary_rows)
    assert all(row["visibility_surface"] == "PRIMARY_OPERATOR_TIMELINE" for row in primary_rows)
    assert all(row.get("operator_relevance_reason") for row in primary_rows)
    assert "market_data_refresh" not in primary_keys
    assert "candidate_scheduler_timer" not in primary_keys
    assert {"morning_ai_run", "afternoon_ai_run", "final_eod_certification"}.issubset({row["event_key"] for row in payload["operator_workflow_events"]})


def test_runtime_timeline_diagnostics_include_refresh_cadence(tmp_path: Path) -> None:
    day = "2026-05-21"
    snapshot = _base_artifacts(tmp_path, day)
    payload = build_runtime_timeline_projection_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot=snapshot,
        now_utc=f"{day}T12:00:00Z",
        repo_root=ROOT,
    )

    internal_rows = payload["infrastructure_events"]
    internal_times = {row["scheduled_at_utc"] for row in internal_rows if row["event_key"] == "market_data_refresh"}
    assert internal_rows
    assert all(row["event_category"] == "INTERNAL_INFRASTRUCTURE_EVENT" for row in internal_rows)
    assert all(row["visibility_surface"] == "DIAGNOSTICS_ONLY" for row in internal_rows)
    assert all(row.get("operator_relevance_reason") for row in internal_rows)
    assert "2026-05-21T13:35:00Z" in internal_times
    assert "2026-05-21T15:55:00Z" in internal_times
    assert "2026-05-21T19:45:00Z" in internal_times
    assert "2026-05-21T20:10:00Z" in internal_times


def test_workflow_timeline_reflects_manifest_cadence_only(tmp_path: Path) -> None:
    day = "2026-05-21"
    snapshot = _base_artifacts(tmp_path, day)
    payload = build_runtime_timeline_projection_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot=snapshot,
        now_utc=f"{day}T12:00:00Z",
        repo_root=ROOT,
    )

    workflow_times = {row["event_key"]: row["scheduled_at_utc"] for row in payload["operator_workflow_events"]}
    assert workflow_times["morning_ai_run"] == "2026-05-21T13:50:00Z"
    assert workflow_times["afternoon_ai_run"] == "2026-05-21T18:50:00Z"
    assert workflow_times["final_eod_certification"] == "2026-05-21T22:00:00Z"
    primary_times = {row["scheduled_at_utc"] for row in payload["next_scheduled_events"]}
    assert "2026-05-21T13:35:00Z" not in primary_times
    assert "2026-05-21T15:55:00Z" not in primary_times
    assert "2026-05-21T19:45:00Z" not in primary_times
    assert "2026-05-21T20:10:00Z" not in primary_times


def test_schedule_changes_propagate_from_runtime_manifest(tmp_path: Path) -> None:
    day = "2026-05-21"
    repo = tmp_path / "repo"
    (repo / "ops/runtime").mkdir(parents=True)
    (repo / "ops/runtime/runtime_manifest.yaml").write_text(
        "schema_version: 1\n"
        "operator_workflow_events:\n"
        "  - event_key: morning_ai_run\n"
        "    event_name: Morning AI Run\n"
        "    event_type: CANDIDATE_GENERATION\n"
        "    event_category: OPERATOR_WORKFLOW_EVENT\n"
        "    local_time: \"10:05\"\n"
        "    timezone: America/New_York\n"
        "    day_offset: 0\n"
        "    trigger_type: governed-runtime-manifest\n"
        "    owning_job: candidate_generation\n"
        "    expected_artifact_type: candidate_generation_manifest_v1\n"
        "    expected_artifact_path_key: candidate\n"
        "    grace_window_minutes: 20\n"
        "    purpose: Test governed cadence.\n"
        "    expected_result: Test output.\n",
        encoding="utf-8",
    )
    snapshot = _base_artifacts(tmp_path, day)
    payload = build_runtime_timeline_projection_v1(
        truth_root=tmp_path,
        day_utc=day,
        operator_snapshot=snapshot,
        now_utc=f"{day}T12:00:00Z",
        repo_root=repo,
    )

    assert payload["operator_workflow_events"][0]["event_key"] == "morning_ai_run"
    assert payload["operator_workflow_events"][0]["scheduled_at_utc"] == "2026-05-21T14:05:00Z"

def test_runtime_timeline_workspace_is_primary_operator_route() -> None:
    nav = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/navigation_schema.js").read_text(encoding="utf-8")
    pages = pages_source_v1(ROOT)
    runtime_block = _runtime_workspace_block()

    assert "Runtime Timeline" in nav
    assert "/aegis-runtime-timeline" in nav
    assert "function renderAegisRuntimeTimelineWorkflow" in pages
    assert "runtime-mission-banner" in runtime_block
    assert "runtime-compact-schedule" in runtime_block
    assert "renderRuntimeNeedsAttention" in runtime_block
    assert "renderRuntimeUpcoming" in runtime_block
    assert "renderRuntimeCompletedToday" in runtime_block
    assert "renderRuntimeDomainCertificationGrid" in runtime_block
    assert "runtime-minimal-progress" in runtime_block
    assert "renderRuntimeHorizontalTimeline(timeline, schedule)" not in runtime_block
    assert 'contextHtml: ""' in runtime_block
    assert "hideContextRail: true" in runtime_block


def test_runtime_timeline_hero_is_compact_and_three_second_readable() -> None:
    runtime_block = _runtime_workspace_block()

    assert "NEXT" in runtime_block
    assert "Expected by" in runtime_block
    assert "Capture recommendations" in runtime_block
    assert "Expected after certification" in runtime_block
    assert "Operational day" in runtime_block
    assert "Last update" in runtime_block
    assert "Certification ETA" in runtime_block
    assert "runtime-clean-meta" in runtime_block
    assert "runtimeCountdown" in runtime_block
    assert "Diagnostics recommended" in runtime_block
    assert "next.operatorNextAction" in runtime_block


def test_runtime_timeline_uses_grouped_schedule_not_dense_table() -> None:
    runtime_block = _runtime_workspace_block()
    schedule_start = runtime_block.index("function renderRuntimeNeedsAttention")
    schedule_end = runtime_block.index("function runtimeProgressStepState")
    schedule_block = runtime_block[schedule_start:schedule_end]

    assert "TODAY" in runtime_block
    assert "TOMORROW" in runtime_block
    assert "runtime-schedule-group" in runtime_block
    assert "runtime-schedule-row" in runtime_block
    assert "Needs Attention" in runtime_block
    assert "Upcoming" in runtime_block
    assert "Completed Today" in runtime_block
    assert "Future scheduled events only" in runtime_block
    assert "Domain Certification" in runtime_block
    assert "Independent data domains certify separately" in runtime_block
    assert "renderSimpleTable" not in schedule_block
    assert "Expected Result" not in schedule_block


def test_runtime_timeline_links_to_repair_center_instead_of_hosting_workflow() -> None:
    runtime_block = _runtime_workspace_block()
    main_js = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")

    assert "renderRuntimeRepairCenterSummary" in runtime_block
    assert "data-runtime-repair-summary" in runtime_block
    assert "repair items open" in runtime_block
    assert "source setup required" in runtime_block
    assert "Open Repair Center" in runtime_block
    assert "OPEN_VALID_ROUTE" in runtime_block
    assert "/aegis-repair-center" in runtime_block
    assert "data-domain-repair-workflow" not in runtime_block
    assert "renderRuntimeDomainRepairPlan(repairPlans" not in runtime_block
    assert "executeAegisCommand" in main_js
    assert "Rechecking domain…" not in main_js


def test_runtime_timeline_has_minimal_progression_above_diagnostics() -> None:
    runtime_block = _runtime_workspace_block()
    css = (ROOT / "constellation_2/phaseL/ui/static/aegis.css").read_text(encoding="utf-8")

    assert "data-runtime-minimal-progression" in runtime_block
    for label in ["Market Close", "Preliminary AI", "Certification", "Capture Window", "Reconciliation"]:
        assert label in runtime_block
    for marker in ['"✓"', '"●"', '"○"']:
        assert marker in runtime_block
    assert ".runtime-minimal-progress" in css
    assert ".runtime-progress-dot" in css
    assert ".runtime-progress-line" in css


def test_runtime_timeline_primary_view_hides_infrastructure_diagnostics() -> None:
    runtime_block = _runtime_workspace_block()

    assert "Evidence Drawer" not in runtime_block
    assert "Market Session Timeline" not in runtime_block
    assert "Scheduled Jobs" not in runtime_block
    assert "Runtime Clock Semantics" not in runtime_block
    assert "Retry Visibility" not in runtime_block
    assert "Show Diagnostics" in runtime_block
    assert "Retry queue depth" in runtime_block
    assert "Scheduler drift and alerts" in runtime_block
    assert "Raw backoff" in runtime_block
    assert "Lineage metadata" in runtime_block
    assert "Schedule event ledger" in runtime_block
    assert "Expected artifacts" in runtime_block
    assert "Actual artifacts" in runtime_block
    assert "Escalation" in runtime_block
    assert "Provider/feed" in runtime_block
    assert "Retry exhaustion" in runtime_block
    assert "Dependency age" in runtime_block
    assert "SLA threshold" in runtime_block
    assert "Escalation threshold" in runtime_block
    assert "Missing artifacts" in runtime_block
    assert "Status reason" in runtime_block


def test_runtime_timeline_avoids_excessive_na_rendering() -> None:
    runtime_block = _runtime_workspace_block().lower()

    assert "not scheduled today" in runtime_block
    assert runtime_block.count("n/a") == 0


def test_runtime_timeline_responsive_layout_supports_1600_by_900() -> None:
    css = (ROOT / "constellation_2/phaseL/ui/static/aegis.css").read_text(encoding="utf-8")

    assert ".runtime-mission-banner" in css
    assert ".runtime-compact-schedule" in css
    assert ".runtime-minimal-progress" in css
    assert "width: min(1180px, 100%);" in css
    assert "grid-template-columns: minmax(0, 1.45fr) minmax(260px, 0.75fr);" in css
    assert "grid-template-columns: 92px minmax(0, 1fr);" in css
    assert ".runtime-diagnostics-compact" in css
    assert ".runtime-domain-command-results" in css
    assert "grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));" in css
    assert "min-width: 240px;" in css
    assert "overflow-wrap: anywhere" in css
    assert "@media (max-width: 900px)" in css


def test_operator_can_determine_next_expected_event_from_visible_text() -> None:
    runtime_block = _runtime_workspace_block()

    workflow_start = runtime_block.index("function renderAegisRuntimeTimelineWorkflow")
    workflow_block = runtime_block[workflow_start:]
    assert workflow_block.index("renderRuntimeNextEventHero") < workflow_block.index("renderRuntimeDomainCertificationGrid")
    assert workflow_block.index("renderRuntimeDomainCertificationGrid") < workflow_block.index("renderRuntimeNeedsAttention")
    assert workflow_block.index("renderRuntimeNeedsAttention") < workflow_block.index("renderRuntimeUpcoming")
    assert workflow_block.index("renderRuntimeUpcoming") < workflow_block.index("renderRuntimePipelineProgress")
    assert workflow_block.index("renderRuntimePipelineProgress") < workflow_block.index("renderRuntimeCompletedToday") < workflow_block.index("renderRuntimeDiagnostics")
    assert "scheduledTime" in runtime_block
    assert "runtimeFormatClock" in runtime_block
    assert "runtimeCountdown" in runtime_block
    assert "expectedOutcome" in runtime_block


def test_runtime_timeline_schedule_rules_match_operator_expectations() -> None:
    runtime_block = _runtime_workspace_block()

    assert runtime_block.index("runtimeScheduleSortTime") < runtime_block.index("function renderRuntimeCompactSchedule")
    assert "runtimeOperatorStateLabel" in runtime_block
    assert 'return "Waiting on vendor data"' in runtime_block
    assert '"Vendor data overdue"' in runtime_block
    assert '"Vendor dependency exceeded operational threshold"' in runtime_block
    assert 'return "Missed"' in runtime_block
    assert 'return "Scheduled"' in runtime_block
    assert "runtimeBuildEventGroups" in runtime_block


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_http_ok(url: str, timeout: float = 15.0) -> None:
    import urllib.request

    deadline = time.time() + timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except Exception as exc:
            last_error = exc
        time.sleep(0.25)
    raise RuntimeError(f"Server did not become ready: {last_error}")


def test_runtime_timeline_rendered_browser_visibility_contract() -> None:
    chromium = shutil.which("chromium") or shutil.which("chromium-browser")
    if not chromium:
        pytest.skip("Chromium is not installed")
    server_port = _free_port()
    cdp_port = _free_port()
    server = subprocess.Popen(
        [
            "python3",
            "-B",
            "-m",
            "constellation_2.phaseL.ui.server.run_ops_dashboard_v1",
            "--host",
            "127.0.0.1",
            "--port",
            str(server_port),
        ],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    profile = tempfile.TemporaryDirectory()
    browser = None
    cdp = None
    try:
        _wait_http_ok(f"http://127.0.0.1:{server_port}/healthz")
        browser = subprocess.Popen(
            [
                chromium,
                "--headless=new",
                "--disable-gpu",
                "--no-sandbox",
                "--disable-application-cache",
                "--disk-cache-size=0",
                "--media-cache-size=0",
                f"--remote-debugging-port={cdp_port}",
                f"--user-data-dir={profile.name}",
                "about:blank",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        cdp = CdpClient(wait_for_target(cdp_port, timeout=10))
        cdp.command("Runtime.enable")
        cdp.command("Page.enable")
        cdp.command("Network.enable")
        cdp.command("Network.setCacheDisabled", {"cacheDisabled": True})
        cdp.command("Emulation.setDeviceMetricsOverride", {"width": 1600, "height": 900, "deviceScaleFactor": 1, "mobile": False})
        cdp.command("Page.navigate", {"url": f"http://127.0.0.1:{server_port}/aegis-runtime-timeline?cache_bust=visibility_contract"})
        ready_expr = r'''
(() => {
  const body = document.body.innerText || '';
  return body.includes('9:50 AM') && body.includes('Morning AI Run') && body.includes('2:50 PM') && body.includes('Afternoon AI Run') && body.includes('6:00 PM');
})()
'''
        for _ in range(60):
            time.sleep(0.5)
            ready = cdp.command("Runtime.evaluate", {"expression": ready_expr, "returnByValue": True}, timeout=10).get("result", {}).get("value")
            if ready:
                break
        expr = r'''
(() => {
  const text = (selector) => document.querySelector(selector)?.innerText || '';
  const hero = text('[data-runtime-next-event]');
  const needs = text('[data-runtime-needs-attention]');
  const upcoming = text('[data-runtime-upcoming-events]');
  const completed = text('[data-runtime-completed-today]');
  const progress = text('[data-runtime-minimal-progression]');
  const primaryText = [hero, needs, upcoming, completed, progress].join('\n');
  const diagnostics = document.querySelector('[data-runtime-diagnostics]');
  const diagnosticsInitiallyOpen = Boolean(diagnostics?.open);
  if (diagnostics) diagnostics.open = true;
  const diagnosticsText = diagnostics?.innerText || '';
  const deprecated = /(9:35 AM|11:55 AM|3:45 PM|4:10 PM)/;
  return {
    path: window.location.pathname,
    viewport: { width: window.innerWidth, height: window.innerHeight },
    primaryHasMorning: primaryText.includes('9:50 AM') && primaryText.includes('Morning AI Run'),
    primaryHasAfternoon: primaryText.includes('2:50 PM') && primaryText.includes('Afternoon AI Run'),
    primaryHasFinal: primaryText.includes('6:00 PM') && primaryText.includes('Final Certification'),
    primaryHasDeprecated: deprecated.test(primaryText),
    diagnosticsHasDeprecated: deprecated.test(diagnosticsText),
    diagnosticsInitiallyOpen,
    heroHasDeprecated: deprecated.test(hero),
    needsHasDeprecated: deprecated.test(needs),
    completedHasDeprecated: deprecated.test(completed),
  };
})()
'''
        result = cdp.command("Runtime.evaluate", {"expression": expr, "returnByValue": True}, timeout=20).get("result", {}).get("value")
        assert result["path"] == "/aegis-runtime-timeline"
        assert result["viewport"] == {"width": 1600, "height": 900}
        assert result["primaryHasMorning"] is True
        assert result["primaryHasAfternoon"] is True
        assert result["primaryHasFinal"] is True
        assert result["primaryHasDeprecated"] is False
        assert result["heroHasDeprecated"] is False
        assert result["needsHasDeprecated"] is False
        assert result["completedHasDeprecated"] is False
        assert result["diagnosticsInitiallyOpen"] is False
        assert result["diagnosticsHasDeprecated"] is True

        repair_link_expr = r'''
(() => {
  const summary = document.querySelector('[data-runtime-repair-summary]');
  const button = summary?.querySelector('[data-aegis-command-id="OPEN_VALID_ROUTE"][data-route="/aegis-repair-center"]');
  return {
    summaryVisible: Boolean(summary),
    hasLink: Boolean(button),
    text: summary?.innerText || '',
  };
})()
'''
        repair_link = cdp.command("Runtime.evaluate", {"expression": repair_link_expr, "returnByValue": True}, timeout=20).get("result", {}).get("value")
        assert repair_link["summaryVisible"] is True
        assert repair_link["hasLink"] is True
        assert "repair items open" in repair_link["text"]

        cdp.command("Page.navigate", {"url": f"http://127.0.0.1:{server_port}/aegis-repair-center?cache_bust=repair_center"}, timeout=20)
        ready_repair_expr = "document.querySelector('[data-repair-center-workspace]')?.innerText || ''"
        for _ in range(40):
            time.sleep(0.25)
            repair_text = cdp.command("Runtime.evaluate", {"expression": ready_repair_expr, "returnByValue": True}, timeout=20).get("result", {}).get("value") or ""
            if "Repair Center" in repair_text and ("Source setup" in repair_text or "Automatic" in repair_text):
                break
        click_expr = r'''
(() => {
  const card = [...document.querySelectorAll('[data-repair-card]')].find((node) => (node.innerText || '').includes('MACRO_CALENDAR') || (node.innerText || '').includes('Macro')) || document.querySelector('[data-repair-card]');
  const button = card?.querySelector('.primary-button[data-aegis-command-id]');
  if (!button) return { clicked: false, cardText: card?.innerText || '' };
  button.scrollIntoView({ block: 'center' });
  button.click();
  return { clicked: true, command: button.getAttribute('data-aegis-command-id'), target: button.getAttribute('data-aegis-command-target-id'), cardText: card.innerText.slice(0, 400) };
})()
'''
        click_result = cdp.command("Runtime.evaluate", {"expression": click_expr, "returnByValue": True}, timeout=20).get("result", {}).get("value")
        assert click_result["clicked"] is True
        layout_result = {}
        layout_expr = r'''
(() => {
  const text = document.body.innerText || '';
  const cards = [...document.querySelectorAll('[data-repair-card]')];
  const targetCard = cards.find((node) => (node.innerText || '').includes('MACRO_CALENDAR') || (node.innerText || '').includes('Macro')) || cards[0];
  const panel = targetCard?.querySelector('[data-command-result-panel]');
  const rects = cards.map((card) => card.getBoundingClientRect());
  let overlaps = false;
  for (let i = 0; i < rects.length; i += 1) {
    for (let j = i + 1; j < rects.length; j += 1) {
      const horizontal = Math.max(0, Math.min(rects[i].right, rects[j].right) - Math.max(rects[i].left, rects[j].left));
      const vertical = Math.max(0, Math.min(rects[i].bottom, rects[j].bottom) - Math.max(rects[i].top, rects[j].top));
      if (horizontal * vertical > 2) overlaps = true;
    }
  }
  return {
    path: window.location.pathname,
    no404: !/Error response\s+Error code:\s*404|ENDPOINT_NOT_FOUND|File not found/i.test(text),
    cardCount: cards.length,
    resultVisible: Boolean(panel),
    resultText: panel?.innerText || '',
    sourceSetupVisible: text.includes('Source setup required') || text.includes('Upload/configure source'),
    copyPathVisible: text.includes('Copy required path'),
    domainGridCardPanelCount: document.querySelectorAll('.runtime-domain-card [data-command-result-panel]').length,
    cardsReadable: rects.every((rect) => rect.width >= 320 && rect.height >= 120),
    noOverlaps: !overlaps,
    noVerticalText: rects.every((rect) => rect.width >= 320),
  };
})()
'''
        for _ in range(30):
            time.sleep(0.25)
            layout_result = cdp.command("Runtime.evaluate", {"expression": layout_expr, "returnByValue": True}, timeout=20).get("result", {}).get("value") or {}
            if layout_result.get("resultVisible"):
                break
        assert layout_result["path"] == "/aegis-repair-center"
        assert layout_result["no404"] is True
        assert layout_result["cardCount"] >= 1
        assert layout_result["resultVisible"] is True
        assert "NEXT STEP" in layout_result["resultText"]
        assert "MACRO" in (layout_result["resultText"] + click_result["cardText"]).upper()
        assert layout_result["sourceSetupVisible"] is True
        assert layout_result["copyPathVisible"] is True
        assert layout_result["domainGridCardPanelCount"] == 0
        assert layout_result["cardsReadable"] is True
        assert layout_result["noOverlaps"] is True
        assert layout_result["noVerticalText"] is True

        cdp.command("Page.navigate", {"url": f"http://127.0.0.1:{server_port}/aegis-repair-center?cache_bust=after_repair"}, timeout=20)
        refresh_result = {}
        for _ in range(40):
            time.sleep(0.25)
            refresh_result = cdp.command("Runtime.evaluate", {"expression": layout_expr, "returnByValue": True}, timeout=20).get("result", {}).get("value") or {}
            if refresh_result.get("cardCount", 0) >= 1 and refresh_result.get("sourceSetupVisible"):
                break
        assert refresh_result["path"] == "/aegis-repair-center"
        assert refresh_result["sourceSetupVisible"] is True
        assert refresh_result["copyPathVisible"] is True
        assert refresh_result["domainGridCardPanelCount"] == 0
        assert refresh_result["cardsReadable"] is True
        assert refresh_result["noOverlaps"] is True
    finally:
        if cdp is not None:
            cdp.close()
        if browser is not None:
            browser.terminate()
            try:
                browser.wait(timeout=5)
            except subprocess.TimeoutExpired:
                browser.kill()
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
        profile.cleanup()
