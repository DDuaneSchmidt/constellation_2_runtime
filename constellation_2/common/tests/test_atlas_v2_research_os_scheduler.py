from pathlib import Path

from constellation_2.common.atlas_v2_research_os import scheduler
from constellation_2.common.atlas_v2_research_os.scheduler import run_event_scheduler_once, run_hourly_scheduler_once, run_scheduler_trigger
from constellation_2.common.atlas_v2_research_os.scheduler_models import SchedulerTriggerStatus, SchedulerTriggerType
from constellation_2.common.atlas_v2_research_os.scheduler_reports import build_scheduler_report, read_scheduler_executions, read_scheduler_triggers


def _fake_execution(status="COMPLETED"):
    return {
        "execution_id": "bounded-exec-1",
        "status": status,
        "governance_result": {"status": "PASS"},
        "lineage_result": {"status": "PASS"},
        "certification_result": {"status": "PASS"},
        "warnings": [],
    }


def test_hourly_trigger_invokes_bounded_research_once(monkeypatch, tmp_path: Path):
    calls = []
    monkeypatch.setattr(scheduler, "run_bounded_research_once", lambda root, day=None: calls.append((root, day)) or _fake_execution())
    result = run_hourly_scheduler_once(root=tmp_path, source={"created_at": "2026-06-05T10:15:00Z"}, day="2026-06-05")
    assert result["status"] == SchedulerTriggerStatus.EXECUTION_COMPLETED.value
    assert calls == [(tmp_path, "2026-06-05")]
    assert result["execution"]["bounded_execution_id"] == "bounded-exec-1"
    report = build_scheduler_report(tmp_path, day="2026-06-05")
    assert report["execution_counts"]["EXECUTION_COMPLETED"] == 1


def test_event_trigger_invokes_bounded_research_once(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(scheduler, "run_bounded_research_once", lambda root, day=None: _fake_execution())
    result = run_event_scheduler_once(SchedulerTriggerType.NEW_BACKLOG_ITEM.value, root=tmp_path, source={"backlog_item_id": "bi-1"}, day="2026-06-05")
    assert result["status"] == SchedulerTriggerStatus.EXECUTION_COMPLETED.value
    assert result["trigger"]["trigger_type"] == "NEW_BACKLOG_ITEM"
    assert read_scheduler_executions(tmp_path)[0]["metadata"]["execution_function"] == "run_bounded_research_once"


def test_duplicate_trigger_is_skipped(monkeypatch, tmp_path: Path):
    calls = []
    monkeypatch.setattr(scheduler, "run_bounded_research_once", lambda root, day=None: calls.append(1) or _fake_execution())
    first = run_scheduler_trigger(SchedulerTriggerType.MANUAL.value, root=tmp_path, source={"request_id": "same"}, day="2026-06-05")
    second = run_scheduler_trigger(SchedulerTriggerType.MANUAL.value, root=tmp_path, source={"request_id": "same"}, day="2026-06-05")
    assert first["status"] == "EXECUTION_COMPLETED"
    assert second["status"] == "TRIGGER_SKIPPED"
    assert second["trigger"]["reason"] == "DUPLICATE_TRIGGER"
    assert len(calls) == 1


def test_scheduler_writes_trigger_and_execution_ledgers(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(scheduler, "run_bounded_research_once", lambda root, day=None: _fake_execution())
    run_scheduler_trigger(SchedulerTriggerType.MANUAL.value, root=tmp_path, source={"request_id": "ledger"}, day="2026-06-05")
    triggers = read_scheduler_triggers(tmp_path)
    executions = read_scheduler_executions(tmp_path)
    assert triggers[-1]["status"] == "EXECUTION_COMPLETED"
    assert executions[0]["bounded_execution_status"] == "COMPLETED"
    assert (tmp_path / "scheduler" / "2026-06-05" / "scheduler_report.v1.json").exists()
    assert (tmp_path / "scheduler" / "latest.json").exists()
