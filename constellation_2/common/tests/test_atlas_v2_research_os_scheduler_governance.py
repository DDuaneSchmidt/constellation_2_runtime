from pathlib import Path

from constellation_2.common.atlas_v2_research_os import scheduler
from constellation_2.common.atlas_v2_research_os.scheduler import run_scheduler_trigger
from constellation_2.common.atlas_v2_research_os.scheduler_governance import validate_scheduler_execution_allowed, validate_scheduler_no_authority_escalation, validate_scheduler_trigger_allowed
from constellation_2.common.atlas_v2_research_os.scheduler_models import SchedulerTriggerStatus, SchedulerTriggerType


def test_scheduler_may_only_invoke_bounded_research_once():
    ok = validate_scheduler_execution_allowed({"execution_function": "run_bounded_research_once", "continuous_loop": False, "daemon": False})
    bad = validate_scheduler_execution_allowed({"execution_function": "trade", "continuous_loop": False, "daemon": False})
    loop = validate_scheduler_execution_allowed({"execution_function": "run_bounded_research_once", "continuous_loop": True, "daemon": False})
    assert ok["result"] == "PASS"
    assert bad["result"] == "FAIL"
    assert loop["result"] == "FAIL"


def test_forbidden_authority_markers_are_rejected():
    result = validate_scheduler_no_authority_escalation({"artifact_type": "CapitalAllocation"})
    assert result["result"] == "FAIL"


def test_trigger_source_trading_authority_is_blocked():
    result = validate_scheduler_trigger_allowed({"trigger_type": "MANUAL", "source": {"trading_authority": True}})
    assert result["result"] == "FAIL"


def test_governance_failure_blocks_scheduler_execution(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(scheduler, "run_bounded_research_once", lambda root, day=None: {"execution_id": "should-not-run"})
    result = run_scheduler_trigger(SchedulerTriggerType.MANUAL.value, root=tmp_path, source={"request_id": "blocked", "trading_authority": True}, day="2026-06-05")
    assert result["status"] == SchedulerTriggerStatus.TRIGGER_BLOCKED.value
    assert result["governance_result"]["status"] == "FAIL"
