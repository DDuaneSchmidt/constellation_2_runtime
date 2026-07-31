from constellation_2.common.atlas_v2_research_os.scheduler_events import build_trigger_key, create_scheduler_trigger, is_event_driven_trigger, is_supported_trigger
from constellation_2.common.atlas_v2_research_os.scheduler_models import SchedulerTriggerType


def test_supported_trigger_types_include_required_set():
    for trigger_type in [
        "MANUAL",
        "HOURLY",
        "NEW_BACKLOG_ITEM",
        "NEW_MEMORY_SIGNAL",
        "NEW_FAILURE_PATTERN",
        "CANDIDATE_QUALITY_REGRESSION",
        "RESEARCH_OS_CERTIFICATION_FAILURE",
    ]:
        assert is_supported_trigger(trigger_type)


def test_hourly_trigger_key_dedupes_by_hour():
    assert build_trigger_key("HOURLY", {}, "2026-06-05T10:05:00Z") == build_trigger_key("HOURLY", {}, "2026-06-05T10:55:00Z")
    assert build_trigger_key("HOURLY", {}, "2026-06-05T10:05:00Z") != build_trigger_key("HOURLY", {}, "2026-06-05T11:00:00Z")


def test_event_trigger_key_uses_source_identifier():
    trigger = create_scheduler_trigger(SchedulerTriggerType.NEW_MEMORY_SIGNAL.value, source={"memory_id": "mem-1"}, created_at="2026-06-05T00:00:00Z")
    assert trigger.trigger_key == "NEW_MEMORY_SIGNAL:memory_id:mem-1"
    assert is_event_driven_trigger(trigger.trigger_type)


def test_unsupported_trigger_fails_closed():
    try:
        create_scheduler_trigger("TRADE_SIGNAL", created_at="2026-06-05T00:00:00Z")
    except ValueError as exc:
        assert "unsupported scheduler trigger type" in str(exc)
    else:
        raise AssertionError("unsupported trigger should fail")
